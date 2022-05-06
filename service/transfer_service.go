/*
 * Copyright 2020-2021 the original author(https://github.com/wj596)
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */
package service

import (
	"context"
	"fmt"
	"github.com/siddontang/go-mysql/mysql"
	"go-mysql-transfer/util"
	"log"
	"regexp"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"go.uber.org/atomic"

	"go-mysql-transfer/global"
	"go-mysql-transfer/service/endpoint"
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util/logutil"
)

const _metricsTaskInterval = 10

type TransferService struct {
	config          *global.Config
	canal           *canal.Canal
	positionStorage storage.PositionStorage

	endpoint endpoint.Endpoint
	handler  *handler

	listenerStarted atomic.Bool
	running         atomic.Bool
	wg              sync.WaitGroup
	ctx             context.Context
	cancelFunc      context.CancelFunc
}

func (s *TransferService) initialize() error {
	if err := s.initCanal(); err != nil {
		return errors.Trace(err)
	}

	if err := s.initRules(); err != nil {
		return errors.Trace(err)
	}

	// 初始化 endpoint   Start 里面会根据标识是否同步表结构、获取所有表的行数
	_endpoint := endpoint.NewEndpoint(s.config, s.canal)
	if err := _endpoint.Start(); err != nil {
		return errors.Trace(err)
	}
	global.SetDestinationState(global.MetricsStateOK)
	s.endpoint = _endpoint

	// start()中已经检测和同步了表结构 接下来 全量同步数据，全量数据按表的行数来分批，每批的量由 参数 控制
	stockService := NewStockService(s)
	if err := stockService.Run(); err != nil {
		return errors.Trace(err)
	}

	s.initDumper()

	s.positionStorage = storage.NewPositionStorage(s.config)
	if err := s.positionStorage.Init(); err != nil {
		return errors.Trace(err)
	}

	s.ctx, s.cancelFunc = context.WithCancel(context.Background())

	//初始化hashMap 来对应分配线程使用
	hashMap := global.NewHashMap(s.config.Threads, 4096)

	s.handler = &handler{
		requestQueue:    make(chan interface{}, 4096),
		ddLRequestQueue: make(chan interface{}, 4096),
		transfer:        s,
		hashMap:         hashMap,
	}
	s.canal.SetEventHandler(s.handler)

	return nil
}

func (s *TransferService) run() error {
	s.wg.Add(1)
	s.handler.startRequestQueueListener()

	////--------------------------------------------
	//bulkSize := s.handler.transfer.config.BulkSize
	////binlog事件需求队列
	//var message = make(chan []*global.RowRequest, bulkSize)
	//
	//// 创建binlog事件的Worker
	//var n int
	//n = 20
	//for i := 0; i < n; i ++ {
	//	go s.endpoint.Consume(i, message)
	//}
	//-----------------------------------------------

	if s.config.IsExporterEnable() {
		s.startMetricsTask()
	}
	//G_full if true,全量同步；if false,增量同步
	var current mysql.Position
	var err error
	if util.G_full {
		// 新的全量加增量同步 读取源端 当前 position
		current = endpoint.InitPos
	} else if util.G_pos.Timestamp > 0 {
		// 指定 position 开始增量同步，position 是从 bolt 里面获取的 有Timestamp字段
		current.Timestamp = util.G_pos.Timestamp
		current.Pos = util.G_pos.Pos
		current.Name = util.G_pos.Name
	} else {
		// 掉线后，重新开启同步，不提供 position，自动从 bolt 里面获取
		logutil.Infof("获取十五分钟前的binlog位置")
		current, err = s.positionStorage.AcquirePosition()
		if err != nil {
			panic(err)
		}
		logutil.Infof("重新开始同步数据")
	}

	logutil.BothInfof("transfer run from pos %s %d", current.Name, current.Pos)

	s.running.Store(true)
	//fmt.Println("准备开始拉取数据")
	//time.Sleep(time.Second * 30)
	//return nil
	if err := s.canal.RunFrom(current); err != nil {
		log.Println(fmt.Sprintf("start transfer : %v", err))
		logutil.Errorf("start transfer : %v", err)
		s.cancelFunc()
		return errors.Trace(err)
	}

	s.running.Store(false)
	logutil.Info("Canal is Closed")
	return nil
}

func (s *TransferService) Pause() {
	if s.running.Load() {
		logutil.BothInfof("transfer paused !!!")
		s.canal.Close()
		s.canal = nil
		s.running.Store(false)
	}
}

func (s *TransferService) Restart() {
	if s.listenerStarted.Load() {
		if s.canal == nil {
			logutil.BothInfof("transfer rerun !!!")
			go s.rerun()
		}
	} else {
		logutil.BothInfof("transfer run !!!")
		go s.run()
	}
}

func (s *TransferService) rerun() {
	s.initCanal()
	s.initDumper()
	s.canal.SetEventHandler(s.handler)
	s.running.Store(true)

	current, _ := s.positionStorage.AcquirePosition()
	logutil.Infof("TransferService Restart! ,Position: %s-%d", current.Name, current.Pos)
	if err := s.canal.RunFrom(current); err != nil {
		logutil.Errorf("start transfer err %v", err)
	}
}

func (s *TransferService) close() {
	logutil.Infof("closing transfer")

	s.cancelFunc()

	if s.canal != nil {
		s.canal.Close()
	}

	s.endpoint.Close()

	s.wg.Wait()
}

func (s *TransferService) initCanal() error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = s.config.Addr
	cfg.User = s.config.User
	cfg.Password = s.config.Password
	cfg.Charset = s.config.Charset
	cfg.Flavor = s.config.Flavor
	cfg.ServerID = s.config.SlaveID
	cfg.Dump.ExecutionPath = s.config.DumpExec
	cfg.Dump.DiscardErr = false
	cfg.Dump.SkipMasterData = s.config.SkipMasterData
	cfg.Dump.ExtraOptions = []string{"--set-gtid-purged=OFF"}

	//for _, s := range s.config.RuleConfigs {
	//	cfg.IncludeTableRegex = append(cfg.IncludeTableRegex, s.Schema+"."+s.Table)
	//}
	var err error
	s.canal, err = canal.NewCanal(cfg)
	//fmt.Println("TransferService:initCanal:cfg.dump.databases:",cfg.Dump.Databases)
	return errors.Trace(err)
}

func AddDatabases(cfg *canal.Config, dbs string) {
	cfg.Dump.Databases = append(cfg.Dump.Databases, dbs)
}

func AddTables(cfg *canal.Config, db string, tables string) {
	if cfg.Dump.TableDB != db {
		cfg.Dump.TableDB = db
		cfg.Dump.Tables = cfg.Dump.Tables[0:0]
	}
	cfg.Dump.Tables = append(cfg.Dump.Tables, tables)
}

func (s *TransferService) initRules() error {
	wildcards := make(map[string]bool)
	if s.config.IsMysql() {
		for _, rc := range s.config.RuleConfigs {
			newRule, _ := global.RuleDeepClone(rc)
			ruleKey := global.RuleKey(rc.Schema, rc.Table)
			global.AddRuleIns(ruleKey, newRule)
		}
	} else {
		for _, rc := range s.config.RuleConfigs {
			if rc.Table == "*" {
				return errors.Errorf("wildcard * is not allowed for table name")
			}
			if regexp.QuoteMeta(rc.Table) != rc.Table { //通配符
				if _, ok := wildcards[global.RuleKey(rc.Schema, rc.Schema)]; ok {
					return errors.Errorf("duplicate wildcard table defined for %s.%s", rc.Schema, rc.Table)
				}

				tableName := rc.Table
				if rc.Table == "*" {
					tableName = "." + rc.Table
				}
				sql := fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE
					table_name RLIKE "%s" AND table_schema = "%s";`, tableName, rc.Schema)
				res, err := s.canal.Execute(sql)
				if err != nil {
					return errors.Trace(err)
				}
				for i := 0; i < res.Resultset.RowNumber(); i++ {
					tableName, _ := res.GetString(i, 0)
					newRule, err := global.RuleDeepClone(rc)
					if err != nil {
						return errors.Trace(err)
					}
					newRule.Table = tableName
					ruleKey := global.RuleKey(rc.Schema, tableName)
					global.AddRuleIns(ruleKey, newRule)
				}
			} else {
				newRule, err := global.RuleDeepClone(rc)
				if err != nil {
					return errors.Trace(err)
				}
				ruleKey := global.RuleKey(rc.Schema, rc.Table)
				global.AddRuleIns(ruleKey, newRule)
			}
		}

		//取配置中所有表的元数据
		for _, rule := range global.RuleInsList() {
			tableMata, err := s.canal.GetTable(rule.Schema, rule.Table)
			if err != nil {
				return errors.Trace(err)
			}
			if len(tableMata.PKColumns) == 0 {
				if !s.config.SkipNoPkTable {
					return errors.Errorf("%s.%s must have a PK for a column", rule.Schema, rule.Table)
				}
			}
			if len(tableMata.PKColumns) > 1 {
				rule.IsCompositeKey = true // 组合主键
			}
			rule.TableInfo = tableMata
			rule.TableColumnSize = len(tableMata.Columns)

			if err := rule.Initialize(); err != nil {
				return errors.Trace(err)
			}

			if rule.LuaNecessary() {
				if err := rule.PreCompileLuaScript(s.config.DataDir); err != nil {
					return err
				}
			}
		}
	}
	//fmt.Println("TransferService:initRules:Rule")
	//fmt.Println("-------------------------------")
	//for _,rule :=range global.RuleInsList() {
	//	fmt.Println(rule.Schema,rule.Table)
	//}
	//fmt.Println("-------------------------------")
	return nil
}

func (s *TransferService) initDumper() {
	var schema string
	schemas := make(map[string]int)
	tables := make([]string, 0, global.RuleInsTotal())
	for _, rule := range global.RuleInsList() {
		if s.config.IsMysql() {
			if rule.Table == "table_all_in" {
				s.canal.AddDumpDatabases(rule.Schema)
			} else {
				s.canal.AddDumpTables(rule.Schema, rule.Table)
			}
		} else {
			//fmt.Println(rule.Schema,rule.Table)
			schema = rule.Schema
			schemas[rule.Schema] = 1
			tables = append(tables, rule.Table)
		}
	}
	if !s.config.IsMysql() {
		if len(schemas) == 1 {
			//fmt.Println("add tables",schema,tables)
			s.canal.AddDumpTables(schema, tables...)
		} else {
			keys := make([]string, 0, len(schemas))
			for key := range schemas {
				keys = append(keys, key)
			}
			//fmt.Println("add databases")
			s.canal.AddDumpDatabases(keys...)
		}
	}
	//fmt.Println("IsMysql",s.config.IsMysql())
	//s.config.IsMysql()
	//fmt.Println("schema,table",schemas,tables)
	//fmt.Println("--------------------------")
	//fmt.Println("TransferService:initDumper")
	//s.canal.Run()
	//fmt.Println("--------------------------")
	//fmt.Println("TransferService:initDumper:cfg.dump.databases:",s.canal.c)
}

func (s *TransferService) updateRule(schema, table string) error {
	rule, ok := global.RuleIns(global.RuleKey(schema, table))
	if ok {
		tableInfo, err := s.canal.GetTable(schema, table)
		if err != nil {
			return errors.Trace(err)
		}

		if len(tableInfo.PKColumns) == 0 {
			if !s.config.SkipNoPkTable {
				return errors.Errorf("%s.%s must have a PK for a column", rule.Schema, rule.Table)
			}
		}

		if len(tableInfo.PKColumns) > 1 {
			rule.IsCompositeKey = true
		}

		rule.TableInfo = tableInfo
		rule.TableColumnSize = len(tableInfo.Columns)

		err = rule.AfterUpdateTableInfo()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *TransferService) startMetricsTask() {
	ticker := time.NewTicker(_metricsTaskInterval * time.Second)
	go func() {
		for {
			<-ticker.C
			if err := s.endpoint.Ping(); err != nil {
				global.SetDestinationState(global.MetricsStateNO)
			} else {
				global.SetDestinationState(global.MetricsStateOK)
			}

			global.SetTransferDelay(s.canal.GetDelay())

			//发起延迟数据上报线程
			global.SetDelay(global.ExecuteSQLTimestamp, uint32(time.Now().Unix()))
			//fmt.Printf("当前延迟: %d s", uint32(time.Now().Unix())-global.ExecuteSQLTimestamp)
			//上报本机状态数值
			global.GetMachineState()

		}
	}()
}
