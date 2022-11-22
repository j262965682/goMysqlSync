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
	"fmt"
	"github.com/panjf2000/ants/v2"
	"github.com/siddontang/go-mysql/schema"
	"go-mysql-sync/service/endpoint"
	"go-mysql-sync/util"
	"go-mysql-sync/util/stringutil"
	"go.uber.org/atomic"
	"runtime"
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"

	"go-mysql-sync/global"
	"go-mysql-sync/util/dateutil"
	"go-mysql-sync/util/logutil"
)

var _threads = runtime.NumCPU()

// StockService 存量数据
type StockService struct {
	canal    *canal.Canal
	endpoint endpoint.Endpoint
	//pageSize int64
	dumpRecordRows int64
	queueCh        chan []*global.RowRequest
	counter        map[string]int64
	lockOfCounter  sync.Mutex
	totalRows      map[string]int64
	table          map[string]endpoint.TableStatus
	wg             sync.WaitGroup
	shutoff        *atomic.Bool
}

func NewStockService(t *TransferService) *StockService {
	return &StockService{
		canal:          t.canal,
		endpoint:       t.endpoint,
		table:          endpoint.GFullProgress.Table,
		dumpRecordRows: int64(t.config.DumpRecordRows),
		//pageSize:  int64(t.config.BulkSize),
		//queueCh:   make(chan []*global.RowRequest),
		counter:   make(map[string]int64),
		totalRows: make(map[string]int64),
		shutoff:   atomic.NewBool(false),
	}
}

type stockRequest struct {
	key                 string //新表名
	columnsString       string
	columnsStringBTABLE string
	batch               int64
	SourceSchemaTable   string
}

func (s *StockService) Run() error {
	var tableMate *schema.Table
	var err error

	// 获取当前 position
	if endpoint.InitPos, err = s.canal.GetMasterPos(); err != nil {
		return err
	}
	logutil.BothInfof(fmt.Sprintf("当前 position : %s", endpoint.InitPos.String()))

	// 判断是否同步全量数据标识
	if util.GStockFlag {
		startTime := dateutil.NowMillisecond()
		logutil.BothInfof(fmt.Sprintf("全量同步开始,批量小大为 size: %d", s.dumpRecordRows))
		size := s.dumpRecordRows
		//  key 为旧表名
		for key, status := range s.table {
			sourceSchemaTable := status.Schema + "." + status.Name
			// 带进来的 key就是新库表名
			TargetSchemaTable := status.TargetSchema + "." + status.TargetName
			// 取表的列
			if tableMate, err = s.canal.GetTable(status.Schema, status.Name); err != nil {
				errors.Trace(err)
				return err
			}
			status.Columns = tableMate.Columns
			s.table[key] = status
			columnsString := s.getColumnsString(tableMate.Columns)
			columnsStringBTABLE := s.getColumnsStringBTABLE(tableMate.Columns)
			tableCount := status.Rows

			if tableCount == 0 {
				logutil.BothInfof(fmt.Sprintf("%s 表无数据 ,调过 ", key))
				continue
			} else {
				logutil.BothInfof(fmt.Sprintf("开始导出 %s,共 %d 条数据", key, status.Rows))
			}

			s.counter[key] = 0

			// 批次
			var batch int64
			if tableCount%size == 0 {
				batch = tableCount / size
			} else {
				batch = (tableCount / size) + 1
			}
			// logutil.BothInfof(fmt.Sprintf(" %d 线程数", global.Cfg().DumpThreads))
			pool, _ := ants.NewPoolWithFunc(global.Cfg().DumpThreads, func(stockReq interface{}) {
				req := stockReq.(*stockRequest)
				var n int
				s.syncBatchRows(req.SourceSchemaTable, req.key, req.columnsString, req.columnsStringBTABLE, req.batch, n)
				s.wg.Done()
			})
			var index int64
			for index = 1; index <= batch; index++ {
				var oneStock = &stockRequest{key: TargetSchemaTable, columnsString: columnsString, columnsStringBTABLE: columnsStringBTABLE, batch: index, SourceSchemaTable: sourceSchemaTable}
				s.wg.Add(1)
				pool.Invoke(oneStock)
			}
			s.wg.Wait()
			pool.Release()
			logutil.BothInfof(fmt.Sprintf(" %s 导入完成", key))
			//// -----------------------------------------------------------------------------------------
			//var stockArray []chan *stockRequest
			//
			////var index int64
			//for i := 0; i < global.Cfg().DumpThreads; i++ {
			//	stockArray[i] = make(chan *stockRequest)
			//	go func(request chan *stockRequest) {
			//		for req := range request {
			//			s.syncBatchRows(key, columnsString, columnsStringBTABLE, req.batch, i)
			//		}
			//	}(stockArray[i])
			//}
			//for index = 0; index < batch; index++ {
			//	var oneStock = &stockRequest{key: key,columnsString: columnsString,columnsStringBTABLE: columnsStringBTABLE,batch: index}
			//	wg.Add(1)
			//	_ = primitive.Invoker()
			//}
			//
			//// -------------------------------------------------------------多线程处理插入
			//// 批次进度
			//var processed atomic.Int64
			//for i := 0; i < global.Cfg().DumpThreads; i++ {
			//	s.wg.Add(1)
			//	go func(key, columnsString, columnsStringBTABLE string, i int) {
			//		for {
			//			logutil.BothInfof("%d 号 线程开始循环", i)
			//			processed.Inc()
			//			err = s.syncBatchRows(key, columnsString, columnsStringBTABLE, processed.Load(), i)
			//			if err != nil {
			//				logutil.Error(err.Error())
			//				s.shutoff.Store(true)
			//				break
			//			}
			//			if processed.Load() > batch {
			//				break
			//			}
			//		}
			//		s.wg.Done()
			//	}(key, columnsString, columnsStringBTABLE, i)
			//}
			//s.wg.Wait()
			//logutil.BothInfof(fmt.Sprintf(" %s 导入完成", key))
			// --------------------------------------------------------------------------------------
		}
		logutil.BothInfof(fmt.Sprintf("全量同步完成 共耗时 ：%d（毫秒）", dateutil.NowMillisecond()-startTime))
		return err
	} else {
		logutil.BothInfof("跳过全量同步")
		return nil
	}
}

//func (s *StockService) export(fullName, columns string, batch int64, rule *global.Rule) ([]*global.RowRequest, error) {
//	if s.shutoff.Load() {
//		return nil, errors.New("shutoff")
//	}
//
//	offset := s.offset(batch)
//	sql := fmt.Sprintf("select %s from %s order by %s limit %d,%d", columns, fullName, rule.OrderByColumn, offset, s.pageSize)
//	resultSet, err := s.transfer.canal.Execute(sql)
//	if err != nil {
//		logutil.Errorf("数据导出错误: %s - %s", sql, err.Error())
//		return nil, err
//	}
//	rowNumber := resultSet.RowNumber()
//	requests := make([]*global.RowRequest, 0, rowNumber)
//	for i := 0; i < rowNumber; i++ {
//		rowValues := make([]interface{}, 0, len(rule.TableInfo.Columns))
//		request := new(global.RowRequest)
//		for j := 0; j < len(rule.TableInfo.Columns); j++ {
//			val, err := resultSet.GetValue(i, j)
//			if err != nil {
//				logutil.Errorf("数据导出错误: %s - %s", sql, err.Error())
//				break
//			}
//			rowValues = append(rowValues, val)
//			request.Action = canal.InsertAction
//			request.RuleKey = global.RuleKey(rule.Schema, rule.Table)
//			request.Row = rowValues
//		}
//		requests = append(requests, request)
//	}
//
//	return requests, nil
//}

//func (s *StockService) imports(fullName string, requests []*global.RowRequest, processed int64) {
//	if s.shutoff.Load() {
//		return
//	}
//
//	succeeds := s.transfer.endpoint.Stock(requests)
//	count := s.incCounter(fullName, succeeds)
//	log.Println(fmt.Sprintf("%s 导入数据 %d 条", fullName, count))
//}

func (s *StockService) exportColumns(rule *global.Rule) string {
	if rule.IncludeColumnConfig != "" {
		var columns string
		includes := strings.Split(rule.IncludeColumnConfig, ",")
		for _, c := range rule.TableInfo.Columns {
			for _, e := range includes {
				var column string
				if strings.ToUpper(e) == strings.ToUpper(c.Name) {
					column = c.Name
				} else {
					column = "null as " + c.Name
				}

				if columns != "" {
					columns = columns + ","
				}
				columns = columns + column
			}
		}
		return columns
	}

	if rule.ExcludeColumnConfig != "" {
		var columns string
		excludes := strings.Split(rule.ExcludeColumnConfig, ",")
		for _, c := range rule.TableInfo.Columns {
			for _, e := range excludes {
				var column string
				if strings.ToUpper(e) == strings.ToUpper(c.Name) {
					column = "null as " + c.Name
				} else {
					column = c.Name
				}

				if columns != "" {
					columns = columns + ","
				}
				columns = columns + column
			}
		}
		return columns
	}

	return "*"
}

func (s *StockService) offset(currentPage int64) int64 {
	var offset int64

	if currentPage > 0 {
		offset = (currentPage - 1) * s.dumpRecordRows
	}
	return offset
}

func (s *StockService) Close() {
	s.endpoint.Close()
}

func (s *StockService) incCounter(fullName string, n int64) int64 {
	s.lockOfCounter.Lock()
	defer s.lockOfCounter.Unlock()

	count, ok := s.counter[fullName]
	if ok {
		count = count + n
		s.counter[fullName] = count
	}

	return count
}

func (s *StockService) getColumnsString(tableColumn []schema.TableColumn) string {
	var columns string
	for _, column := range tableColumn {
		if columns != "" {
			columns = columns + ","
		}
		columns = columns + "`" + column.Name + "`"
	}
	return columns
}

func (s *StockService) getColumnsStringBTABLE(tableColumn []schema.TableColumn) string {
	var columns string
	for _, column := range tableColumn {
		if columns != "" {
			columns = columns + ","
		}
		columns = columns + "b.`" + column.Name + "`"
	}
	return columns
}

func (s *StockService) syncBatchRows(SourceSchemaTable string, TargetSchemaTable string, columns string, columnsB string, batch int64, threadNum int) error {
	//var mysqlEndpoint endpoint.Endpoint
	//if s.shutoff.Load() {
	//	return errors.New("shutoff")
	//}
	//if global.Cfg().IsMysql() {
	//	mysqlEndpoint, _ := s.endpoint.(*endpoint.MysqlEndpoint)
	//}
	//var err error
	offset := s.offset(batch)

	//  select b.* from (select id from zlb_expense_form_check_result order by id limit 100000,1000) a left join zlb_expense_form_check_result b on a.id=b.id
	selectSql := fmt.Sprintf("select %s from (select id from %s order by id limit %d,%d) a left join %s b on a.id=b.id", columnsB, SourceSchemaTable, offset, s.dumpRecordRows, SourceSchemaTable)

	//logutil.Info(selectSql)

	// 结果集 用 canal 来查询
	//resultSet, err := s.canal.Execute(selectSql)
	//if err != nil {
	//	logutil.Errorf("数据导出错误: %s - %s", schemaTable, err.Error())
	//	return err
	//}

	// 结果集 用 gorm 来查询
	resultSet, err := s.endpoint.FindSQLToMap(selectSql)
	if err != nil {
		return err
	}

	rowNumber := len(resultSet)
	if rowNumber == 0 {
		logutil.Infof("未查询到数据: %s : limit %d, %d", SourceSchemaTable, offset, s.dumpRecordRows)
		return nil
	}
	// 插入sql的前半部分
	insertTable := fmt.Sprintf(`INSERT IGNORE INTO %s (%s) VALUES`, TargetSchemaTable, columns)
	// 插入sql的后半部分
	var insertValues string // (?,?,?),(),()
	// values list
	allValues := make([]interface{}, 0, len(s.table[SourceSchemaTable].Columns)*rowNumber)

	for index := range resultSet {
		rowValues := make([]interface{}, 0, len(s.table[SourceSchemaTable].Columns))
		var itemValues string
		itemValues = strings.Repeat(" ?,", len(s.table[SourceSchemaTable].Columns))
		itemValues = stringutil.CutLastString(itemValues, 1)
		itemValues = fmt.Sprintf("( %s )", itemValues)

		if insertValues != "" {
			insertValues = insertValues + ","
		}
		insertValues = insertValues + itemValues

		for _, column := range s.table[SourceSchemaTable].Columns {
			val := resultSet[index][column.Name]
			rowValues = append(rowValues, val)
		}
		allValues = append(allValues, rowValues...)
	}

	insertSql := insertTable + insertValues
	var rowsAffected int64
	rowsAffected, err = s.endpoint.StockExecSql(insertSql, allValues)
	if err != nil {
		return err
	}
	//logutil.BothInfof(fmt.Sprintf("%s 导入数据 %d 条", schemaTable, s.incCounter(schemaTable, rowsAffected)))
	logutil.BothInfof(fmt.Sprintf("%s 导入数据 %d 条", TargetSchemaTable, rowsAffected))
	//if rowsAffected == 0 {
	//	logutil.BothInfof(selectSql)
	//}
	return nil
}
