package endpoint

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/mattn/go-colorable"
	"github.com/siddontang/go-mysql/canal"
	"github.com/vmihailenco/msgpack"
	"go-mysql-transfer/global"
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util"
	"go-mysql-transfer/util/logutil"
	"go-mysql-transfer/util/stringutil"
	gormMysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"strings"
	"time"
)

type MysqlEndpoint struct {
	config *global.Config
	cached *storage.BoltRowStorage

	client *gorm.DB

	ddlClient DdlClient
}

type DdlClient struct {
	schema string
	client *gorm.DB
}

func newMysqlEndpoint(c *global.Config) *MysqlEndpoint {
	// dsn format: "user:password@addr?dbname"
	dsn := c.MysqlUsername + ":" + c.MysqlPass + "@tcp(" + c.MysqlAddr + ")/" + "test?charset=utf8&parseTime=True&loc=Local"
	mysql := &MysqlEndpoint{}
	newLogger := logger.New(
		log.New(colorable.NewColorableStdout(), "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold: time.Second, // 慢 SQL 阈值
			LogLevel:      logger.Info, // Log level
			Colorful:      true,        // 禁用彩色打印
		},
	)
	db, _ := gorm.Open(gormMysql.New(gormMysql.Config{
		DSN:                       dsn,   // DSN data source name
		DefaultStringSize:         256,   // string 类型字段的默认长度
		DisableDatetimePrecision:  true,  // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
		DontSupportRenameIndex:    true,  // 重命名索引时采用删除并新建的方式，MySQL 5.7 之前的数据库和 MariaDB 不支持重命名索引
		DontSupportRenameColumn:   true,  // 用 `change` 重命名列，MySQL 8 之前的数据库和 MariaDB 不支持重命名列
		SkipInitializeWithVersion: false, // 根据当前 MySQL 版本自动配置
	}), &gorm.Config{Logger: newLogger})
	mysql.client = db
	mysql.config = c
	mysql.cached = &storage.BoltRowStorage{}
	sqlDB, _ := db.DB()
	// SetMaxIdleConns sets the maximum number of connections in the idle connection pool.
	sqlDB.SetMaxIdleConns(30)
	// SetMaxOpenConns sets the maximum number of open connections to the database.
	sqlDB.SetMaxOpenConns(100)
	// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
	sqlDB.SetConnMaxLifetime(time.Hour)
	//fmt.Println(2)
	return mysql
}

func (s *MysqlEndpoint) Start() error {
	var err error
	if err = s.Ping(); err != nil {
		return err
	}
	//判断是否需要同步表结构标识
	if util.G_full {
		//同步库表信息
		if err = s.SyncTableStructure(); err != nil {
			return err
		}
	}
	return nil
}

func (s *MysqlEndpoint) GetMasterClient() (db *gorm.DB, err error) {
	dsn := s.config.User + ":" + s.config.Password + "@tcp(" + s.config.Addr + ")/" + "test?charset=utf8&parseTime=True&loc=Local"
	db, err = gorm.Open(gormMysql.New(gormMysql.Config{
		DSN:                       dsn,   // DSN data source name
		DefaultStringSize:         256,   // string 类型字段的默认长度
		DisableDatetimePrecision:  true,  // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
		DontSupportRenameIndex:    true,  // 重命名索引时采用删除并新建的方式，MySQL 5.7 之前的数据库和 MariaDB 不支持重命名索引
		DontSupportRenameColumn:   true,  // 用 `change` 重命名列，MySQL 8 之前的数据库和 MariaDB 不支持重命名列
		SkipInitializeWithVersion: false, // 根据当前 MySQL 版本自动配置
	}), &gorm.Config{})
	sqlDB, _ := db.DB()
	// SetMaxIdleConns sets the maximum number of connections in the idle connection pool.
	sqlDB.SetMaxIdleConns(40)
	// SetMaxOpenConns sets the maximum number of open connections to the database.
	sqlDB.SetMaxOpenConns(100)
	// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
	sqlDB.SetConnMaxLifetime(time.Hour)
	return
}

func (s *MysqlEndpoint) SyncTableStructure() error {
	var masterDb *gorm.DB
	var err error
	var count int64
	var total int64
	var length int64
	masterDb, err = s.GetMasterClient()
	tableList := make([]global.Tables, 0)
	tablesList := make([]string, 0)
	tablesNoSchemaList := make([]string, 0)

	//同步库信息
	for _, rule := range global.RuleInsList() {
		schema := rule.Schema
		createDatabaseSql := "SHOW CREATE DATABASE " + strings.ToLower(schema) + ";"
		var databaseDesc global.DatabaseDesc
		masterDb.Raw(createDatabaseSql).Scan(&databaseDesc)
		//sql := "CREATE DATABASE IF NOT EXISTS "+ schema +";"
		CreateDatabaseNotExistsSql := databaseDesc.CreateDatabase[0:16] + "IF NOT EXISTS " + databaseDesc.CreateDatabase[15:]
		if db := s.client.Exec(CreateDatabaseNotExistsSql); db.Error != nil {
			return db.Error
		}
	}
	fkOffSql := "SET FOREIGN_KEY_CHECKS = 0;"
	fkOnSql := "SET FOREIGN_KEY_CHECKS = 1;"
	//先关闭外键约束
	if db := s.client.Exec(fkOffSql); db.Error != nil {
		return db.Error
	}
	//同步表结构
	for _, rule := range global.RuleInsList() {
		logutil.BothInfof("开始获取表结构信息")
		if rule.Table == "table_all_in" {
			masterDb.Table("information_schema.tables").Select("table_name").Where("TABLE_SCHEMA = ?", rule.Schema).Scan(&tableList)
			logutil.BothInfof("获取整个库表信息 %s", rule.Schema)
			for _, table := range tableList {
				//sql :="show create table " + table.TableName
				tablesList = append(tablesList, rule.Schema+"."+table.TableName)
				tablesNoSchemaList = append(tablesNoSchemaList, table.TableName)
			}
			length = length + int64(len(tableList))
			s.client.Table("information_schema.tables").Where("TABLE_SCHEMA = ? and table_name IN ?", rule.Schema, tablesNoSchemaList).Count(&count)
			total += count
			count = 0
			tablesNoSchemaList = tablesNoSchemaList[0:0]
		} else {
			logutil.BothInfof("获取单个表信息 %s %s", rule.Schema, rule.Table)
			tablesList = append(tablesList, rule.Schema+"."+rule.Table)
			s.client.Table("information_schema.tables").Where("TABLE_SCHEMA = ? and table_name = ?", rule.Schema, rule.Table).Count(&count)
			length += 1
			//fmt.Println(count)
			if count != 0 {
				total++
			}
			count = 0
		}
	}
	//fmt.Println(total,len(tablesList))
	fmt.Println(total, length)
	if total == length {
		logutil.Info("表结构已经同步，跳过")
		return nil
	}
	if total > 0 {
		return errors.New("部分表已存在")
	} else {
		logutil.Info("开始初始化表结构")
		for _, table := range tablesList {
			sql := "SHOW CREATE TABLE " + table + ";"
			var tableDesc global.TableDesc
			masterDb.Raw(sql).Scan(&tableDesc)
			//createTablesList[table] = tableDesc.CreateTable
			//useSql := "use "+
			schema := strings.Split(table, ".")[0]
			useSql := "use " + schema + ";"
			if err = s.client.Exec(useSql).Error; err != nil {
				logutil.Error("表结构初始化出错: use  ;" + schema + ";" + err.Error())
				return err
			}
			createSql := tableDesc.CreateTable
			logutil.Infof("表结构初始化: " + table + ":" + createSql)
			if err = s.client.Exec(createSql).Error; err != nil {
				logutil.Error("表结构初始化出错: create table " + table + ";" + err.Error())
				return err
			}
		}
	}
	//先开启外键约束
	if db := s.client.Exec(fkOnSql); db.Error != nil {
		return db.Error
	}
	return err
}

func (s *MysqlEndpoint) Ping() error {
	return s.client.Exec("select 1").Error
}

//消费binlog事件
func (s *MysqlEndpoint) Consume(n int, message chan *global.RowRequest, changeChan global.ChangeChan) {
	var err error
	//判断是不是DDL，如果是100 则是DDL
	//执行sql结束 需要上报时间戳
	if n == 100 {
		logutil.Info("启动 ddl处理线程")
		for row := range message {
			//logutil.Info("ddl处理线程:" + row.Query)

			resp := s.toBeRespond(row)
			//logutil.Info("开始执行sql")
			err = s.Exec(resp)
			if err != nil {
				panic(err)
			} else {
				//logutil.Info("ddl执行完成")
				global.GlobalChangeChan.Mutex.Lock()
				global.GlobalChangeChan.DdlControl = false
				global.GlobalChangeChan.Mutex.Unlock()
				global.GlobalChangeChan.DdlControlChan <- struct{}{}
			}
			exportActionNum(row.Action, row.RuleKey)
		}

	} else {
		var lastSql *global.MysqlRespond
		//如果0.1s接收不到sql就执行lastSql
		ticker := time.NewTicker(time.Millisecond * 100)
		defer ticker.Stop()

		for {
			select {
			case row := <-message:
				//改造row
				resp := s.toBeRespond(row)
				//判断 sql类型  是 则 插入和删除 可以优化， 不是 则 不优化
				if resp.Action == canal.InsertAction || resp.Action == canal.DeleteAction {
					if lastSql != nil {
						if resp.Schema == lastSql.Schema && resp.Table == lastSql.Table && resp.Action == lastSql.Action {
							//改造 last
							if resp.Action == canal.InsertAction {
								//改造 InsertAction last ，增加合并数，把val 加入进 val组
								lastSql.NumOfSql++
								lastSql.ManyVal = append(lastSql.ManyVal, resp.Val...)
								//如果合并已经达到指定条数sql就执行
								if lastSql.NumOfSql == 50 {
									//执行last
									err = s.Exec(lastSql)
									s.AfterExec(lastSql, err)
									//情空 lastSql
									lastSql = nil
								}
							} else if resp.Action == canal.DeleteAction {
								//改造 DeleteAction last ，增加合并数，把id 加入进 id组
								lastSql.NumOfSql++
								lastSql.ManyId = append(lastSql.ManyId, resp.Id)
								//如果合并已经达到指定条数sql就执行
								if lastSql.NumOfSql == 50 {
									//执行last
									err = s.Exec(lastSql)
									s.AfterExec(lastSql, err)
									//情空 lastSql
									lastSql = nil
								}
							}
						} else {
							//执行last
							err = s.Exec(lastSql)
							s.AfterExec(lastSql, err)
							//设置 合并数为1，把val 放进 val组 ,初始化lastSql
							lastSql = resp
							lastSql.NumOfSql = 1
							if lastSql.Action == canal.InsertAction {
								lastSql.ManyVal = append(lastSql.ManyVal, resp.Val...)
							} else if lastSql.Action == canal.DeleteAction {
								lastSql.ManyId = append(lastSql.ManyId, resp.Id)
							}
						}
					} else {
						//初始化lastSql
						lastSql = resp
						lastSql.NumOfSql = 1
						if lastSql.Action == canal.InsertAction {
							lastSql.ManyVal = append(lastSql.ManyVal, resp.Val...)
						} else if lastSql.Action == canal.DeleteAction {
							lastSql.ManyId = append(lastSql.ManyId, resp.Id)
						}
					}

				} else if resp.Action == canal.UpdateAction {
					if lastSql == nil {
						//执行row
						err = s.Exec(resp)
						s.AfterExec(resp, err)
					} else {
						//执行last
						err = s.Exec(lastSql)
						s.AfterExec(lastSql, err)
						//执行row
						err = s.Exec(resp)
						s.AfterExec(resp, err)
						//情空 lastSql
						lastSql = nil
					}
				}
			case <-ticker.C:
				if lastSql != nil {
					//执行last
					err = s.Exec(lastSql)
					//如果insert报错 则改成replace
					s.AfterExec(lastSql, err)
					lastSql = nil
				}
			}
		}
	}
}

func (s *MysqlEndpoint) AfterExec(row *global.MysqlRespond, errOfAfter error) {
	//执行报错则放入存储，执行成功则计数
	if errOfAfter != nil {
		if row.Action == canal.InsertAction && row.Sql[0:6] == "insert" {
			//fmt.Println("流程里的sql",row.Sql)
			//fmt.Println("流程里的sql的参数",row.NumOfSql)
			row.Sql = "replace" + row.Sql[6:]
			err := s.Exec(row)
			errOfAfter = err
		}
		logutil.Error(errors.ErrorStack(errOfAfter))
		logutil.Info("执行sql报错写入存储，后续待执行 ID = " + row.Table)
		pushFailedRow(row, s.cached)
	} else {
		if row.NumOfSql > 1 {
			exportActionAdd(row.Action, row.RuleKey, row.NumOfSql)
		} else {
			exportActionNum(row.Action, row.RuleKey)
		}
	}
	if row.Timestamp > 0 {
		//SQl执行后 上报当前sql的时间戳
		global.ExecuteSQLTimestamp = row.Timestamp
		logutil.Info("binlog延迟(s)" + string(global.ExecuteSQLTimestamp))
	}
}
func (s *MysqlEndpoint) Exec(resp *global.MysqlRespond) (err error) {
	if resp.NumOfSql > 1 {
		return s.ManyExec(resp)
	} else {
		return s.OneExec(resp)
	}
}

func (s *MysqlEndpoint) OneExec(resp *global.MysqlRespond) (err error) {
	switch resp.Action {
	case canal.InsertAction:
		err = s.client.Exec(resp.Sql, resp.Val).Error
	case canal.UpdateAction:
		err = s.client.Exec(resp.Sql, resp.Val...).Error
	case canal.DeleteAction:
		err = s.client.Exec(resp.Sql, resp.Id).Error
	case "DDL":
		logutil.Info("执行DDL")
		//err = s.client.Exec(resp.SchemaStart).Error
		//err = s.client.Exec(resp.Sql).Error
		err = s.getDdlClient(resp.Schema)
		err = s.ddlClient.client.Exec(resp.Sql).Error
		//err = s.client.Exec(resp.SchemaEnd).Error
	}
	return
}

func (s *MysqlEndpoint) ManyExec(resp *global.MysqlRespond) (err error) {
	var sqlStr string
	switch resp.Action {
	case canal.InsertAction:
		// insert into %s.%s (%s) values (?)
		sqlStr = resp.Sql
		sqlStr = stringutil.CutLastString(sqlStr, 3)
		oneTableCol := "(" + strings.Repeat("?,", len(resp.Val))
		oneTableCol = stringutil.CutLastString(oneTableCol, 1)
		oneTableCol += "),"
		allTableCol := strings.Repeat(oneTableCol, resp.NumOfSql)
		sqlStr += stringutil.CutLastString(allTableCol, 1)

		//sqlStr += strings.Repeat("(?),", resp.NumOfSql)
		//sqlStr = stringutil.CutLastString(sqlStr, 1)
		//logutil.Infof("sql", sqlStr)
		//fmt.Println(sqlStr)
		resp.Sql = sqlStr
		err = s.client.Exec(resp.Sql, resp.ManyVal...).Error
	case canal.DeleteAction:
		sqlStr = resp.Sql
		sqlStr = stringutil.CutLastString(sqlStr, 3)
		sqlStr += "in (" + strings.Repeat("?,", 5)
		sqlStr = stringutil.CutLastString(sqlStr, 1)
		sqlStr += ")"
		err = s.client.Exec(resp.Sql, resp.ManyId...).Error
	}
	return
}

func (s *MysqlEndpoint) Stock(rows []*global.RowRequest) int64 {
	var err error
	var counter int64
	for _, row := range rows {
		if s.config.IsMysql() {
			if global.RuleInsExist(row.Table.Schema+":*") || global.RuleInsExist(row.Table.Schema+":"+row.Table.Name) {
				resp := s.toBeRespond(row)
				if err = s.Exec(resp); err != nil {
					logutil.Error(err.Error())
				} else {
					counter++
				}
			} else {
				logutil.Warnf("%s schema mismatching", row.RuleKey)
				continue
			}
		}
	}
	return counter
}

func (s *MysqlEndpoint) toBeRespond(row *global.RowRequest) *global.MysqlRespond {
	resp := new(global.MysqlRespond)
	//fmt.Println(row.Action)
	//fmt.Println(row.Row)
	resp.Action = row.Action
	resp.Timestamp = row.Timestamp
	resp.Val = row.Row
	if row.Action != "DDL" {
		resp.Table = row.Table.Name
	}
	resp.Schema = row.Schema
	resp.RuleKey = row.RuleKey
	resp.NumOfSql = 0
	var colList []string
	var colVal []string
	var setTokens []string
	if resp.Action == canal.InsertAction {
		for _, currColumn := range row.Table.Columns {
			colList = append(colList, currColumn.Name)
			colVal = append(colVal, "?")
		}
		//fmt.Println("colList:",colList)
		//fmt.Println("colVal:",colVal)
		resp.Sql = fmt.Sprintf(`insert into %s.%s (%s) values (?)`, row.Table.Schema, row.Table.Name, strings.Join(colList, ", "))
		//fmt.Println("insertSql:", resp.Sql)
		//fmt.Println("row val:",row.Row)

		colList = colList[0:0]
		colVal = colVal[0:0]
	} else if resp.Action == canal.UpdateAction {
		var val []interface{}
		for index, currColumn := range row.Table.Columns {
			if strings.ToLower(currColumn.Name) == "id" {
				resp.Id = row.Row[index]
				continue
			}
			setToken := fmt.Sprintf("%s=?", currColumn.Name)
			setTokens = append(setTokens, setToken)
			val = append(val, row.Row[index])
		}
		val = append(val, resp.Id)
		resp.Val = val
		resp.Sql = fmt.Sprintf(`Update %s.%s set %s where id = ?`, row.Table.Schema, row.Table.Name, strings.Join(setTokens, ", "))
		setTokens = setTokens[0:0]
	} else if resp.Action == canal.DeleteAction {
		for index, currColumn := range row.Table.Columns {
			if strings.ToLower(currColumn.Name) == "id" {
				resp.Id = row.Row[index]
			}
		}
		resp.Sql = fmt.Sprintf(`Delete From %s.%s where id = ?`, row.Table.Schema, row.Table.Name)
	} else { // DDL
		resp.Sql = row.Query
		//resp.SchemaStart = row.SchemaStart
		//resp.SchemaEnd = row.SchemaEnd
	}
	return resp
}
func (s *MysqlEndpoint) DoRetryRow() {
	//失败的sql重试
	//只把执行失败的sql 保存到bolt 每五分钟重试一次 如果重试的第一条sql就报错则不往下重试 等待下一次重试
	//--------------------------------------
	//if err = s.doRetryTask(); err != nil {
	//	logutil.Error(err.Error())
	//	pushFailedRows(rows, s.cached)
	//	return
	//}s
	//--------------------------------------
	interval := time.Duration(s.config.RetryInterval)
	ticker := time.NewTicker(time.Second * interval)
	defer ticker.Stop()
	var size int
	var id uint64
	for {
		size = s.cached.Size()
		select {
		case <-ticker.C:
			var data []byte
			//logutil.Infof("当前重试队列有%d 条数据", size)
			if size > 0 {
				ids := s.cached.IdList()
				for _, id = range ids {
					var err error
					data, err = s.cached.Get(id)
					if err != nil {
						logutil.Warn(err.Error())
						s.cached.Delete(id)
						continue
					}
					var row *global.MysqlRespond
					err = msgpack.Unmarshal(data, &row)
					if err != nil {
						logutil.Warn(err.Error())
						s.cached.Delete(id)
						continue
					}
					if row.Action == canal.InsertAction && row.Sql[0:6] == "insert" {
						//fmt.Println("retry 里的sql ",row.Sql)
						//fmt.Println("sql里的值 ",row.NumOfSql)
						row.Sql = "replace" + row.Sql[6:]
					}
					if err = s.Exec(row); err != nil {
						logutil.Error(err.Error())
						logutil.Infof("重试失败: cached id :%d ,Action: %s, SQL: %s ,VALUES: %v ,PRIM_KEY: %v", id, row.Action, row.Sql, row.Val, row.Id)
						goto LOOP
					}
					logutil.Infof("重试成功: cached id :%d ,Action: %s, SQL: %s ,VALUES: %v ,PRIM_KEY: %v", id, row.Action, row.Sql, row.Val, row.Id)
					//logutil.Infof("cached id :%d , 数据重试成功", id)
					s.cached.Delete(id)
				}
			}
		LOOP:
			//logutil.Infof("cached id :%d ,数据重试失败", id)
		}
	}
}

func (s *MysqlEndpoint) doRetryTask() {
	if s.cached.Size() == 0 {
		return
	}

	if err := s.Ping(); err != nil {

	}

	logutil.Infof("当前重试队列有%d 条数据", s.cached.Size())

	var data []byte
	ids := s.cached.IdList()
	for _, id := range ids {
		var err error
		data, err = s.cached.Get(id)
		if err != nil {
			logutil.Warn(err.Error())
			s.cached.Delete(id)
			continue
		}
		var row global.RowRequest
		err = msgpack.Unmarshal(data, &row)
		if err != nil {
			logutil.Warn(err.Error())
			s.cached.Delete(id)
			continue
		}

		resp := s.toBeRespond(&row)
		if err = s.Exec(resp); err != nil {
			logutil.Error(err.Error())
		}
		logutil.Infof("Action: %s, SQL: %s ,VALUES: %v ,PRIM_KEY: %v", resp.Action, resp.Sql, resp.Val, resp.Id)
		logutil.Infof("cached id :%d , 数据重试成功", id)
		s.cached.Delete(id)
	}

}

func (s *MysqlEndpoint) Close() {
	if s.client != nil {
		if sqlDb, err := s.client.DB(); err != nil {
			logutil.Error(err.Error())
		} else {
			sqlDb.Close()
		}
	}
}

func (s *MysqlEndpoint) getDdlClient(schema string) (err error) {
	if s.ddlClient.schema != schema {
		//dsn := c.MysqlUsername + ":" + c.MysqlPass + "@tcp(" + c.MysqlAddr + ")/" + "test?charset=utf8&parseTime=True&loc=Local"
		dns := s.config.MysqlUsername + ":" + s.config.MysqlPass + "@tcp(" + s.config.MysqlAddr + ")/" + schema + "?charset=utf8&parseTime=True&loc=Local"
		if db, err := getClient(dns); err != nil {
			return err
		} else {
			s.ddlClient.client = db
			s.ddlClient.schema = schema
			return nil
		}
	}
	return nil
}

func getClient(dsn string) (*gorm.DB, error) {
	db, err := gorm.Open(gormMysql.New(gormMysql.Config{
		DSN:                       dsn,   // DSN data source name
		DefaultStringSize:         256,   // string 类型字段的默认长度
		DisableDatetimePrecision:  true,  // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
		DontSupportRenameIndex:    true,  // 重命名索引时采用删除并新建的方式，MySQL 5.7 之前的数据库和 MariaDB 不支持重命名索引
		DontSupportRenameColumn:   true,  // 用 `change` 重命名列，MySQL 8 之前的数据库和 MariaDB 不支持重命名列
		SkipInitializeWithVersion: false, // 根据当前 MySQL 版本自动配置
	}), &gorm.Config{})
	sqlDB, _ := db.DB()
	// SetMaxIdleConns sets the maximum number of connections in the idle connection pool.
	sqlDB.SetMaxIdleConns(40)
	// SetMaxOpenConns sets the maximum number of open connections to the database.
	sqlDB.SetMaxOpenConns(100)
	// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
	sqlDB.SetConnMaxLifetime(time.Hour)
	return db, err
}
