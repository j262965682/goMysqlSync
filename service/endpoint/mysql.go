package endpoint

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/mattn/go-colorable"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/schema"
	"github.com/vmihailenco/msgpack"
	"go-mysql-sync/global"
	"go-mysql-sync/storage"
	"go-mysql-sync/util"
	"go-mysql-sync/util/logutil"
	"go-mysql-sync/util/stringutil"
	gormMysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"strings"
	"sync"
	"time"
)

var GFullProgress = FullOfProgress{}
var InitPos = mysql.Position{}

type FullOfProgress struct {
	Status     bool                   //全量同步是否完成
	Table      map[string]TableStatus // 每个表的行数，string为表的 schema_name.table_name
	AllRow     int64                  // 总行数
	CurrentRow int64                  // 当前已同步的行数
	Lock       sync.Mutex             //线程锁
}

type TableStatus struct {
	Schema       string
	TargetSchema string
	Name         string
	TargetName   string
	Rows         int64  // 每个表的行数
	Done         bool   // 每个表是否完成全量
	Sql          string //建表语句
	Columns      []schema.TableColumn
	RangeId      RangeID //id范围
}

type RangeID struct {
	Max int64 `gorm:"column:max"`
	Min int64 `gorm:"column:min"`
}

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
	dsn := c.MysqlUsername + ":" + c.MysqlPass + "@tcp(" + c.MysqlAddr + ")/" + "demo_sync?charset=utf8&parseTime=True&loc=Local"
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
	//搜集库表信息 内部会判断是否需要同步表结构标识
	if err = s.SyncTableStructure(); err != nil {
		return err
	}
	return nil
}

func (s *MysqlEndpoint) GetMasterClient() (db *gorm.DB, err error) {
	dsn := s.config.User + ":" + s.config.Password + "@tcp(" + s.config.Addr + ")/" + "demo_sync?charset=utf8&parseTime=True&loc=Local"
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

	var tableDesc global.TableDesc
	masterDb, err = s.GetMasterClient()
	tableList := make([]global.Tables, 0) //映射sql的结果集

	tableDump := make(map[string]TableStatus)
	createSchemas := make([]string, 0)
	// 先 检测是否有id主键 再 拉取建库和建表sql 再执行建库和建表sql
	// 拉取 建库和建表sql
	for _, rule := range global.RuleInsList() {
		// 先把加入 库列表  目的库名
		if result := IsContain(createSchemas, rule.TargetSchema); !result {
			createSchemas = append(createSchemas, rule.TargetSchema)
		}
		// 某个库下的所有表
		if rule.Table == "table_all_in" {
			// 获取全库的表列表
			masterDb.Table("information_schema.tables").Select("table_name").Where("TABLE_SCHEMA = ?", rule.Schema).Scan(&tableList)
			logutil.BothInfof("获取 %s 库表信息", rule.Schema)
			for _, table := range tableList {
				schemaTable := rule.Schema + "." + table.TableName
				// 检查表是否有 id主键
				masterDb.Table("information_schema.COLUMNS").Where("TABLE_SCHEMA = ? and table_name = ? and COLUMN_NAME = 'id' and COLUMN_KEY = 'PRI'", rule.Schema, table.TableName).Count(&count)
				if count == 0 {
					logutil.Errorf(rule.Schema + "." + table.TableName + "没有主键id")
					panic(rule.Schema + "." + table.TableName + "没有主键id")
				}
				// 拉取 create table SQL
				sql := "SHOW CREATE TABLE " + schemaTable + ";"
				masterDb.Raw(sql).Scan(&tableDesc)
				// 获取表的行数
				masterDb.Table(schemaTable).Count(&count)

				tableDump[schemaTable] = TableStatus{
					Schema:       rule.Schema,
					TargetSchema: rule.TargetSchema,
					Name:         table.TableName,
					TargetName:   rule.TargetTable,
					Rows:         count,
					Done:         false,
					Sql:          tableDesc.CreateTable,
				}
			}
		} else {
			logutil.BothInfof("获取 %s.%s 表信息", rule.Schema, rule.Table)
			// 单表
			schemaTable := rule.Schema + "." + rule.Table
			// TargetSchemaTable := rule.TargetSchema + "." + rule.TargetTable
			// 检查表是否有 id主键
			masterDb.Table("information_schema.COLUMNS").Where(`TABLE_SCHEMA = ? and table_name = ? and COLUMN_NAME = 'id'`, rule.Schema, rule.Table).Count(&count)
			if count == 0 {
				logutil.Error(rule.Schema + "." + rule.Table + "没有主键id" + err.Error())
				panic(rule.Schema + "." + rule.Table + "没有主键id")
			}
			// 拉取 create table SQL
			sql := "SHOW CREATE TABLE " + schemaTable + ";"
			masterDb.Raw(sql).Scan(&tableDesc)
			// 获取表的行数
			masterDb.Table(schemaTable).Count(&count)

			tableDump[schemaTable] = TableStatus{
				Schema:       rule.Schema,
				TargetSchema: rule.TargetSchema,
				Name:         rule.Table,
				TargetName:   rule.TargetTable,
				Rows:         count,
				Done:         false,
				Sql:          tableDesc.CreateTable,
			}
		}
	}

	GFullProgress.Table = tableDump
	// 根据 表结构标识 判断是否需要执行表结构
	if util.GSchemaFlag {
		// 执行建库  目标库
		for _, schema := range createSchemas {
			logutil.BothInfof("新建 %s 库", schema)
			// CREATE DATABASE IF NOT EXISTS mytestdb CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_general_ci';
			CreateDatabaseNotExistsSql := `CREATE DATABASE IF NOT EXISTS ` + strings.ToLower(schema) + ` CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_general_ci';`
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
		// 新建表    GFullProgress.Table
		for _, status := range GFullProgress.Table {
			useSql := "use " + status.TargetSchema + ";"
			if err = s.client.Exec(useSql).Error; err != nil {
				logutil.Error("表结构初始化出错: use  ;" + status.TargetSchema + ";" + err.Error())
				return err
			}
			// 要把表结构里第一个匹配到的表名 `table_name` 替换成新表名
			createSql := createTableSqlChangeTableName(status.Sql, status.Name, status.TargetName)

			logutil.Infof("表结构初始化: " + status.TargetSchema + "." + status.TargetName + ":" + createSql)
			if err = s.client.Exec(createSql).Error; err != nil {
				logutil.Error("表结构初始化出错: create table " + status.TargetSchema + "." + status.TargetName + ";" + err.Error())
				return err
			}
		}
		//先开启外键约束
		if db := s.client.Exec(fkOnSql); db.Error != nil {
			return db.Error
		}
		return err
	}
	return nil
}

func (s *MysqlEndpoint) Ping() error {
	return s.client.Exec("select 1").Error
}

// Consume 消费 binlog 事件
func (s *MysqlEndpoint) Consume(n int, message chan *global.RowRequest, changeChan global.ChangeChan) {
	var err error
	//判断是不是DDL，如果是100 则是DDL
	//执行sql结束 需要上报时间戳
	if n == 100 {
		logutil.Info("启动 ddl处理线程")
		for row := range message {
			//logutil.Info("ddl处理线程:" + row.Query)
			//fmt.Println("从执行队列中拿到请求")
			resp := s.toBeRespond(row)
			//logutil.Info("开始执行sql")
			err = s.Exec(resp)
			if err != nil {
				panic(err)
			} else {
				//logutil.Info("ddl执行完成")
				//fmt.Println("请求执行结束")
				//fmt.Println("空结构体放入 控制chan")
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
			if err != nil {
				logutil.Error(errors.ErrorStack(errOfAfter))
				panic(errOfAfter)
			}
		} else {
			logutil.Error(errors.ErrorStack(errOfAfter))
			panic(errOfAfter)
		}
		//logutil.Error(errors.ErrorStack(errOfAfter))
		//logutil.Info("执行sql报错写入存储，后续待执行 ID = " + row.Table)
		//pushFailedRow(row, s.cached)
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
		//logutil.Info("binlog延迟(s)" + string(global.ExecuteSQLTimestamp))
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

func (s *MysqlEndpoint) StockExecSql(sql string, valuesList []interface{}) (int64, error) {
	db := s.client.Exec(sql, valuesList...)
	if db.Error != nil {
		logutil.Error(errors.ErrorStack(db.Error))
		return 0, db.Error
	}
	return db.RowsAffected, nil
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
	return
}

func (s *MysqlEndpoint) getDdlClient(schema string) (err error) {
	//判断当前数据库连接是否可用
	ok := util.IsNil(s.ddlClient.client)
	if s.ddlClient.schema != schema || !ok {
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

func IsContain(items []string, item string) bool {
	for _, eachItem := range items {
		if eachItem == item {
			return true
		}
	}
	return false
}

func (s *MysqlEndpoint) FindSQLToMap(sql string) ([]map[string]interface{}, error) {
	var sqlResult []map[string]interface{}
	// s.client.Select(sql).Find(&sqlResult).Debug()
	db, err := s.GetMasterClient()
	if err != nil {
		return nil, err
	}
	db.Raw(sql).Find(&sqlResult)
	return sqlResult, nil
}

func insertBeReplace(insertSQL string) string {
	return "replace" + insertSQL[6:]
}

func createTableSqlChangeTableName(sql string, OldTableName string, NewTableName string) string {
	OldTableNameSwap := "`" + OldTableName + "`"
	NewTableNameSwap := "`" + NewTableName + "`"
	return strings.Replace(sql, OldTableNameSwap, NewTableNameSwap, 1)
}
