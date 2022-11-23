# goMysqlSync
golang mysql to mysql 轻量级多线程库表级数据同步

### 测试运行
1. 设置当前binlog位置并且开始运行  

`go run main.go -position mysql-bin.000001 1 1619431429`

2. 查询当前binlog位置，参数n为秒数，查询结果为n秒前的binlog位置

`go run main.go -status n`

3. 开始同步，会先进行一次全量同步，增量同步也会自动执行       

`go run main.go -full`

4. 若程序掉线，可以继续执行，回滚到 掉线时 900秒 之前的binlog位置重新开始同步

`go run main.go`

### 编译运行

set GOOS=linux

set GOARCH=amd64

go build -o "goMysqlSync"

chmod +x goMysqlSync

编辑配置文件 app.yml
```yaml
#源 mysql 配置
addr: 127.0.0.1:3306
user: test_sync
pass: test_sync
charset : utf8
#伪装id
slave_id: 520               #slave ID

#系统相关配置
db_days: 30
data_dir:                  #/usr/local/goMysqlSync #应用产生的数据存放地址，包括日志、缓存数据等，默认当前运行目录下store文件夹
logger:
  file_name: system.log
  level: debug  #日志级别；支持：debug|info|warn|error，默认info
  store:        #/usr/local/goMysqlSync

#prometheus相关配置
label: goMysqlSync                 #prometheus exporter的tag
enable_exporter: true       #是否启用prometheus exporter，默认false
exporter_addr: 9595         #prometheus exporter端口，默认9595

#目标类型
target: mysql
mysql_addrs: 127.0.0.2:3306   #mysql地址，多个用逗号分隔
mysql_username: test_sync     #mysql用户名
mysql_pass: test_sync         #mysql密码
threads: 20                   #增量数据回放多线程数量
record_rows: 255              #增量数据回放每批次大小
dump_threads: 40              #全量同步线程数
dump_record_rows: 1000        #全量同步每批次大小

#规则配置
rule:
  -
    schema: db_student            #源库名
    target_schema: db_student_bak #目标库名
    table: student                #源表名
    target_table: student_bak     #目标表名
  -
    schema: db_student            #源库名
    target_schema: db_student_bak #目标库名
    table: budget_dict            #源表名
    target_table: budget_dict     #目标表名
  -
    schema: db_student            #源库名
    target_schema: db_student_bak #目标库名
    table: economy_section           #源表名
    target_table: economy_section    #目标表名
  -
    schema: db_student            #源库名
    target_schema: db_student_bak #目标库名
    table: funds_nature           #源表名
    target_table: funds_nature    #目标表名
```
启动
```shell
nohup ./goMysqlSync  --sync-mode=6  >/dev/null 2>nohup.goMysqlSync.log & 
```


### 监控

http://localhost:9595/metrics

监控项包括 机器的CPU、磁盘情况 和 每秒网络流量值，以及 目标库的连接状态 和 程序每秒同步速度

### 注意事项:

1. 需要使github.com/siddontang/go-mysql项目的Position结构体支持时间戳

修改 "github.com/siddontang/go-mysql/mysql/position.go"文件  
``` 
type Position struct {
	Name string
	Pos  uint32
	Timestamp uint32  #新加
}

func (p Position) String() string {
	return fmt.Sprintf("(%s, %d, %d)", p.Name, p.Pos,p.Timestamp)   #修改
}
```
修改 "github.com/siddontang/go-mysql/canal/sync.go"库文件  
```
curPos := pos.Pos
// next binlog pos
pos.Pos = ev.Header.LogPos
pos.Timestamp = ev.Header.Timestamp   #新加
```

2. 由于项目基于mysqldump，必须要有mysqldump的工具文件，并且在配置文件中指定路径，本项目 linux和windows 都支持

3. mysql 的binlog格式必须是 row 模式，不支持外键约束，数据表必须有id字段类型为整型并且为主键

4. 修改go-mysql项目的 github.com/siddontang/go-mysql/canal/sync.go

支持 CREATE INDEX INDEX_1NAME3 ON `courses12`  (score) 和 DROP INDEX INDEX_1NAME3 ON `courses12`;形式的sql

```
func parseStmt(stmt ast.StmtNode) (ns []*node) {
	switch t := stmt.(type) {
	case *ast.RenameTableStmt:
		for _, tableInfo := range t.TableToTables {
			n := &node{
				db:    tableInfo.OldTable.Schema.String(),
				table: tableInfo.OldTable.Name.String(),
			}
			ns = append(ns, n)
		}
	case *ast.AlterTableStmt:
		n := &node{
			db:    t.Table.Schema.String(),
			table: t.Table.Name.String(),
		}
		ns = []*node{n}
	case *ast.DropTableStmt:
		for _, table := range t.Tables {
			n := &node{
				db:    table.Schema.String(),
				table: table.Name.String(),
			}
			ns = append(ns, n)
		}
	case *ast.CreateTableStmt:
		n := &node{
			db:    t.Table.Schema.String(),
			table: t.Table.Name.String(),
		}
		ns = []*node{n}
	case *ast.TruncateTableStmt:
		n := &node{
			db:    t.Table.Schema.String(),
			table: t.Table.Schema.String(),
		}
		ns = []*node{n}
	case *ast.CreateIndexStmt:                            #新加
		n := &node{                                   #新加
			db:    t.Table.Schema.String(),       #新加
			table: t.Table.Schema.String(),       #新加
		}                                             #新加
		ns = []*node{n}                               #新加
	case *ast.DropIndexStmt:                              #新加
		n := &node{                                   #新加
			db:    t.Table.Schema.String(),       #新加
			table: t.Table.Schema.String(),       #新加
		}                                             #新加
		ns = []*node{n}	                              #新加
	}                          
	return
}
```

5. 项目必须有 test 库，用来做执行跳板库，项目不会对 test 库的数据做任何操作。 

6. 源库需要的账号权限 

   ```
   GRANT SELECT, PROCESS, REPLICATION SLAVE, REPLICATION CLIENT, RELOAD ON *.* TO 'xxx'@'%' IDENTIFIED BY 'xxx';

   FLUSH  PRIVILEGES;
   ```

### 本项目基于二次开发:
<a href="https://github.com/go-mysql-org/go-mysql" target="_blank">github.com/siddontang/go-mysql</a>

<a href="https://github.com/wj596/go-mysql-sync" target="_blank">github.com/wj596/go-mysql-sync</a>

