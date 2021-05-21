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

chmod 777 goMysqlSync

编辑配置文件 app.yml

nohup ./goMysqlSync  -full  >/dev/null 2>goMysqlSync.log & 

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

4. 修改go-mysql支持 CREATE INDEX INDEX_1NAME3 ON `courses12`  (score) 和 DROP INDEX INDEX_1NAME3 ON `courses12`;

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

### 本项目基于二次开发:
<a href="https://github.com/go-mysql-org/go-mysql" target="_blank">github.com/siddontang/go-mysql</a>

<a href="https://github.com/wj596/go-mysql-transfer" target="_blank">github.com/wj596/go-mysql-transfer</a>

