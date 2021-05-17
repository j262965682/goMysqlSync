# goMysqlSync
mysql->mysql多线程库表级数据同步

### 测试运行
1. 设置当前binlog位置并且开始运行  

`go run main.go -position mysql-bin.000469 324783 1619431429`

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

编辑配置文件 app.yml

nohup ./goMysqlSync  -full  >/dev/null 2>goMysqlSync.log & 

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


### 本项目基于二次开发:
github.com/siddontang/go-mysql

github.com/wj596/go-mysql-transfer
