# goMysqlSync
mysql->mysql多线程库表级数据同步


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
--- 
2. 由于项目基于mysqldump，必须要有mysqldump的工具文件，并且在配置文件中指定路径，本项目 linux和windows 都支持
--- 
3. mysql 的binlog格式必须是 row 模式
--- 

### 本项目基于:
github.com/siddontang/go-mysql
github.com/wj596/go-mysql-transfer
