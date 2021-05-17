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
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"go-mysql-transfer/global"
	"go-mysql-transfer/util/logutil"
)

type handler struct {
	transfer *TransferService

	requestQueue    chan interface{}
	hashMap         *global.HashMap
	ddLRequestQueue chan interface{}
}

func (h *handler) OnRotate(e *replication.RotateEvent) error {
	logutil.Info("拉取下一个binlog文件: " + string(e.NextLogName))
	return h.transfer.ctx.Err()
}

func (h *handler) OnTableChanged(schema, table string) error {
	//更新表的元数据
	err := h.transfer.updateRule(schema, table)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (h *handler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	var err error
	var is bool
	pos := global.PosRequest{
		Name:      nextPos.Name,
		Pos:       nextPos.Pos,
		Force:     true,
		Timestamp: nextPos.Timestamp,
	}
	//rr := global.RowRequestPool.Get().(*global.RowRequest)

	//判断 Timestamp 在 bolt中是否存在  , 若返回为true，存在的话直接放弃
	if is, err = storage.ExistsTimestamp(nextPos.Timestamp); !is {
		rr := &global.RowRequest{}
		schema := string(queryEvent.Schema)
		logutil.Info(schema)
		ruleKey := global.RuleKey(schema, "")
		ddlSql := string(queryEvent.Query)
		//ddlSql = "use " + schema + ";" + ddlSql + ";use test"
		//rr.SchemaStart = "use " + schema
		//rr.SchemaEnd = "use test"
		rr.Query = ddlSql
		rr.Action = "DDL"
		rr.RuleKey = ruleKey
		rr.Schema = schema
		// rr 带入 Timestamp 用于判断该DDL是否执行过
		//rr.Timestamp = pos.Timestamp

		h.ddLRequestQueue <- rr

		//logutil.Info("ddl:" + ddlSql)
		//logutil.Info("ddl into chan of ddLRequestQueue")

		global.GlobalChangeChan.Mutex.Lock()
		global.GlobalChangeChan.DdlControl = true
		global.GlobalChangeChan.Mutex.Unlock()
		if global.GlobalChangeChan.DdlControl {
			//logutil.Info("控制等待ddl执行完成")
			<-global.GlobalChangeChan.DdlControlChan
			//logutil.Info("ddl完成，阻塞接触")
		}

		//logutil.Info("ddl一次完成")
		//h.ddLRequestQueue <- pos
		//DDL的点位 需要及时记录，不能延迟
		//logutil.Infof("after DDL save position %s %d", current.Name, current.Pos)

		//判断时间戳  大于0正常
		if pos.Timestamp > 0 {
			if err = h.transfer.positionStorage.RecordPosition(pos); err != nil {
				//异常报错，退出线程
				logutil.Errorf("RecordPosition wrong !,after DDL save sync position %s err %v, close sync", pos, err)
				h.transfer.cancelFunc()
			} else {
				logutil.Info("Timestamp:" + strconv.Itoa(int(pos.Timestamp)) + ",Datetime:" + util.TimestampToDatetime(int64(pos.Timestamp)) + ",PosName:" + pos.Name + ",Pos:" + strconv.Itoa(int(pos.Pos)) + "\n" + "DDL: " + rr.Query)
			}
		} else { //小于等于0 异常退出
			logutil.Errorf("Must Timestamp > 0 !,after DDL save sync position %s , close sync", pos)
			h.transfer.cancelFunc()
		}
	}
	return h.transfer.ctx.Err()
}

func (h *handler) OnXID(nextPos mysql.Position) error {
	h.requestQueue <- global.PosRequest{
		Name:      nextPos.Name,
		Pos:       nextPos.Pos,
		Force:     false,
		Timestamp: nextPos.Timestamp,
	}

	return h.transfer.ctx.Err()
}

//要从事件里面取事件的对应事件 ，通过事件的对应时间 去取 postion 的五分钟前的点位，这就需要服务器的时间基本一致
func (h *handler) OnRow(e *canal.RowsEvent) error {
	//fmt.Println("拉取到一条记录")
	//取事件归属表的元数据
	//ruleKey := global.RuleKey(e.Table.Schema, e.Table.Name)
	//判断 事件归属表 是不是在监听范围内
	//if !global.RuleInsExist(ruleKey) {
	//	return nil
	//}
	//fmt.Println("OnRow")
	//fmt.Println(e.Table.Schema + ":" + e.Table.Name)
	ruleKey := strings.ToLower(e.Table.Schema + ":" + e.Table.Name)
	if !(global.RuleInsExist(e.Table.Schema+":table_all_in") || global.RuleInsExist(e.Table.Schema+":"+e.Table.Name)) {
		return nil
	}

	record := fmt.Sprintf("%v %v %s %v\n", e.Table.Schema, e.Table.Name, e.Action, e.Rows)
	fmt.Println("OnRow", record)
	return nil

	//var requests []*global.RowRequest
	//var rr *global.RowRequest

	//取行id
	index := getIDIndex(e)

	if e.Action == canal.UpdateAction {
		//fmt.Println(e.Rows)
		//取行id
		for i := 0; i < len(e.Rows); i++ {
			if (i+1)%2 == 0 {
				rr := &global.RowRequest{}
				//rr := global.RowRequestPool.Get().(*global.RowRequest)
				rr.RuleKey = ruleKey
				rr.Action = e.Action
				rr.Table = e.Table
				if e.Header != nil {
					rr.Timestamp = e.Header.Timestamp
				}
				if h.transfer.config.IsRedis() {
					rr.OldRow = e.Rows[i-1]
				}
				rr.Row = e.Rows[i]
				rr.IdNum = rr.Row[index]

				getTableAndIDToHash(rr, h)
				h.requestQueue <- rr
			}
		}
	} else {
		for _, row := range e.Rows {
			rr := &global.RowRequest{}
			rr.RuleKey = ruleKey
			rr.Action = e.Action
			rr.Table = e.Table
			if e.Header != nil {
				rr.Timestamp = e.Header.Timestamp
			}
			rr.Row = row
			rr.IdNum = rr.Row[index]
			getTableAndIDToHash(rr, h)
			h.requestQueue <- rr
		}
	}

	//rr := &global.RowRequest{}
	//
	//
	////格式化请求
	//if e.Action == canal.UpdateAction {
	//	//fmt.Println(e.Rows)
	//	rr.RuleKey = ruleKey
	//	rr.Action = e.Action
	//	rr.Table = e.Table
	//	if h.transfer.config.IsRedis() {
	//		rr.OldRow = e.Rows[0]
	//	}
	//	rr.Row = e.Rows[1]
	//} else {
	//	rr.RuleKey = ruleKey
	//	rr.Action = e.Action
	//	rr.Table = e.Table
	//	rr.Row = e.Rows[0]
	//}
	////取行id
	//for index, column := range rr.Table.Columns {
	//	if strings.ToLower(column.Name) == "id" {
	//		rr.IdNum = rr.Row[index]
	//		break
	//	}
	//}
	//对表名和ID的字符做hash
	//idString, ok := rr.IdNum.(string)
	//if !ok {
	//	logutil.Info("id 转换出错:" + idString)
	//	idString = ""
	//}
	//var idString string
	//switch idValue := rr.IdNum.(type) {
	//case int:
	//	idString = fmt.Sprintf("%d", idValue)
	//case int64:
	//	idString = fmt.Sprintf("%d", idValue)
	//case int32:
	//	idString = fmt.Sprintf("%d", idValue)
	//case string:
	//	idString = fmt.Sprintf("%s", idValue)
	//case float64:
	//	idString = fmt.Sprintf("%g", idValue)
	//default:
	//	idString = "no"
	//}
	//rr.IdString = idString
	//logutil.Info("SQL 的hash:" + idString)
	//hashNum := util.Hash(strings.ToLower(rr.Table.Name) + idString)

	//logutil.Info("SQL 的 hashcode 前:" + strings.ToLower(rr.Table.Name) + idString)
	//logutil.Info("SQL 的 hashcode 后:" + strconv.Itoa(hashNum))

	//rr.Hash = hashNum % h.hashMap.ChanLen
	//logutil.Info("SQL 取模 后:" + strconv.Itoa(rr.Hash))
	////h.hashMap.Array[index] <- rr
	//if e.Header != nil {
	//	logutil.Infof("binlog 时间戳 ：%d ", e.Header.Timestamp)   //1618887285
	//}                                                            1618889309

	//h.requestQueue <- rr
	////global.RowRequestPool.Put(rr)
	return h.transfer.ctx.Err()
}

func (h *handler) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

func (h *handler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

func (h *handler) String() string {
	return "TransferHandler"
}

func (h *handler) startRequestQueueListener() {
	go func() {

		h.transfer.listenerStarted.Store(true)

		bulkSize := h.transfer.config.BulkSize

		//刷binlog的时间间隔
		//interval := time.Duration(h.transfer.config.FlushBulkInterval)
		//
		ticker := time.NewTicker(time.Millisecond * 1000)
		defer ticker.Stop()

		defer h.transfer.wg.Done()

		lastSavedTime := time.Now()

		var current global.PosRequest
		//requests := make([]*global.RowRequest, 0, bulkSize)
		//DDLRequests := make([]*global.RowRequest, 0, bulkSize)

		//binlog事件需求队列
		//var message = make(chan []*global.RowRequest, bulkSize)

		//DDL事件需求队列
		var DDLMessage = make(chan *global.RowRequest, bulkSize)

		//初始化控制器
		global.GlobalChangeChan = global.ChangeChan{
			DdlControl:     false,
			DdlControlChan: make(chan struct{}),
		}

		//创建 消费binlog事件 线程
		for i := 0; i < h.transfer.config.Threads; i++ {
			go h.transfer.endpoint.Consume(i, h.hashMap.Array[i], global.GlobalChangeChan)
		}

		//创建 DDL专门处理线程
		go h.transfer.endpoint.Consume(100, DDLMessage, global.GlobalChangeChan)

		//创建 执行失败需要重试的sql 线程
		//go h.transfer.endpoint.DoRetryRow()

		isMysql := h.transfer.config.IsMysql()

		for {
			//DDLSavePos := false
			//logutil.Info("监听线程循环一次")
			//判断是否是DDL
			if global.GlobalChangeChan.DdlControl {
				//logutil.Info("监听线程 into ddl")
				select {
				case ddlRequest := <-h.ddLRequestQueue:
					switch ddlRequest := ddlRequest.(type) {
					//case global.PosRequest:
					//	current = mysql.Position{
					//		Name: ddlRequest.Name,
					//		Pos:  ddlRequest.Pos,
					//	}
					//DDLSavePos = true
					case *global.RowRequest:
						if isMysql {
							//DDLRequests = append(DDLRequests, ddlRequest)
							DDLMessage <- ddlRequest
						}
					}
				//时间间隔 不然上面会堵死
				case <-ticker.C:
					//logutil.Info("等待DDL执行完成")
				}
				//if len(DDLRequests) > 0 {
				//	DDLMessage <- DDLRequests
				//	DDLRequests = make([]*global.RowRequest, 0, bulkSize)
				//}
				//if DDLSavePos {
				//	logutil.Infof("after DDL save position %s %d", current.Name, current.Pos)
				//	if err := h.transfer.positionStorage.Save(current); err != nil {
				//		logutil.Errorf("after DDL save sync position %s err %v, close sync", current, err)
				//		h.transfer.cancelFunc()
				//		return
				//	}
				//}
			} else {

				//logutil.Info("监听线程 into dml")
				//needFlush := false
				needSavePos := false
				select {
				//非ddl线程
				case v := <-h.requestQueue:
					switch v := v.(type) {
					case global.PosRequest:
						now := time.Now()
						//三分钟一次打印 position
						if v.Force || now.Sub(lastSavedTime) > 2*time.Minute {
							lastSavedTime = now
							//needFlush = true
							needSavePos = true
							current = global.PosRequest{
								Name:      v.Name,
								Pos:       v.Pos,
								Timestamp: v.Timestamp,
								Force:     false,
							}
						}
					case *global.RowRequest:
						if isMysql {
							//搜集binlog 每次搜集满 BulkSize 就主动刷binlog 不等刷新间隔
							//requests = append(requests, v)
							//needFlush = len(requests) >= h.transfer.config.BulkSize

							h.hashMap.Array[v.Hash] <- v

							//logutil.Info("输出sql ID = " + v.IdString + ",hash = " + strconv.Itoa(v.Hash))

						}
					}
				//case <-ticker.C:
				//	needFlush = true
				case <-h.transfer.ctx.Done():
					return

				//时间间隔 保证一秒一循环  不然无法进入ddl判断
				case <-ticker.C:
					//logutil.Info("没有需要执行的dml")

					//没有需要执行的sql，上报当前时间戳
					global.ExecuteSQLTimestamp = uint32(time.Now().Unix())
					//logutil.Infof("binlog 时间戳 ：%d ", e.Header.Timestamp)   //1618887285

				}
				//if needFlush {
				//	if len(requests) > 0 {
				//		h.hashMap.Array[v.hash] <- requests
				//		requests = make([]*global.RowRequest, 0, bulkSize)
				//	}
				//}
				//三分钟记录一次 position
				if needSavePos {
					//判断 时间戳 是否正常，正常则保存到存储且输出到日志，异常则不保存丢弃
					if current.Timestamp > 0 {
						if err := h.transfer.positionStorage.RecordPosition(current); err != nil { //报错则打错误日志 退出  不报错则打正常日志
							logutil.Errorf("row event save sync position %s err %v, close sync", current, err)
							h.transfer.cancelFunc()
							return
						} else {
							logutil.Info("Timestamp:" + strconv.Itoa(int(current.Timestamp)) + ",Datetime:" + util.TimestampToDatetime(int64(current.Timestamp)) + ",PosName:" + current.Name + ",Pos:" + strconv.Itoa(int(current.Pos)))
						}
					}
				}
			}
		}
	}()

}

//	for i := 0; i < len(e.Rows); i++ {
//		if (i+1)%2 == 0 {
//			rr := global.RowRequestPool.Get().(*global.RowRequest)
//			rr.RuleKey = ruleKey
//			rr.Action = e.Action
//			rr.Table = e.Table
//			if h.transfer.config.IsRedis() {
//				rr.OldRow = e.Rows[i-1]
//			}
//			rr.Row = e.Rows[i]
//			requests = append(requests, rr)
//			global.RowRequestPool.Put(rr)
//		}
//	}

//func SavePosition(pos mysql.Position) error {
//
//}

func getIDIndex(e *canal.RowsEvent) int {
	//取行id
	for index, column := range e.Table.Columns {
		if strings.ToLower(column.Name) == "id" {
			return index
		}
	}
	return 0
}

func getTableAndIDToHash(rr *global.RowRequest, h *handler) *global.RowRequest {
	//对表名和ID的字符做hash
	var idString string
	switch idValue := rr.IdNum.(type) {
	case int:
		idString = fmt.Sprintf("%d", idValue)
	case uint:
		idString = fmt.Sprintf("%d", idValue)
	case int64:
		idString = fmt.Sprintf("%d", idValue)
	case uint64:
		idString = fmt.Sprintf("%d", idValue)
	case int32:
		idString = fmt.Sprintf("%d", idValue)
	case uint32:
		idString = fmt.Sprintf("%d", idValue)
	case int8:
		idString = fmt.Sprintf("%d", idValue)
	case uint8:
		idString = fmt.Sprintf("%d", idValue)
	case uint16:
		idString = fmt.Sprintf("%d", idValue)
	case int16:
		idString = fmt.Sprintf("%d", idValue)
	case string:
		idString = fmt.Sprintf("%s", idValue)
	case float64:
		idString = fmt.Sprintf("%g", idValue)
	default:
		idString = "no"
	}
	rr.IdString = idString
	hashNum := util.Hash(strings.ToLower(rr.Table.Name) + idString)
	rr.Hash = hashNum % h.hashMap.ChanLen
	return rr
}

//
////对表名和ID的字符做hash
////idString, ok := rr.IdNum.(string)
////if !ok {
////	logutil.Info("id 转换出错:" + idString)
////	idString = ""
////}
//var idString string
//switch idValue := rr.IdNum.(type) {
//case int:
//idString = fmt.Sprintf("%d", idValue)
//case int64:
//idString = fmt.Sprintf("%d", idValue)
//case int32:
//idString = fmt.Sprintf("%d", idValue)
//case string:
//idString = fmt.Sprintf("%s", idValue)
//case float64:
//idString = fmt.Sprintf("%g", idValue)
//default:
//idString = "no"
//}
//rr.IdString = idString
////logutil.Info("SQL 的hash:" + idString)
//hashNum := util.Hash(strings.ToLower(rr.Table.Name) + idString)
////hashNum := fmt.Sprintf("%d",util.Hash(strings.ToLower(rr.Table.Name) + idString))
////logutil.Info("SQL 的 hashcode 前:" + strings.ToLower(rr.Table.Name) + idString)
////logutil.Info("SQL 的 hashcode 后:" + strconv.Itoa(hashNum))
//
//rr.Hash = hashNum % h.hashMap.ChanLen
////logutil.Info("SQL 取模 后:" + strconv.Itoa(rr.Hash))
////h.hashMap.Array[index] <- rr
////if e.Header != nil {
////	logutil.Infof("binlog 时间戳 ：%d ", e.Header.Timestamp)   //1618887285
////}                                                            1618889309
