package util

import (
	"errors"
	"hash/crc32"
	"reflect"
	"regexp"
	"strings"
	"time"
)

var G_full bool

func IsNil(i interface{}) bool {
	vi := reflect.ValueOf(i)
	if vi.Kind() == reflect.Ptr {
		return vi.IsNil()
	}
	return false
}

func StoreVal(val bool) {
	G_full = val
}

func Hash(str string) int {
	v := int(crc32.ChecksumIEEE([]byte(str)))
	if v >= 0 {
		return v
	}
	if -v >= 0 {
		return -v
	}
	// v == MinInt
	return 0
}

func TimestampToDatetime(Timestamp int64) string {
	timeLayout := "2006-01-02 15:04:05"
	return time.Unix(Timestamp, 0).Format(timeLayout)
}

func CaptureTableName(sql string) (tableName string, err error) {
	var indexOfTable int
	var indexOfOn int

	sql = strings.Replace(sql, `"`, "", -1)
	sql = strings.Replace(sql, `'`, "", -1)
	sql = strings.Replace(sql, "`", "", -1)
	reg := regexp.MustCompile("\\s+")
	sqlStr := reg.ReplaceAllString(sql, " ")
	//fmt.Println(sqlStr)
	indexOfTable = -1
	indexOfOn = -1
	//取 table关键字 后面一个字符串，如果有‘.’,则取‘.’后的字符
	array := strings.Fields(sqlStr)
	for index, value := range array {
		if strings.ToLower(value) == "on" {
			indexOfOn = index
		}
		if strings.ToLower(value) == "table" {
			indexOfTable = index
			break
		}
	}

	//fmt.Println("indexOfTable:",indexOfTable)
	//fmt.Println("indexOfOn:",indexOfOn)
	//fmt.Println("array:",array)

	//如果没找到 table 关键字，则找 ON 关键字
	if indexOfTable > -1 {
		tableName = indexToTableName(indexOfTable, array)
	} else if indexOfTable == -1 && indexOfOn > -1 {
		tableName = indexToTableName(indexOfOn, array)
	} else {
		return "", errors.New("DDL解析表名报错")
	}
	return tableName, nil
}

func indexToTableName(index int, array []string) string {
	tableName := array[index+1]
	indexOfPoint := strings.Index(tableName, ".")
	if indexOfPoint >= 0 {
		tableName = tableName[indexOfPoint+1:]
		if tableName[len(tableName)-1:] == ";" {
			tableName = tableName[:len(tableName)-1]
		}
	}
	return tableName
}

//func (s *MysqlEndpoint) Consume(n int,message chan []*global.RowRequest) {
//	var err error
//	for rows := range message {
//		//失败的sql重试
//		//只把执行失败的sql 保存到bolt 每五分钟重试一次 如果重试的第一条sql就报错则不往下重试 等待下一次重试
//		//--------------------------------------
//		//if err = s.doRetryTask(); err != nil {
//		//	logutil.Error(err.Error())
//		//	pushFailedRows(rows, s.cached)
//		//	return
//		//}
//		//--------------------------------------
//		expect := true
//		for _, row := range rows {
//			exportActionNum(row.Action, row.RuleKey)
//			resp := s.toBeRespond(row)
//			err = s.Exec(resp)
//			if err != nil {
//				logutil.Error(errors.ErrorStack(err))
//				expect = false
//				break
//			}
//		}
//		if !expect {
//			pushFailedRows(rows, s.cached)
//		} else {
//			logutil.Infof("%d号线程，处理完成 %d 条数据", n, len(rows))
//		}
//	}
//}

//func pushFailedRows(rs []*global.RowRequest, cached *storage.BoltRowStorage) {
//	logutil.Infof("%d 条数据处理失败，插入重试队列", len(rs))
//
//	list := make([][]byte, 0, len(rs))
//	for _, r := range rs {
//		if data, err := msgpack.Marshal(r); err == nil {
//			list = append(list, data)
//		}
//	}
//
//	cached.BatchAdd(list)
//}
