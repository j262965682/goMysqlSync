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
package global

import (
	"fmt"
	"github.com/siddontang/go-mysql/schema"
	"sync"
)

//type DDLRequest struct {
//	RuleKey string
//	Action  string
//	Query   string
//	Pos PosRequest
//}

var GlobalChangeChan ChangeChan
var GlobalInstance string

//DDL线程控制器
type ChangeChan struct {
	DdlControl     bool
	Mutex          sync.Mutex
	DdlControlChan chan struct{}
}

type Tables struct {
	TableName string
}

type TableDesc struct {
	Table       string
	CreateTable string `gorm:"column:Create Table"`
}

type DatabaseDesc struct {
	Database       string
	CreateDatabase string `gorm:"column:Create Database"`
}

type ConsumeQueue struct {
	Number int
	Queue  chan []*RowRequest
}

type RowRequest struct {
	IdNum       interface{}
	IdString    string
	RuleKey     string
	Action      string
	OldRow      []interface{}
	Row         []interface{}
	Query       string
	Table       *schema.Table
	Hash        int
	SchemaStart string
	SchemaEnd   string
	Schema      string
	Timestamp   uint32
}

type PosRequest struct {
	Name      string
	Pos       uint32
	Force     bool
	Timestamp uint32
}

func (p PosRequest) String() string {
	return fmt.Sprintf("(%d, %s, %d)", p.Timestamp, p.Name, p.Pos)
}

type MysqlRespond struct {
	RuleKey         string
	Schema          string
	Table           string
	Action          string
	Sql             string
	Id              interface{}
	Field           string
	Val             []interface{}
	ManyVal         []interface{}
	ManyId          []interface{}
	KeyVal          map[string]interface{}
	SchemaStart     string
	SchemaEnd       string
	LastTableName   string
	LastTableSchema string
	NumOfSql        int
	Timestamp       uint32
}

type RedisRespond struct {
	Action    string
	Structure string
	Key       string
	Field     string
	Score     float64
	OldVal    interface{}
	Val       interface{}
}

type MQRespond struct {
	Topic     string      `json:"-"`
	Action    string      `json:"action"`
	Date      interface{} `json:"date"`
	ByteArray []byte      `json:"-"`
}

type ESRespond struct {
	Index  string
	Id     string
	Action string
	Date   string
}

type MongoRespond struct {
	RuleKey    string
	Collection string
	Action     string
	Id         interface{}
	Table      map[string]interface{}
}

type Padding struct {
	WrapName string

	ColumnName     string
	ColumnIndex    int
	ColumnType     int
	ColumnMetadata *schema.TableColumn
}

var MysqlRespondPool = sync.Pool{
	New: func() interface{} {
		return new(MysqlRespond)
	},
}

var RedisRespondPool = sync.Pool{
	New: func() interface{} {
		return new(RedisRespond)
	},
}

var MQRespondPool = sync.Pool{
	New: func() interface{} {
		return new(MQRespond)
	},
}

var RowRequestPool = sync.Pool{
	New: func() interface{} {
		return new(RowRequest)
	},
}

type HashMap struct {
	Array   []chan *RowRequest //chan list
	ChanLen int                //线程数量
	Lock    sync.Mutex         //线程锁
}

func NewHashMap(len int, bulkSize int) (hashMap *HashMap) {
	list := make([]chan *RowRequest, len)
	for i := 0; i < len; i++ {
		list[i] = make(chan *RowRequest, bulkSize)
	}
	hashMap = &HashMap{
		Array:   list,
		ChanLen: len,
		Lock:    sync.Mutex{},
	}
	return
}
