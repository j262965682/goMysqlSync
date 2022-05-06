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
package storage

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/vmihailenco/msgpack"
	"go-mysql-transfer/global"
	"go-mysql-transfer/util/logutil"
	"go.etcd.io/bbolt"
	"strconv"
	"strings"
)

type boltPositionStorage struct {
}

func (s *boltPositionStorage) Initialize() error {
	return _bolt.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_positionBucket)
		data := bt.Get(_fixPositionId)
		if data != nil {
			return nil
		}

		bytes, err := msgpack.Marshal(mysql.Position{})
		if err != nil {
			return err
		}
		return bt.Put(_fixPositionId, bytes)
	})
}

func (s *boltPositionStorage) Save(pos mysql.Position) error {
	fmt.Println(pos.String())
	return _bolt.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_positionBucket)
		data, err := msgpack.Marshal(pos)
		if err != nil {
			return err
		}
		return bt.Put(_fixPositionId, data)
	})
}

func (s *boltPositionStorage) Get() (mysql.Position, error) {
	var entity mysql.Position
	err := _bolt.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_positionBucket)
		data := bt.Get(_fixPositionId)
		if data == nil {
			return errors.NotFoundf("PositionStorage")
		}

		return msgpack.Unmarshal(data, &entity)
	})

	return entity, err
}

func (s *boltPositionStorage) Init() error {
	return _bolt.Update(func(tx *bbolt.Tx) error {
		//_positionBucket   = []byte("Position")
		//_fixPositionId    = byteutil.Uint64ToBytes(uint64(1))
		bt := tx.Bucket(_positionBucket)
		value := bt.Get(_firstKey)
		if value != nil {
			return nil
		}
		bytes, err := msgpack.Marshal(global.PosRequest{})
		if err != nil {
			return err
		}
		return bt.Put(_firstKey, bytes)
	})
}

//记录 position
func (s *boltPositionStorage) RecordPosition(pos global.PosRequest) error {
	var err error
	var value []byte
	var posKey string
	fmt.Println(pos.String())
	return _bolt.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_positionBucket)
		//key := byteutil.Uint32ToBytes(pos.Timestamp)
		if posKey, err = PosToKey(pos); err != nil {
			return err
		}
		value, err = msgpack.Marshal(pos)
		if err != nil {
			return err
		}
		return bt.Put([]byte(posKey), value)
	})
}

func (s *boltPositionStorage) AcquirePosition() (pos mysql.Position, err error) {
	return s.AcquirePositionBySecond(900)
}

// 按当前的binlog时间戳往前推n秒取binlog位置
func (s *boltPositionStorage) AcquirePositionBySecond(second uint32) (pos mysql.Position, err error) {
	var lastKeyByte, lastValueByte, keyByte, valueByte []byte
	//var lastKeyStr string
	//var KeyStr string

	var position, eachPosition global.PosRequest
	var lastGetPos global.PosRequest
	err = _bolt.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(_positionBucket).Cursor()

		//取 last Timestamp
		lastKeyByte, lastValueByte = c.Last()
		//lastKeyStr = string(lastKeyByte)
		msgpack.Unmarshal(lastValueByte, &lastGetPos)
		logutil.Infof("获取当前的 position : %s %d %d ", lastGetPos.Name, lastGetPos.Pos, lastGetPos.Timestamp)

		//判断 last 是否是空，是空则报错
		if IsFirstNilPos(lastKeyByte) {
			return errors.NotFoundf("Wrong!,, Currently acquired position is the first nil position, Run in less than 15 minutes,Not enough position to roll back")
		} else {
			//不为空，则往前一个 Timestamp 取，每个 Timestamp 都需要判断是否为空 ，直到 last 比 Timestamp 大 900 以上
			for {
				keyByte, valueByte = c.Prev()
				if IsFirstNilPos(keyByte) {
					return errors.NotFoundf("Wrong!, Run in less than 15 minutes,Not enough position to roll back")
				}
				//KeyStr = string(keyByte)
				msgpack.Unmarshal(valueByte, &eachPosition)
				if lastGetPos.Timestamp-eachPosition.Timestamp > second {
					break
				}
			}
		}
		return msgpack.Unmarshal(valueByte, &position)
	})
	pos.Name = position.Name
	pos.Pos = position.Pos
	pos.Timestamp = position.Timestamp
	logutil.Infof("当前获取 %d s 前的 position :\n", second, pos.String())
	return pos, err
}

//为什么减少日志文件大小 批量删除日志数据   删除时间戳之前的数据
func (s *boltPositionStorage) DeletePositionBySecond(second uint32) (err error) {
	var value []byte
	var pos *global.PosRequest
	return _bolt.Update(func(tx *bbolt.Tx) error {
		c := tx.Bucket(_positionBucket).Cursor()
		_, value = c.First()
		msgpack.Unmarshal(value, &pos)

		//fmt.Printf("first timestamp is %d\n",pos.Timestamp)

		if pos.Timestamp < second {
			err = c.Delete()
			//fmt.Println("delete first timestamp:", pos.Timestamp)
			if err != nil {
				return err
			}

			for {
				_, value = c.Next()
				msgpack.Unmarshal(value, &pos)
				//fmt.Println("get timestamp:", pos.Timestamp)
				if pos.Timestamp < second {
					err = c.Delete()
					//fmt.Println("delete timestamp:", pos.Timestamp)
					//time.Sleep(time.Second * 1)
					if err != nil {
						return err
					}
				} else {
					break
				}
			}
		}
		return err
	})

}

func IsFirstNilPos(key []byte) bool {
	if string(key) == "00000100000000000001" {
		return true
	}
	return false
}

func ExistsTimestamp(pos global.PosRequest) (bool, error) {
	var value []byte
	var err error
	var str string
	str, err = PosToKey(pos)
	if err != nil {
		return false, err
	}
	err = _bolt.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_positionBucket)
		value = bt.Get([]byte(str))
		return nil
	})
	if value == nil {
		return false, err
	} else {
		return true, err
	}
}

func PosToKey(position global.PosRequest) (string, error) {
	//file 6位补0  pos 14位补0
	var fileLeft, posLeft, str string
	str = "0"
	file := strings.Split(position.Name, ".")[1]
	pos := strconv.Itoa(int(position.Pos))
	if len(file) > 6 || len(pos) > 14 {
		return "", errors.New("position 数据有误 无法解析")
	}
	if len(file) < 6 {
		//fmt.Println(file,"补",6-len(file),"个零")
		fileLeft = strings.Repeat(str, 6-len(file))
		//fmt.Println(fileLeft)
	}
	if len(pos) < 14 {
		//fmt.Println(pos,"补",14-len(pos),"个零")
		posLeft = strings.Repeat(str, 14-len(pos))
		//fmt.Println(posLeft)
	}
	return fileLeft + file + posLeft + pos, nil
}
