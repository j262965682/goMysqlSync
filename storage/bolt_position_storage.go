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
	"go-mysql-transfer/util/byteutil"
	"go-mysql-transfer/util/logutil"
	"go.etcd.io/bbolt"
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

func (s *boltPositionStorage) RecordPosition(pos global.PosRequest) error {
	fmt.Println(pos.String())
	return _bolt.Update(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_positionBucket)
		key := byteutil.Uint32ToBytes(pos.Timestamp)
		value, err := msgpack.Marshal(pos)
		if err != nil {
			return err
		}
		return bt.Put(key, value)
	})
}

func (s *boltPositionStorage) AcquirePosition() (pos mysql.Position, err error) {
	return s.AcquirePositionBySecond(900)
}

func (s *boltPositionStorage) AcquirePositionBySecond(second uint32) (pos mysql.Position, err error) {
	var lastKeyByte, lastValueByte, keyByte, valueByte []byte
	var lastKeyuInt32, KeyuInt32 uint32
	var position global.PosRequest
	var firstGetPos global.PosRequest
	err = _bolt.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(_positionBucket).Cursor()

		//取 last Timestamp
		lastKeyByte, lastValueByte = c.Last()
		lastKeyuInt32 = byteutil.BytesToUint32(lastKeyByte)
		msgpack.Unmarshal(lastValueByte, &firstGetPos)
		logutil.Infof("获取当前的 position : %s %d %d ", firstGetPos.Name, firstGetPos.Pos, firstGetPos.Timestamp)

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
				KeyuInt32 = byteutil.BytesToUint32(keyByte)
				if lastKeyuInt32-KeyuInt32 > second {
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

func IsFirstNilPos(key []byte) bool {
	if byteutil.BytesToUint32(key) == 1 {
		return true
	}
	return false
}

func ExistsTimestamp(timestamp uint32) (bool, error) {
	var value []byte
	err := _bolt.View(func(tx *bbolt.Tx) error {
		bt := tx.Bucket(_positionBucket)
		value = bt.Get(byteutil.Uint32ToBytes(timestamp))
		return nil
	})
	if value == nil {
		return false, err
	} else {
		return true, err
	}
}
