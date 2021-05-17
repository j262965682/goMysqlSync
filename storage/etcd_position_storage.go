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
	"encoding/json"

	"github.com/siddontang/go-mysql/mysql"

	"go-mysql-transfer/global"
	"go-mysql-transfer/util/etcdutil"
)

type etcdPositionStorage struct {
	Conf *global.Config
}

func (s *etcdPositionStorage) Initialize() error {
	data, err := json.Marshal(mysql.Position{})
	if err != nil {
		return err
	}

	err = etcdutil.CreateIfNecessary(s.Conf.ZePositionDir(), string(data), _etcdOps)
	if err != nil {
		return err
	}

	return nil
}

func (s *etcdPositionStorage) Save(pos mysql.Position) error {
	data, err := json.Marshal(pos)
	if err != nil {
		return err
	}

	return etcdutil.Save(s.Conf.ZePositionDir(), string(data), _etcdOps)
}

func (s *etcdPositionStorage) Get() (mysql.Position, error) {
	var entity mysql.Position

	data, _, err := etcdutil.Get(s.Conf.ZePositionDir(), _etcdOps)
	if err != nil {
		return entity, err
	}

	err = json.Unmarshal(data, &entity)

	return entity, err
}

func (s *etcdPositionStorage) Init() error {
	//TODO
	return nil
}

func (s *etcdPositionStorage) RecordPosition(pos global.PosRequest) error {
	//TODO
	return nil
}

func (s *etcdPositionStorage) AcquirePosition() (pos mysql.Position, err error) {
	//TODO
	return pos, err
}

func (s *etcdPositionStorage) AcquirePositionBySecond(second uint32) (pos mysql.Position, err error) {
	return pos, err
}
