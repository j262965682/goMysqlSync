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
package endpoint

import (
	"fmt"
	"github.com/go-redis/redis"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/vmihailenco/msgpack"
	"strings"

	"go-mysql-transfer/global"
	"go-mysql-transfer/service/luaengine"
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util/logutil"
	"go-mysql-transfer/util/stringutil"
)

type RedisEndpoint struct {
	config *global.Config
	cached *storage.BoltRowStorage

	isCluster bool
	client    *redis.Client
	cluster   *redis.ClusterClient
}

func newRedisEndpoint(c *global.Config) *RedisEndpoint {
	r := &RedisEndpoint{}
	r.config = c
	r.cached = &storage.BoltRowStorage{}

	list := strings.Split(c.RedisAddr, ",")
	if len(list) == 1 {
		r.client = redis.NewClient(&redis.Options{
			Addr:     c.RedisAddr,
			Password: c.RedisPass,
			DB:       c.RedisDatabase,
		})
	} else {
		if c.RedisGroupType == global.RedisGroupTypeSentinel {
			r.client = redis.NewFailoverClient(&redis.FailoverOptions{
				MasterName:    c.RedisMasterName,
				SentinelAddrs: list,
				Password:      c.RedisPass,
				DB:            c.RedisDatabase,
			})
		}
		if c.RedisGroupType == global.RedisGroupTypeCluster {
			r.isCluster = true
			r.cluster = redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:    list,
				Password: c.RedisPass,
			})
		}
	}

	return r
}

func (s *RedisEndpoint) Start() error {
	return s.Ping()
}

func (s *RedisEndpoint) Ping() error {
	var err error
	if s.isCluster {
		_, err = s.cluster.Ping().Result()
	} else {
		_, err = s.client.Ping().Result()
	}
	return err
}

func (s *RedisEndpoint) pipe() redis.Pipeliner {
	var pipe redis.Pipeliner
	if s.isCluster {
		pipe = s.cluster.Pipeline()
	} else {
		pipe = s.client.Pipeline()
	}

	return pipe
}

func (s *RedisEndpoint) Consume(n int, message chan []*global.RowRequest, changeChan global.ChangeChan) {
	for rows := range message {
		if err := s.doRetryTask(); err != nil {
			logutil.Error(err.Error())
			pushFailedRows(rows, s.cached)
			return
		}

		expect := true
		pipe := s.pipe()
		for _, row := range rows {
			rule, _ := global.RuleIns(row.RuleKey)
			if rule.TableColumnSize != len(row.Row) {
				logutil.Warnf("%s schema mismatching", row.RuleKey)
				continue
			}

			exportActionNum(row.Action, row.RuleKey)

			if rule.LuaNecessary() {
				var err error
				var ls []*global.RedisRespond
				kvm := keyValueMap(row, rule, true)
				if row.Action == canal.UpdateAction {
					previous := oldKeyValueMap(row, rule, true)
					ls, err = luaengine.DoRedisOps(kvm, previous, row.Action, rule)
				} else {
					ls, err = luaengine.DoRedisOps(kvm, nil, row.Action, rule)
				}

				if err != nil {
					logutil.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
					expect = false
					break
				}
				for _, resp := range ls {
					s.preparePipe(resp, pipe)

					logutil.Infof("action: %s, structure: %s ,key: %s ,field: %s, value: %v", resp.Action, resp.Structure, resp.Key, resp.Field, resp.Val)

					global.RedisRespondPool.Put(resp)
				}
			} else {
				resp := s.ruleRespond(row, rule)
				s.preparePipe(resp, pipe)

				logutil.Infof("action: %s, structure: %s ,key: %s ,field: %s, value: %v", resp.Action, resp.Structure, resp.Key, resp.Field, resp.Val)

				global.RedisRespondPool.Put(resp)
			}

			global.RowRequestPool.Put(row)
		}

		_, err := pipe.Exec()
		if err != nil {
			logutil.Error(errors.ErrorStack(err))
			expect = false
		}

		if !expect {
			pushFailedRows(rows, s.cached)
		} else {
			logutil.Infof("处理完成 %d 条数据", len(rows))
		}
	}
}

func (s *RedisEndpoint) Stock(rows []*global.RowRequest) int64 {
	pipe := s.pipe()
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logutil.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		if rule.LuaNecessary() {
			kvm := keyValueMap(row, rule, true)
			ls, err := luaengine.DoRedisOps(kvm, nil, row.Action, rule)
			if err != nil {
				logutil.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
				break
			}
			for _, resp := range ls {
				s.preparePipe(resp, pipe)
				global.RedisRespondPool.Put(resp)
			}
		} else {
			resp := s.ruleRespond(row, rule)
			resp.Action = row.Action
			resp.Structure = rule.RedisStructure
			s.preparePipe(resp, pipe)
			global.RedisRespondPool.Put(resp)
		}
	}

	var counter int64
	res, err := pipe.Exec()
	if err != nil {
		logutil.Error(err.Error())
	}

	for _, re := range res {
		if re.Err() == nil {
			counter++
		}
	}

	return counter
}

func (s *RedisEndpoint) ruleRespond(row *global.RowRequest, rule *global.Rule) *global.RedisRespond {
	resp := global.RedisRespondPool.Get().(*global.RedisRespond)
	resp.Action = row.Action
	resp.Structure = rule.RedisStructure

	kvm := keyValueMap(row, rule, false)
	resp.Key = s.encodeKey(row, rule)
	if resp.Structure == global.RedisStructureHash {
		resp.Field = s.encodeHashField(row, rule)
	}
	if resp.Structure == global.RedisStructureSortedSet {
		resp.Score = s.encodeSortedSetScoreField(row, rule)
	}

	if resp.Action == canal.InsertAction {
		resp.Val = encodeStringValue(rule, kvm)
	} else if resp.Action == canal.UpdateAction {
		if rule.RedisStructure == global.RedisStructureList ||
			rule.RedisStructure == global.RedisStructureSet ||
			rule.RedisStructure == global.RedisStructureSortedSet {

			oldKvm := oldKeyValueMap(row, rule, false)
			resp.OldVal = encodeStringValue(rule, oldKvm)
		}
		resp.Val = encodeStringValue(rule, kvm)
	} else {
		if rule.RedisStructure == global.RedisStructureList ||
			rule.RedisStructure == global.RedisStructureSet ||
			rule.RedisStructure == global.RedisStructureSortedSet {

			resp.Val = encodeStringValue(rule, kvm)
		}
	}

	return resp
}

func (s *RedisEndpoint) preparePipe(resp *global.RedisRespond, pipe redis.Cmdable) {
	switch resp.Structure {
	case global.RedisStructureString:
		if resp.Action == canal.DeleteAction {
			pipe.Del(resp.Key)
		} else {
			pipe.Set(resp.Key, resp.Val, 0)
		}
	case global.RedisStructureHash:
		if resp.Action == canal.DeleteAction {
			pipe.HDel(resp.Key, resp.Field)
		} else {
			pipe.HSet(resp.Key, resp.Field, resp.Val)
		}
	case global.RedisStructureList:
		if resp.Action == canal.DeleteAction {
			pipe.LRem(resp.Key, 0, resp.Val)
		} else if resp.Action == canal.UpdateAction {
			pipe.LRem(resp.Key, 0, resp.OldVal)
			pipe.RPush(resp.Key, resp.Val)
		} else {
			pipe.RPush(resp.Key, resp.Val)
		}
	case global.RedisStructureSet:
		if resp.Action == canal.DeleteAction {
			pipe.SRem(resp.Key, resp.Val)
		} else if resp.Action == canal.UpdateAction {
			pipe.SRem(resp.Key, 0, resp.OldVal)
			pipe.SAdd(resp.Key, resp.Val)
		} else {
			pipe.SAdd(resp.Key, resp.Val)
		}
	case global.RedisStructureSortedSet:
		if resp.Action == canal.DeleteAction {
			pipe.ZRem(resp.Key, resp.Val)
		} else if resp.Action == canal.UpdateAction {
			pipe.ZRem(resp.Key, 0, resp.OldVal)
			val := redis.Z{Score: resp.Score, Member: resp.Val}
			pipe.ZAdd(resp.Key, val)
		} else {
			val := redis.Z{Score: resp.Score, Member: resp.Val}
			pipe.ZAdd(resp.Key, val)
		}
	}
}

func (s *RedisEndpoint) doCmd(resp *global.RedisRespond) error {
	var cmd redis.Cmdable
	if s.isCluster {
		cmd = s.cluster
	} else {
		cmd = s.client
	}

	switch resp.Structure {
	case global.RedisStructureString:
		if resp.Action == canal.DeleteAction {
			r := cmd.Del(resp.Key)
			if r.Err() != nil {
				return r.Err()
			}
		} else {
			r := cmd.Set(resp.Key, resp.Val, 0)
			if r.Err() != nil {
				return r.Err()
			}
		}
	case global.RedisStructureHash:
		if resp.Action == canal.DeleteAction {
			r := cmd.HDel(resp.Key, resp.Field)
			if r.Err() != nil {
				return r.Err()
			}
		} else {
			r := cmd.HSet(resp.Key, resp.Field, resp.Val)
			if r.Err() != nil {
				return r.Err()
			}
		}
	case global.RedisStructureList:
		if resp.Action == canal.DeleteAction {
			r := cmd.LRem(resp.Key, 0, resp.Val)
			if r.Err() != nil {
				return r.Err()
			}
		} else {
			r := cmd.RPush(resp.Key, resp.Val)
			if r.Err() != nil {
				return r.Err()
			}
		}
	case global.RedisStructureSet:
		if resp.Action == canal.DeleteAction {
			r := cmd.SRem(resp.Key, resp.Val)
			if r.Err() != nil {
				return r.Err()
			}
		} else {
			r := cmd.SAdd(resp.Key, resp.Val)
			if r.Err() != nil {
				return r.Err()
			}
		}
	case global.RedisStructureSortedSet:
		if resp.Action == canal.DeleteAction {
			r := cmd.ZRem(resp.Key, resp.Val)
			if r.Err() != nil {
				return r.Err()
			}
		} else {
			val := redis.Z{Score: resp.Score, Member: resp.Val}
			r := cmd.ZAdd(resp.Key, val)
			if r.Err() != nil {
				return r.Err()
			}
		}
	}

	return nil
}

func (s *RedisEndpoint) DoRetryRow() {
	//TODO
}

func (s *RedisEndpoint) doRetryTask() error {
	if s.cached.Size() == 0 {
		return nil
	}

	if err := s.Ping(); err != nil {
		return err
	}

	logutil.Infof("当前重试队列有%d 条数据", s.cached.Size())

	var data []byte
	ids := s.cached.IdList()
	for _, id := range ids {
		var err error
		data, err = s.cached.Get(id)
		if err != nil {
			logutil.Warn(err.Error())
			s.cached.Delete(id)
			continue
		}

		var row global.RowRequest
		err = msgpack.Unmarshal(data, &row)
		if err != nil {
			logutil.Warn(err.Error())
			s.cached.Delete(id)
			continue
		}

		rule, _ := global.RuleIns(row.RuleKey)
		if rule.LuaNecessary() {
			var err error
			var ls []*global.RedisRespond
			kvm := keyValueMap(&row, rule, true)
			if row.Action == canal.UpdateAction {
				previous := oldKeyValueMap(&row, rule, true)
				ls, err = luaengine.DoRedisOps(kvm, previous, row.Action, rule)
			} else {
				ls, err = luaengine.DoRedisOps(kvm, nil, row.Action, rule)
			}

			if err != nil {
				return errors.New(fmt.Sprintf("lua 脚本执行失败 : %s ", errors.ErrorStack(err)))
			}
			for _, resp := range ls {
				err = s.doCmd(resp)

				logutil.Infof("retry: action: %s, structure: %s ,key: %s ,field: %s, value: %v", resp.Action, resp.Structure, resp.Key, resp.Field, resp.Val)

				global.RedisRespondPool.Put(resp)
				if err != nil {
					return err
				}
			}
		} else {
			resp := s.ruleRespond(&row, rule)
			err = s.doCmd(resp)

			logutil.Infof("retry: action: %s, structure: %s ,key: %s ,field: %s, value: %v", resp.Action, resp.Structure, resp.Key, resp.Field, resp.Val)

			global.RedisRespondPool.Put(resp)
			if err != nil {
				return err
			}
		}

		logutil.Infof("cached id :%d , 数据重试成功", id)
		s.cached.Delete(id)
	}

	return nil
}

func (s *RedisEndpoint) encodeKey(re *global.RowRequest, rule *global.Rule) string {
	if rule.RedisKeyValue != "" {
		return rule.RedisKeyValue
	}

	var key string

	if rule.RedisKeyFormatter != "" {
		for column, index := range rule.RedisKeyColumnIndexMap {
			val := stringutil.ToString(re.Row[index])
			temp := rule.RedisKeyFormatter
			temp = strings.ReplaceAll(temp, global.LeftBrace+column+global.RightBrace, val)
			key = temp
		}
		return key
	}

	if rule.RedisKeyColumnIndex < 0 {
		for _, v := range rule.RedisKeyColumnIndexs {
			key += stringutil.ToString(re.Row[v])
		}
	} else {
		key = stringutil.ToString(re.Row[rule.RedisKeyColumnIndex])
	}
	if rule.RedisKeyPrefix != "" {
		key = rule.RedisKeyPrefix + key
	}

	return key
}

func (s *RedisEndpoint) encodeHashField(re *global.RowRequest, rule *global.Rule) string {
	var field string

	if rule.RedisHashFieldColumnIndex < 0 {
		for _, v := range rule.RedisHashFieldColumnIndexs {
			field += stringutil.ToString(re.Row[v])
		}
	} else {
		field = stringutil.ToString(re.Row[rule.RedisHashFieldColumnIndex])
	}

	if rule.RedisHashFieldPrefix != "" {
		field = rule.RedisHashFieldPrefix + field
	}

	return field
}

func (s *RedisEndpoint) encodeSortedSetScoreField(re *global.RowRequest, rule *global.Rule) float64 {
	obj := re.Row[rule.RedisHashFieldColumnIndex]
	if obj == nil {
		return 0
	}

	str := stringutil.ToString(obj)
	return stringutil.ToFloat64Safe(str)
}

func (s *RedisEndpoint) Close() {
	if s.client != nil {
		s.client.Close()
	}
}

func (s *RedisEndpoint) StockExecSql(sql string, valuesList []interface{}) (int64, error) {
	// TODO
	return 0, nil
}
