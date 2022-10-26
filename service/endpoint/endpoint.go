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
	"encoding/json"
	"go-mysql-sync/service/luaengine"
	"strconv"
	"strings"
	"time"

	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/schema"
	"github.com/vmihailenco/msgpack"

	"go-mysql-sync/global"
	"go-mysql-sync/storage"
	"go-mysql-sync/util/logutil"
	"go-mysql-sync/util/stringutil"
)

type Endpoint interface {
	Start() error
	Ping() error
	Consume(n int, message chan *global.RowRequest, changeChan global.ChangeChan)
	Stock([]*global.RowRequest) int64
	Close()
	DoRetryRow()
	StockExecSql(sql string, valuesList []interface{}) (int64, error)
	FindSQLToMap(sql string) ([]map[string]interface{}, error)
}

func NewEndpoint(c *global.Config, ds *canal.Canal) Endpoint {
	luaengine.InitActuator(ds)
	if c.IsMysql() {
		return newMysqlEndpoint(c)
	}

	//if c.IsRedis() {
	//	return newRedisEndpoint(c)
	//}
	//
	//if c.IsRocketmq() {
	//	return newRocketEndpoint(c)
	//}
	//
	//if c.IsMongodb() {
	//	return newMongoEndpoint(c)
	//}
	//
	//if c.IsRabbitmq() {
	//	return newRabbitEndpoint(c)
	//}
	//
	//if c.IsKafka() {
	//	return newKafkaEndpoint(c)
	//}
	//
	//if c.IsEls() {
	//	if c.ElsVersion == 6 {
	//		return newElastic6Endpoint(c)
	//	}
	//	if c.ElsVersion == 7 {
	//		return newElastic7Endpoint(c)
	//	}
	//}

	return nil
}

func convertColumnData(value interface{}, col *schema.TableColumn, rule *global.Rule) interface{} {
	if value == nil {
		return nil
	}

	switch col.Type {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// we insert invalid enum value before, so return empty
				logutil.Warnf("invalid binlog enum index %d, for enum %v", eNum, col.EnumValues)
				return ""
			}
			return col.EnumValues[eNum]
		case string:
			return value
		case []byte:
			return string(value)
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			bitmask := value
			sets := make([]string, 0, len(col.SetValues))
			for i, s := range col.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return strings.Join(sets, ",")
		}
	case schema.TYPE_BIT:
		switch value := value.(type) {
		case string:
			if value == "\x01" {
				return int64(1)
			}
			return int64(0)
		}
	case schema.TYPE_STRING:
		switch value := value.(type) {
		case []byte:
			return string(value[:])
		}
	case schema.TYPE_JSON:
		var f interface{}
		var err error
		switch v := value.(type) {
		case string:
			err = json.Unmarshal([]byte(v), &f)
		case []byte:
			err = json.Unmarshal(v, &f)
		}
		if err == nil && f != nil {
			return f
		}
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		switch v := value.(type) {
		case string:
			vt, err := time.ParseInLocation(mysql.TimeFormat, string(v), time.Local)
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(rule.DatetimeFormatter)
		case []byte:
			return string(v)
		}
	case schema.TYPE_DATE:
		switch v := value.(type) {
		case string:
			vt, err := time.Parse(rule.DateFormatter, string(v))
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(rule.DateFormatter)
		case []byte:
			return string(v)
		}
	case schema.TYPE_DECIMAL, schema.TYPE_NUMBER:
		switch v := value.(type) {
		case string:
			vv, err := strconv.ParseFloat(v, 64)
			if err != nil {
				logutil.Error(err.Error())
				return nil
			}
			return vv
		case []byte:
			str := string(v)
			vv, err := strconv.ParseFloat(str, 64)
			if err != nil {
				logutil.Error(err.Error())
				return nil
			}
			return vv
		}
	}

	return value
}

func encodeStringValue(rule *global.Rule, kv map[string]interface{}) string {
	var val string
	if rule.ValueFormatter != "" {
		val = rule.ValueFormatter
		for k, v := range kv {
			old := global.LeftBrace + k + global.RightBrace
			new := stringutil.ToString(v)
			val = strings.ReplaceAll(val, old, new)
		}
		return val
	}

	switch rule.ValueEncoder {
	case global.ValEncoderJson:
		data, err := json.Marshal(kv)
		if err != nil {
			logutil.Error(err.Error())
			val = ""
		} else {
			val = string(data)
		}
	case global.ValEncoderKVCommas:
		var ls []string
		for k, v := range kv {
			str := stringutil.ToString(k) + "=" + stringutil.ToString(v)
			ls = append(ls, str)
		}
		val = strings.Join(ls, ",")
	case global.ValEncoderVCommas:
		var ls []string
		for _, v := range kv {
			ls = append(ls, stringutil.ToString(v))
		}
		val = strings.Join(ls, ",")
	}

	return val
}

func keyValueMap(re *global.RowRequest, rule *global.Rule, primitive bool) map[string]interface{} {
	kv := make(map[string]interface{}, len(rule.PaddingMap))
	if primitive {
		for _, padding := range rule.PaddingMap {
			kv[padding.ColumnName] = convertColumnData(re.Row[padding.ColumnIndex], padding.ColumnMetadata, rule)
		}
		return kv
	}

	for _, padding := range rule.PaddingMap {
		kv[padding.WrapName] = convertColumnData(re.Row[padding.ColumnIndex], padding.ColumnMetadata, rule)
	}

	if rule.DefaultColumnValueConfig != "" {
		for k, v := range rule.DefaultColumnValueMap {
			kv[rule.WrapName(k)] = v
		}
	}

	return kv
}

func oldKeyValueMap(request *global.RowRequest, rule *global.Rule, primitive bool) map[string]interface{} {
	kv := make(map[string]interface{}, len(rule.PaddingMap))
	if primitive {
		for _, padding := range rule.PaddingMap {
			kv[padding.ColumnName] = convertColumnData(request.OldRow[padding.ColumnIndex], padding.ColumnMetadata, rule)
		}
		return kv
	}

	for _, padding := range rule.PaddingMap {
		kv[padding.WrapName] = convertColumnData(request.OldRow[padding.ColumnIndex], padding.ColumnMetadata, rule)
	}

	if rule.DefaultColumnValueConfig != "" {
		for k, v := range rule.DefaultColumnValueMap {
			kv[rule.WrapName(k)] = v
		}
	}

	return kv
}

func primaryKey(re *global.RowRequest, rule *global.Rule) interface{} {
	if rule.IsCompositeKey { // 组合ID
		var key string
		for _, index := range rule.TableInfo.PKColumns {
			key += stringutil.ToString(re.Row[index])
		}
		return key
	} else {
		index := rule.TableInfo.PKColumns[0]
		data := re.Row[index]
		column := rule.TableInfo.Columns[index]
		return convertColumnData(data, &column, rule)
	}
}

func pushFailedRows(rs []*global.RowRequest, cached *storage.BoltRowStorage) {
	logutil.Infof("%d 条数据处理失败，插入重试队列", len(rs))

	list := make([][]byte, 0, len(rs))
	for _, r := range rs {
		if data, err := msgpack.Marshal(r); err == nil {
			list = append(list, data)
		}
	}

	cached.BatchAdd(list)
}

func pushFailedRow(r *global.MysqlRespond, cached *storage.BoltRowStorage) {
	logutil.Infof("数据处理失败，插入重试队列")
	if data, err := msgpack.Marshal(r); err == nil {
		cached.Add(data)
	}
}

func exportActionAdd(action, ruleKey string, n int) {
	if global.Cfg().IsExporterEnable() {
		switch action {
		case canal.InsertAction:
			//global.IncInsertAdd(ruleKey, n)
			global.IncInsertCounterAdd(n)
		case canal.DeleteAction:
			//global.IncDeleteAdd(ruleKey, n)
			global.IncDeleteCounterAdd(n)
		}
	}
}

func exportActionNum(action, ruleKey string) {
	if global.Cfg().IsExporterEnable() {
		switch action {
		case canal.InsertAction:
			global.IncInsertCounter()
		case canal.UpdateAction:
			global.IncUpdateCounter()
		case canal.DeleteAction:
			global.IncDeleteCounter()
		case "DDL":
			global.IncDDLCounter()
		}
	}
}

func elsHosts(addr string) []string {
	var hosts []string
	splits := strings.Split(addr, ",")
	for _, split := range splits {
		if !strings.HasPrefix(split, "http:") {
			hosts = append(hosts, "http://"+split)
		} else {
			hosts = append(hosts, split)
		}
	}

	return hosts
}

func buildPropertiesByRule(rule *global.Rule) map[string]interface{} {
	properties := make(map[string]interface{})
	for _, padding := range rule.PaddingMap {
		property := make(map[string]interface{})
		switch padding.ColumnType {
		case schema.TYPE_BINARY:
			property["type"] = "binary"
		case schema.TYPE_NUMBER:
			property["type"] = "long"
		case schema.TYPE_DECIMAL:
			property["type"] = "double"
		case schema.TYPE_FLOAT:
			property["type"] = "float"
		case schema.TYPE_DATE:
			property["type"] = "date"
			property["format"] = "yyyy-MM-dd"
		case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
			property["type"] = "date"
			property["format"] = "yyyy-MM-dd HH:mm:ss"
		default:
			property["type"] = "keyword"
		}
		properties[padding.WrapName] = property
	}

	if len(rule.DefaultColumnValueMap) > 0 {
		for key, _ := range rule.DefaultColumnValueMap {
			property := make(map[string]interface{})
			property["type"] = "keyword"
			properties[key] = property
		}
	}

	for _, mapping := range rule.EsMappings {
		property := make(map[string]interface{})
		property["type"] = mapping.Type
		if mapping.Format != "" {
			property["format"] = mapping.Format
		}
		if mapping.Analyzer != "" {
			property["analyzer"] = mapping.Analyzer
		}
		properties[mapping.Field] = property
	}

	return properties
}

func buildPropertiesByMappings(rule *global.Rule) map[string]interface{} {
	properties := make(map[string]interface{})
	for _, mapping := range rule.EsMappings {
		property := make(map[string]interface{})
		property["type"] = mapping.Type
		if mapping.Format != "" {
			property["format"] = mapping.Format
		}
		if mapping.Analyzer != "" {
			property["analyzer"] = mapping.Analyzer
		}
		properties[mapping.Field] = property
	}
	return properties
}
