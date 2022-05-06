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
	"github.com/siddontang/go-mysql/schema"
	"go-mysql-transfer/service/endpoint"
	"go-mysql-transfer/util/stringutil"
	"go.uber.org/atomic"
	"runtime"
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"

	"go-mysql-transfer/global"
	"go-mysql-transfer/util/dateutil"
	"go-mysql-transfer/util/logutil"
)

var _threads = runtime.NumCPU()

// StockService 存量数据
type StockService struct {
	canal    *canal.Canal
	endpoint endpoint.Endpoint
	//pageSize int64
	dumpRecordRows int64
	queueCh        chan []*global.RowRequest
	counter        map[string]int64
	lockOfCounter  sync.Mutex
	totalRows      map[string]int64
	table          map[string]endpoint.TableStatus
	wg             sync.WaitGroup
	shutoff        *atomic.Bool
}

func NewStockService(t *TransferService) *StockService {
	return &StockService{
		canal:          t.canal,
		endpoint:       t.endpoint,
		table:          endpoint.GFullProgress.Table,
		dumpRecordRows: int64(t.config.DumpRecordRows),
		//pageSize:  int64(t.config.BulkSize),
		//queueCh:   make(chan []*global.RowRequest),
		counter:   make(map[string]int64),
		totalRows: make(map[string]int64),
		shutoff:   atomic.NewBool(false),
	}
}

func (s *StockService) Run() error {
	var tableMate *schema.Table
	var err error

	startTime := dateutil.NowMillisecond()
	logutil.BothInfof(fmt.Sprintf("批量小大为 size: %d", s.dumpRecordRows))
	size := s.dumpRecordRows
	// 获取当前 position
	if endpoint.InitPos, err = s.canal.GetMasterPos(); err != nil {
		return err
	}
	logutil.BothInfof(fmt.Sprintf("当前 position : %s", endpoint.InitPos.String()))
	for key, status := range s.table {
		// 取表的列
		if tableMate, err = s.canal.GetTable(status.Schema, status.Name); err != nil {
			errors.Trace(err)
			return err
		}
		status.Columns = tableMate.Columns
		s.table[key] = status
		columnsString := s.getColumnsString(tableMate.Columns)
		tableCount := status.Rows
		logutil.BothInfof(fmt.Sprintf("开始导出 %s,共 %d 条数据", key, status.Rows))

		s.counter[key] = 0

		var batch int64
		if tableCount%size == 0 {
			batch = tableCount / size
		} else {
			batch = (tableCount / size) + 1
		}
		// 批次进度
		var processed atomic.Int64
		for i := 0; i < global.Cfg().DumpThreads; i++ {
			s.wg.Add(1)
			go func(key, columnsString string) {
				for {
					processed.Inc()
					err = s.syncBatchRows(key, columnsString, processed.Load())
					if err != nil {
						logutil.Error(err.Error())
						s.shutoff.Store(true)
						break
					}
					if processed.Load() > batch {
						break
					}
				}
				s.wg.Done()
			}(key, columnsString)
		}
		s.wg.Wait()
		logutil.BothInfof(fmt.Sprintf(" %s 导入完成", key))
	}
	fmt.Println(fmt.Sprintf("共耗时 ：%d（毫秒）", dateutil.NowMillisecond()-startTime))
	return nil
}

//func (s *StockService) export(fullName, columns string, batch int64, rule *global.Rule) ([]*global.RowRequest, error) {
//	if s.shutoff.Load() {
//		return nil, errors.New("shutoff")
//	}
//
//	offset := s.offset(batch)
//	sql := fmt.Sprintf("select %s from %s order by %s limit %d,%d", columns, fullName, rule.OrderByColumn, offset, s.pageSize)
//	resultSet, err := s.transfer.canal.Execute(sql)
//	if err != nil {
//		logutil.Errorf("数据导出错误: %s - %s", sql, err.Error())
//		return nil, err
//	}
//	rowNumber := resultSet.RowNumber()
//	requests := make([]*global.RowRequest, 0, rowNumber)
//	for i := 0; i < rowNumber; i++ {
//		rowValues := make([]interface{}, 0, len(rule.TableInfo.Columns))
//		request := new(global.RowRequest)
//		for j := 0; j < len(rule.TableInfo.Columns); j++ {
//			val, err := resultSet.GetValue(i, j)
//			if err != nil {
//				logutil.Errorf("数据导出错误: %s - %s", sql, err.Error())
//				break
//			}
//			rowValues = append(rowValues, val)
//			request.Action = canal.InsertAction
//			request.RuleKey = global.RuleKey(rule.Schema, rule.Table)
//			request.Row = rowValues
//		}
//		requests = append(requests, request)
//	}
//
//	return requests, nil
//}

//func (s *StockService) imports(fullName string, requests []*global.RowRequest, processed int64) {
//	if s.shutoff.Load() {
//		return
//	}
//
//	succeeds := s.transfer.endpoint.Stock(requests)
//	count := s.incCounter(fullName, succeeds)
//	log.Println(fmt.Sprintf("%s 导入数据 %d 条", fullName, count))
//}

func (s *StockService) exportColumns(rule *global.Rule) string {
	if rule.IncludeColumnConfig != "" {
		var columns string
		includes := strings.Split(rule.IncludeColumnConfig, ",")
		for _, c := range rule.TableInfo.Columns {
			for _, e := range includes {
				var column string
				if strings.ToUpper(e) == strings.ToUpper(c.Name) {
					column = c.Name
				} else {
					column = "null as " + c.Name
				}

				if columns != "" {
					columns = columns + ","
				}
				columns = columns + column
			}
		}
		return columns
	}

	if rule.ExcludeColumnConfig != "" {
		var columns string
		excludes := strings.Split(rule.ExcludeColumnConfig, ",")
		for _, c := range rule.TableInfo.Columns {
			for _, e := range excludes {
				var column string
				if strings.ToUpper(e) == strings.ToUpper(c.Name) {
					column = "null as " + c.Name
				} else {
					column = c.Name
				}

				if columns != "" {
					columns = columns + ","
				}
				columns = columns + column
			}
		}
		return columns
	}

	return "*"
}

func (s *StockService) offset(currentPage int64) int64 {
	var offset int64

	if currentPage > 0 {
		offset = (currentPage - 1) * s.dumpRecordRows
	}
	return offset
}

func (s *StockService) Close() {
	s.endpoint.Close()
}

func (s *StockService) incCounter(fullName string, n int64) int64 {
	s.lockOfCounter.Lock()
	defer s.lockOfCounter.Unlock()

	count, ok := s.counter[fullName]
	if ok {
		count = count + n
		s.counter[fullName] = count
	}

	return count
}

func (s *StockService) getColumnsString(tableColumn []schema.TableColumn) string {
	var columns string
	for _, column := range tableColumn {
		if columns != "" {
			columns = columns + ","
		}
		columns = columns + "`" + column.Name + "`"
	}
	return columns
}

func (s *StockService) syncBatchRows(schemaTable string, columns string, batch int64) error {
	if s.shutoff.Load() {
		return errors.New("shutoff")
	}
	offset := s.offset(batch)
	selectSql := fmt.Sprintf("select %s from %s order by id limit %d,%d", columns, schemaTable, offset, s.dumpRecordRows)
	resultSet, err := s.canal.Execute(selectSql)
	if err != nil {
		logutil.Errorf("数据导出错误: %s - %s", schemaTable, err.Error())
		return err
	}
	rowNumber := resultSet.RowNumber()
	if rowNumber == 0 {
		logutil.Infof("未查询到数据: %s : limit %d, %d", schemaTable, offset, s.dumpRecordRows)
		return nil
	}
	// 插入sql的前半部分
	insertTable := fmt.Sprintf(`INSERT IGNORE INTO %s (%s) VALUES`, schemaTable, columns)
	// 插入sql的后半部分
	var insertValues string // (?,?,?),(),()
	// values list
	allValues := make([]interface{}, 0, len(s.table[schemaTable].Columns)*rowNumber)

	for i := 0; i < rowNumber; i++ {
		rowValues := make([]interface{}, 0, len(s.table[schemaTable].Columns))
		var itemValues string
		itemValues = strings.Repeat(" ?,", len(s.table[schemaTable].Columns))
		itemValues = stringutil.CutLastString(itemValues, 1)
		itemValues = fmt.Sprintf("( %s )", itemValues)

		if insertValues != "" {
			insertValues = insertValues + ","
		}
		insertValues = insertValues + itemValues

		for j := 0; j < len(s.table[schemaTable].Columns); j++ {
			val, err := resultSet.GetValue(i, j)
			if err != nil {
				logutil.Errorf("数据导出错误: %s - %s", selectSql, err.Error())
				break
			}
			rowValues = append(rowValues, val)
		}
		allValues = append(allValues, rowValues...)
	}

	insertSql := insertTable + insertValues
	var rowsAffected int64
	rowsAffected, err = s.endpoint.StockExecSql(insertSql, allValues)
	//logutil.BothInfof(fmt.Sprintf("%s 导入数据 %d 条", schemaTable, s.incCounter(schemaTable, rowsAffected)))
	logutil.BothInfof(fmt.Sprintf("%s 导入数据 %d 条", schemaTable, rowsAffected))
	return nil
}
