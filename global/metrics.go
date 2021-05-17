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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"
	nethttp "net/http"
	"strconv"
	"time"
)

const (
	MetricsStateOK = 1
	MetricsStateNO = 0
)

var (
	ExecuteSQLTimestamp uint32
	CurrentMachineState MachineState
)
var (
	leaderState = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "transfer_leader_state",
			Help: "The cluster leader state: 0=false, 1=true",
		}, []string{"instance"},
	)

	//目的实例连接状态
	//destinationState = promauto.NewGauge(
	//	prometheus.GaugeOpts{
	//		Name: "transfer_destination_state",
	//		Help: "The destination running state: 0=stopped, 1=ok",
	//	},
	//)

	transferDelay = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "transfer_delay",
			Help: "The transfer slave lag",
		}, []string{"instance"},
	)

	//源实例连接状态
	//sourceState = promauto.NewGauge(
	//	prometheus.GaugeOpts{
	//		Name: "source_state",
	//		Help: "The source state: 0=false, 1=true",
	//	},
	//)

	//目的实例连接状态
	destinationState = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "destination_state",
			Help: "The destination state: 0=stopped, 1=ok",
		}, []string{"instance"},
	)
	////当前执行sql的时间戳，也就是数据延迟
	//executeSQLTimestamp = promauto.NewGauge(
	//	prometheus.GaugeOpts{
	//		Name: "executeSQL_timestamp",
	//		Help: "The timestamp of the current SQL",
	//	},
	//)

	//当前时间戳-执行sql的时间戳，也就是数据延迟
	delay = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "delay",
			Help: "delay of sync",
		}, []string{"instance"},
	)

	//插入行数
	insertCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "insert_counter",
			Help: "The number of data inserted to destination",
		}, []string{"instance"},
	)
	//更新行数
	updateCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "update_counter",
			Help: "The number of data updated to destination",
		}, []string{"instance"},
	)
	//删除行数
	deleteCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "delete_counter",
			Help: "The number of data deleted from destination",
		}, []string{"instance"},
	)
	//DDl SQL的数量
	ddlCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ddl_counter",
			Help: "The number of data DDL from destination",
		}, []string{"instance"},
	)

	//insertNum = promauto.NewCounterVec(
	//	prometheus.CounterOpts{
	//		Name: "transfer_inserted_num",
	//		Help: "The number of data inserted to destination",
	//	}, []string{"table"},
	//)
	//
	//updateNum = promauto.NewCounterVec(
	//	prometheus.CounterOpts{
	//		Name: "transfer_updated_num",
	//		Help: "The number of data updated to destination",
	//	}, []string{"table"},
	//)
	//
	//deleteNum = promauto.NewCounterVec(
	//	prometheus.CounterOpts{
	//		Name: "transfer_deleted_num",
	//		Help: "The number of data deleted from destination",
	//	}, []string{"table"},
	//)
	//
	//ddlNum = promauto.NewCounterVec(
	//	prometheus.CounterOpts{
	//		Name: "transfer_DDL_num",
	//		Help: "The number of data DDL from destination",
	//	}, []string{"table"},
	//)

	diskTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "diskTotal",
			Help: "diskTotal(G)",
		}, []string{"instance"},
	)

	diskFree = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "diskFree",
			Help: "diskFree(G)",
		}, []string{"instance"},
	)

	diskUsed = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "diskUsed",
			Help: "diskUsed(G)",
		}, []string{"instance"},
	)

	diskUsage = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "diskUsage",
			Help: "diskUsage",
		}, []string{"instance"},
	)

	memTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "memTotal",
			Help: "memTotal(M)",
		}, []string{"instance"},
	)

	memFree = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "memFree",
			Help: "memFree(M)",
		}, []string{"instance"},
	)

	memUsed = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "memUsed",
			Help: "memUsed(M)",
		}, []string{"instance"},
	)
	memUsage = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "menUsage",
			Help: "内存使用率",
		}, []string{"instance"},
	)

	cpuUsage = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cpuUsage",
			Help: "cpuUsage",
		}, []string{"instance"},
	)

	valueBytesRecv = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "BytesRecv",
			Help: "接收流量BytesRecv(bit)",
		}, []string{"instance"},
	)

	valueBytesSent = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "BytesSent",
			Help: "发送流量BytesSent(bit)",
		}, []string{"instance"},
	)
)

//func SetSourceState(state int) {
//	if _config.EnableExporter {
//		sourceState.Set(float64(state))
//	}
//}

func SetDestinationState(state int) {
	destinationState.WithLabelValues(GlobalInstance).Set(float64(state))
}

func SetDelay(ExecuteSQLTimestamp uint32, current uint32) {
	delay.WithLabelValues(GlobalInstance).Set(float64(current) - float64(ExecuteSQLTimestamp))
}

func IncInsertCounter() {
	insertCounter.WithLabelValues(GlobalInstance).Inc()
}

func IncInsertCounterAdd(n int) {
	insertCounter.WithLabelValues(GlobalInstance).Add(float64(n))
}

func IncUpdateCounter() {
	updateCounter.WithLabelValues(GlobalInstance).Inc()
}

func IncUpdateCounterAdd(n int) {
	updateCounter.WithLabelValues(GlobalInstance).Add(float64(n))
}

func IncDeleteCounter() {
	deleteCounter.WithLabelValues(GlobalInstance).Inc()
}

func IncDeleteCounterAdd(n int) {
	deleteCounter.WithLabelValues(GlobalInstance).Add(float64(n))
}

func IncDDLCounter() {
	ddlCounter.WithLabelValues(GlobalInstance).Inc()
}

func SetTransferDelay(delay uint32) {
	transferDelay.WithLabelValues(GlobalInstance).Set(float64(delay))
}

func SetLeaderState(state int) {
	if _config.EnableExporter {
		leaderState.WithLabelValues(GlobalInstance).Set(float64(state))
	}
}

//-----------------------------------------------------
func StartMonitor() {
	if _config.EnableExporter {
		go func() {
			nethttp.Handle("/metrics", promhttp.Handler())
			nethttp.ListenAndServe(fmt.Sprintf(":%d", _config.ExporterPort), nil)
		}()
	}
}

//func TickerExecuteSQLDelay(ExecuteSQLTimestamp uint32) {
//	ticker := time.NewTicker(time.Millisecond * 1000)
//	defer ticker.Stop()
//	for  {
//		select {
//		case <-ticker.C:
//			SetExecuteSQLTimestamp(ExecuteSQLTimestamp)
//			logutil.Info("sql的时间戳为:" + string(ExecuteSQLTimestamp))
//		}
//	}
//}

type MachineState struct {
	diskTotal      float64
	diskFree       float64
	diskUsed       float64
	diskUsage      float64
	memTotal       float64
	memFree        float64
	memUsed        float64
	memUsage       float64
	cpuUsage       float64
	valueBytesRecv float64
	valueBytesSent float64
}

func GetMachineState() {
	d, _ := disk.Usage("/")
	v, _ := mem.VirtualMemory()
	cc, _ := cpu.Percent(time.Second, false)
	nv, _ := net.IOCounters(true)

	diskTotal := float64(d.Total / 1024 / 1024 / 1024)
	//fmt.Println("总磁盘:",diskTotal)
	diskFree := float64(d.Free / 1024 / 1024 / 1024)
	//fmt.Println("磁盘空闲:",diskFree)
	diskUsed := float64(d.Used / 1024 / 1024 / 1024)
	//fmt.Println("磁盘空闲:",diskFree)
	diskUsageNo := float64(d.UsedPercent)
	diskUsage, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", diskUsageNo), 64)
	//fmt.Println("磁盘使用率:",diskUsage)
	memTotal := float64(v.Total / 1024 / 1024)
	//fmt.Println("总内存:",memTotal)
	memFree := float64(v.Available / 1024 / 1024)
	//fmt.Println("内存空闲:",memFree)
	memUsed := float64(v.Used / 1024 / 1024)
	//fmt.Println("内存使用:",memUsed)
	memUsageNo := float64(v.UsedPercent)
	memUsage, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", memUsageNo), 64)
	//fmt.Println("内存使用率:",memUsage)
	cpuUsageNo := float64(cc[0])
	cpuUsage, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", cpuUsageNo), 64)
	//fmt.Println("CPU使用率:",cpuUsage)
	bytesRecv := float64(nv[0].BytesRecv) * 8
	valueBytesRecv, _ := strconv.ParseFloat(fmt.Sprintf("%.3f", bytesRecv), 64)
	//fmt.Println("接收流量:",valueBytesRecv,"M")
	bytesSent := float64(nv[0].BytesSent) * 8
	valueBytesSent, _ := strconv.ParseFloat(fmt.Sprintf("%.3f", bytesSent), 64)
	//fmt.Println("发送流量:",valueBytesSent,"M")
	CurrentMachineState = MachineState{diskTotal, diskFree, diskUsed, diskUsage, memTotal, memFree, memUsed, memUsage, cpuUsage, valueBytesRecv, valueBytesSent}
	SetMachineState(CurrentMachineState)
}

func SetMachineState(CurrentMachineState MachineState) {
	diskTotal.WithLabelValues(GlobalInstance).Set(CurrentMachineState.diskTotal)
	diskFree.WithLabelValues(GlobalInstance).Set(CurrentMachineState.diskFree)
	diskUsed.WithLabelValues(GlobalInstance).Set(CurrentMachineState.diskUsed)
	diskUsage.WithLabelValues(GlobalInstance).Set(CurrentMachineState.diskUsage)
	memTotal.WithLabelValues(GlobalInstance).Set(CurrentMachineState.memTotal)
	memUsed.WithLabelValues(GlobalInstance).Set(CurrentMachineState.memUsed)
	memUsage.WithLabelValues(GlobalInstance).Set(CurrentMachineState.memUsage)
	cpuUsage.WithLabelValues(GlobalInstance).Set(CurrentMachineState.cpuUsage)
	valueBytesRecv.WithLabelValues(GlobalInstance).Set(CurrentMachineState.valueBytesRecv)
	valueBytesSent.WithLabelValues(GlobalInstance).Set(CurrentMachineState.valueBytesSent)
}
