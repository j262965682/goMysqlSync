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
package main

import (
	"flag"
	"fmt"
	"go-mysql-sync/util"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/juju/errors"
	"go-mysql-sync/global"
	"go-mysql-sync/service"
	"go-mysql-sync/storage"
	"go-mysql-sync/util/logutil"
	"go-mysql-sync/util/stringutil"
	_ "net/http/pprof"
)

var (
	helpFlag      bool
	cfgPath       string
	positionFlag  bool
	startPosition bool
	statusFlag    bool

	deletePosFlag bool

	full       bool
	incrFlag   bool
	schemaFlag bool
	stockFlag  bool
	syncMode   int
)

func init() {
	flag.BoolVar(&helpFlag, "help", false, "this help")
	flag.StringVar(&cfgPath, "config", "app.yml", "application config file")

	//// go run main.go -full
	//
	//// 只同步一次全量数据
	//flag.BoolVar(&stockFlag, "only-stock", false, "One-off stock,no increment sync")
	//
	//// 只增量数据
	//flag.BoolVar(&incrFlag, "only-increment", false, "only increment sync,no stock")
	//
	//// 全量+增量同步---默认值
	//flag.BoolVar(&full, "stock-increment", false, "stock and increment sync")

	//// 只同步表结构
	//flag.BoolVar(&schemaFlag, "only-schema", false, "only sync schema")

	//设置当前binlog位置并且开始运行    go run main.go -start-position mysql-bin.000469 324783 1619431429
	flag.BoolVar(&startPosition, "start-position", false, "start increment sync with that position")

	//查询当前binlog位置              go run main.go -status 900
	flag.BoolVar(&statusFlag, "current-position", false, "Query the position of many seconds age,This position is the first one greater than yours seconds.")

	//position data 日志过大，清理data    go run main.go -delete pos
	flag.BoolVar(&deletePosFlag, "delete-pos", false, "sync table struct or not")

	// sync-mode 同步模式    go run .\main.go --sync-mode=6
	flag.IntVar(&syncMode, "sync-mode", 0, `数值型 :一般为 6
同步模式  表结构   全量数据   增量数据
   1       是        否       否      只同步表结构
   2       否        是       否      只同步全量数据
   3       否        否       是      只同步增量数据
   4       是        是       否      同步表结构和全量数据
   5       否        是       是      同步全量数据和增量数据，忽略表结构
   6       是        是       是      先同步表结构，再同步全量数据和增量数据`)

	flag.Usage = usage
}

func main() {
	//第一次需要同步表结构
	//如果掉线了 重启则 直接跑 go run main.go，会沿着记录的binlog点继续跑
	flag.Parse()
	if helpFlag {
		flag.Usage()
		return
	}
	// sync-mode 同步模式
	//        表结构   全量数据   增量数据
	// 1        是        否       否      只同步表结构
	// 2        否        是       否      只同步全量数据
	// 3        否        否       是      只同步增量数据
	// 4        是        是       否      同步表结构和全量数据
	// 5        否        是       是      同步全量数据和增量数据，忽略表结构
	// 6        是        是       是      先同步表结构，再同步全量数据和增量数据

	others := make([]string, 2)
	if syncMode != 0 {
		switch syncMode {
		case 1:
			util.SetSchemaFlag(true)
		case 2:
			util.SetStockFlag(true)
		case 3:
			util.SetIncrFlag(true)
		case 4:
			util.SetSchemaFlag(true)
			util.SetStockFlag(true)
		case 5:
			util.SetStockFlag(true)
			util.SetIncrFlag(true)
		case 6:
			util.SetSchemaFlag(true)
			util.SetStockFlag(true)
			util.SetIncrFlag(true)
			fmt.Println("全量加增量")
		default:
			fmt.Println("Unidentifiable number,only 1 to 6.")
			return
		}
	}

	//-----------------------------------
	go http.ListenAndServe(":9999", nil)
	//-----------------------------------
	n := runtime.GOMAXPROCS(runtime.NumCPU())

	err := service.InitApplication(cfgPath)
	if err != nil {
		println(
			errors.ErrorStack(err))
		return
	}

	logutil.Infof("GOMAXPROCS :%d", n)

	if statusFlag {
		var err error
		var secondInt int
		others = flag.Args()
		second := others[0]
		if secondInt, err = strconv.Atoi(second); err != nil {
			println("error: The parameter second must be number")
			return
		}
		ps := storage.NewPositionStorage(global.Cfg())
		pos, _ := ps.AcquirePositionBySecond(uint32(secondInt))
		fmt.Printf("The current dump position is : %s, %d, %d \n", pos.Name, pos.Pos, pos.Timestamp)
		return
	}

	if positionFlag {
		//go run main.go -position mysql-bin.000469 324783 1619431429
		var err error
		var pp, timestamp uint32
		others = flag.Args()
		if len(others) != 3 {
			println("error: please input the binlog's File and Position")
			return
		}
		f := others[0]
		p := others[1]
		timestampStr := others[2]

		if !strings.HasPrefix(f, "mysql-bin.") {
			println("error: The parameter File must be like: mysql-bin.000001")
			return
		}

		pp, err = stringutil.ToUint32(p)
		if nil != err {
			println("error: The parameter Position must be number")
			return
		}

		timestamp, err = stringutil.ToUint32(timestampStr)
		if nil != err {
			println("error: The parameter timestamp must be number")
			return
		}

		//ps := storage.NewPositionStorage(global.Cfg())
		pos := global.PosRequest{
			Name:      f,
			Pos:       pp,
			Timestamp: timestamp,
		}

		util.StoreNewPos(pos)
		fmt.Printf("The current dump position is : %s, %d, %d \n", f, pp, timestamp)
	}

	if !util.GIncrFlag {
		logutil.Infof("OK.")
	}
	//if stockFlag {
	//	transfer := service.TransferServiceIns()
	//	stock := service.NewStockService(transfer)
	//	err = stock.Run()
	//	if err != nil {
	//		println(errors.ErrorStack(err))
	//	}
	//	stock.Close()
	//	return
	//}
	//开始上传 promethus 监控数据
	global.StartMonitor()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan,
		os.Kill,
		os.Interrupt,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	if global.Cfg().NotCluster() {
		service.StartApplication()
	} else {
		service.BootCluster()
	}

	select {
	case sig := <-signalChan:
		log.Printf("Application Stop，Signal: %s \n", sig.String())
	case <-service.CtxDone():
		log.Printf("context is done with %v, closing", service.CtxErr())
	}

	service.CloseApplication()
}

func usage() {
	fmt.Fprintf(os.Stderr, `version: 1.0.0
Usage: transfer [-c filename] [-s stock]

Options:
`)
	flag.PrintDefaults()
}
