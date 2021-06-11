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
	"go-mysql-transfer/util"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/juju/errors"
	"go-mysql-transfer/global"
	"go-mysql-transfer/service"
	"go-mysql-transfer/storage"
	"go-mysql-transfer/util/logutil"
	"go-mysql-transfer/util/stringutil"
	_ "net/http/pprof"
)

var (
	helpFlag      bool
	cfgPath       string
	stockFlag     bool
	positionFlag  bool
	statusFlag    bool
	deletePosFlag bool
	full          bool
)

func init() {
	flag.BoolVar(&helpFlag, "help", false, "this help")
	flag.StringVar(&cfgPath, "config", "app.yml", "application config file")

	//按表查数据导入
	//flag.BoolVar(&stockFlag, "stock", false, "stock data import")

	//设置当前binlog位置并且开始运行    go run main.go -position mysql-bin.000469 324783 1619431429
	flag.BoolVar(&positionFlag, "position", false, "set dump position")
	//查询当前binlog位置              go run main.go -status 900
	flag.BoolVar(&statusFlag, "status", false, "Query the position of many seconds age,This position is the first one greater than yours seconds.")
	//是否同步表结构                   go run main.go -full
	flag.BoolVar(&full, "full", false, "sync table struct or not")
	//position data 日志过大，清理data    go run main.go -deletepos
	flag.BoolVar(&deletePosFlag, "deletepos", false, "sync table struct or not")

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
	util.StoreVal(full)
	//-----------------------------------
	go http.ListenAndServe(":9999", nil)
	//-----------------------------------
	n := runtime.GOMAXPROCS(runtime.NumCPU())

	// cfgPath = "D:\\transfer\\app_redis_lua.yml"
	// stockFlag = true

	err := service.InitApplication(cfgPath)
	if err != nil {
		println(
			errors.ErrorStack(err))
		return
	}

	logutil.Infof("GOMAXPROCS :%d ", n)

	others := make([]string, 2)
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
