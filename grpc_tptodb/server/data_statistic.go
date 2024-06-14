package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	db "thingspanel-TDengine/db"
	pb "thingspanel-TDengine/grpc_tptodb"

	"gitee.com/chunanyong/zorm"
)

type result struct {
	tmpMap map[string]interface{}
	err    error
}

// 不聚合查询
func (s *server) GetDeviceKVDataWithNoAggregate(ctx context.Context, in *pb.GetDeviceKVDataWithNoAggregateRequest) (*pb.GetDeviceKVDataWithNoAggregateReply, error) {
	var deviceId string = in.GetDeviceId()
	var key string = in.GetKey()

	startTime := time.Unix(0, in.GetStartTime()*int64(time.Millisecond))
	endTime := time.Unix(0, in.GetEndTime()*int64(time.Millisecond))

	// 暂时先这样
	// startTime, _ := time.Parse("2006-01-02 15:04:05", t1.Format("2006-01-02 15:04:05"))
	// endTime, _ := time.Parse("2006-01-02 15:04:05", t2.Format("2006-01-02 15:04:05"))

	finder := zorm.NewFinder()
	query := "SELECT ts,k,bool_v,number_v,string_v,tenant_id FROM %s.%s WHERE device_id = ? AND k = ? AND ts >= ? AND ts <= ? order by ts asc"
	finder.Append(fmt.Sprintf(query, db.DBName, db.SuperTableTv), deviceId, key, startTime, endTime)
	dataMap, err := zorm.QueryMap(ctx, finder, nil)
	if err != nil {
		return &pb.GetDeviceKVDataWithNoAggregateReply{Status: 1, Message: err.Error(), Data: string("{}")}, nil
	}

	log.Print("len: ", len(dataMap))
	// 格式化
	timeSeries := make([]map[string]interface{}, len(dataMap))
	for i, v := range dataMap {
		tmpMap := make(map[string]interface{})
		ts, ok := v["ts"].(time.Time)
		if ok {
			tmpMap["x"] = ts.UnixMilli() // 处理时间戳成微秒
			tmpMap["y"] = v["number_v"]  // 处理横轴
			timeSeries[i] = tmpMap
		}
	}
	jsonStr, err := json.Marshal(timeSeries)
	if err != nil {
		log.Printf("Failed to marshal dataMap: %v", err)
		return &pb.GetDeviceKVDataWithNoAggregateReply{Status: 1, Message: err.Error(), Data: string("{}")}, nil
	}

	return &pb.GetDeviceKVDataWithNoAggregateReply{Status: 1, Message: "", Data: string(jsonStr)}, nil
}

func (s *server) GetDeviceKVDataWithAggregate(ctx context.Context, in *pb.GetDeviceKVDataWithAggregateRequest) (*pb.GetDeviceKVDataWithAggregateReply, error) {
	var deviceId string = in.GetDeviceId()
	var key string = in.GetKey()
	currentStartTime := in.GetStartTime()
	currentEnd := in.GetEndTime()
	window := in.GetAggregateWindow() //毫秒
	log.Printf("currentStartTime: %v, window: %v\n", currentStartTime, window)

	startTimeParsed := time.Unix(0, currentStartTime*int64(time.Millisecond))
	endTimeParsed := time.Unix(0, currentEnd*int64(time.Millisecond))

	finder := zorm.NewFinder()
	aggregateFunc := in.GetAggregateFunc()
	queryStr := fmt.Sprintf("SELECT %s(number_v) AS v FROM things.ts_kv WHERE ts >= ? AND ts <= ? AND k = ? AND device_id = ? INTERVAL(%ds)",
		aggregateFunc, window/1000)
	finder.Append(queryStr, startTimeParsed, endTimeParsed, key, deviceId)

	dataMap, err := zorm.QueryMap(ctx, finder, nil)
	if err != nil {
		log.Printf("Failed to SliceMap dataMap: %v", err)
		return nil, err
	}

	// log.Printf("%+v\n", dataMap)

	dataMapList := make([]map[string]interface{}, 0)
	for _, v := range dataMap {
		tmpMap := make(map[string]interface{})
		tmpMap["x"] = currentStartTime
		tmpMap["x2"] = currentStartTime + window
		tmpMap["y"] = v["v"]
		dataMapList = append(dataMapList, tmpMap)
		currentStartTime = currentStartTime + window
	}

	// resultsChan := make(chan result, 2000)
	// var wg sync.WaitGroup
	// var number int = 0
	// for currentStartTime <= in.GetEndTime() {
	// 	number++
	// 	if number > 500 { // 限制最大并发数
	// 		log.Println("number > 500")
	// 		break
	// 	}
	// 	wg.Add(1)
	// 	go func(startTime int64) {
	// 		defer wg.Done()

	// 		currentEnd := startTime + window
	// 		// t1 := time.Unix(0, startTime*int64(time.Millisecond))
	// 		// t2 := time.Unix(0, currentEnd*int64(time.Millisecond))
	// 		startTimeParsed := time.Unix(0, startTime*int64(time.Millisecond))
	// 		endTimeParsed := time.Unix(0, currentEnd*int64(time.Millisecond))
	// 		// startTimeParsed, _ := time.Parse("2006-01-02 15:04:05", t1.Format("2006-01-02 15:04:05"))
	// 		// endTimeParsed, _ := time.Parse("2006-01-02 15:04:05", t2.Format("2006-01-02 15:04:05"))

	// 		finder := zorm.NewFinder()
	// 		aggregateFunc := in.GetAggregateFunc()
	// 		queryStr := fmt.Sprintf("SELECT %s(number_v) as v FROM %s.%s WHERE device_id = ? AND k = ? AND ts >= ? AND ts <= ?", aggregateFunc, db.DBName, db.SuperTableTv)
	// 		finder.Append(queryStr, deviceId, key, startTimeParsed, endTimeParsed)
	// 		dataMap, err := zorm.QueryMap(ctx, finder, nil)
	// 		if err != nil {
	// 			log.Printf("Failed to SliceMap dataMap: %v", err)
	// 			resultsChan <- result{err: err}
	// 			return
	// 		}

	// 		if len(dataMap) == 0 {
	// 			resultsChan <- result{err: errors.New("nil ")}
	// 			return
	// 		}

	// 		tmpMap := make(map[string]interface{})
	// 		tmpMap["x"] = startTime
	// 		tmpMap["x2"] = currentEnd
	// 		tmpMap["y"] = dataMap[0]["v"]

	// 		resultsChan <- result{tmpMap: tmpMap}
	// 	}(currentStartTime)

	// 	currentStartTime += window
	// }

	// go func() {
	// 	wg.Wait()
	// 	close(resultsChan)
	// }()

	// timeSeries := make(map[int64]map[string]interface{}, 0)
	// xkey := []int64{}
	// for res := range resultsChan {
	// 	if res.err == nil {
	// 		x := res.tmpMap["x"].(int64)
	// 		xkey = append(xkey, x)
	// 		timeSeries[x] = res.tmpMap
	// 	}
	// }

	// sort.Slice(xkey, func(i, j int) bool {
	// 	return xkey[i] < xkey[j]
	// })

	// timeSeriesResp := make([]map[string]interface{}, 0)
	// for _, v := range xkey {
	// 	timeSeriesResp = append(timeSeriesResp, timeSeries[v])
	// }

	log.Println("timeSeries len:", len(dataMapList))
	jsonStr, err := json.Marshal(dataMapList)

	if err != nil {
		log.Printf("Failed to marshal dataMap: %v", err)
		return &pb.GetDeviceKVDataWithAggregateReply{Status: 1, Message: "", Data: string("{}")}, nil
	}

	return &pb.GetDeviceKVDataWithAggregateReply{Status: 1, Message: "", Data: string(jsonStr)}, nil
}
