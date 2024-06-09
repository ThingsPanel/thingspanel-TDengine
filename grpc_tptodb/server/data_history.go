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

// SayHello implements helloworld.GreeterServer
func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	log.Printf("Received: %v", in.GetName())
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

// 设备数据历史记录
func (s *server) GetDeviceAttributesHistory(ctx context.Context, in *pb.GetDeviceAttributesHistoryRequest) (*pb.GetDeviceAttributesHistoryReply, error) {
	// 时间是毫秒数字时间戳，需要转成time.Time
	startTime := time.Unix(0, in.GetStartTime()*int64(time.Millisecond))
	endTime := time.Unix(0, in.GetEndTime()*int64(time.Millisecond))

	log.Print("st: ", startTime.String())
	log.Print("ed: ", endTime.String())

	var dataSlice [][]map[string]interface{}
	var attributeList []string = in.GetAttribute()
	finder := zorm.NewFinder()

	// 用indexList记录dataSlice中的每个list中的下标,初始化每个list的下标为0
	var indexList []int
	var err error

	// 遍历Attributes
	for _, v := range attributeList {
		if v == "" || v == "systime" {
			continue
		}
		indexList = append(indexList, 0)
		// 获取每个属性的历史数据列表
		var dataList []map[string]interface{}

		finder.Append(fmt.Sprintf("SELECT ts,k,bool_v,number_v,string_v,tenant_id FROM %s.%s WHERE device_id = ? AND k = ? AND ts >= ? AND ts <= ? order by ts asc",
			db.DBName, db.SuperTableTv), in.GetDeviceId(), v, startTime, endTime)
		dataList, err = zorm.QueryMap(ctx, finder, nil)
		if err != nil {
			log.Printf("Failed to get data from ts_kv: %v", err)
		}

		// 根据时间从数据库获取数据
		// iter := db.CassandraSession.Query("SELECT key,ts,dbl_v,str_v FROM ts_kv WHERE device_id = ? AND key = ? AND ts >= ? AND ts <= ? order by ts asc", in.GetDeviceId(), v, startTime, endTime).Iter()
		// // 将结果存入dataList
		// dataList, err := iter.SliceMap()
		// if err != nil {
		// 	log.Printf("Failed to get data from ts_kv: %v", err)
		// }

		dataSlice = append(dataSlice, dataList)
	}

	var dataMap = make(map[string][]interface{})

	// 每个列的时间戳数组
	for {
		var nullCount int
		// 清空tsList
		var tsList []interface{}
		// 取当前下标里数据的ts,如果都为空值，则跳出循环
		for i, v := range indexList {

			// 超过或等于indexList数据长度的下标赋空值
			if v < len(dataSlice[i]) {
				tsList = append(tsList, dataSlice[i][indexList[i]]["ts"])
			} else {
				tsList = append(tsList, nil)
				nullCount++
			}
		}
		if nullCount == len(indexList) {
			break
		}
		// 判断tsList哪个下标的ts最小
		minIndex := 0
		for i := 1; i < len(tsList); i++ {
			// tsList为空值的下标不参与比较
			if tsList[i] != nil {
				if tsList[i].(time.Time).Before(tsList[minIndex].(time.Time)) {
					minIndex = i
				}
			}
		}
		// fmt.Println("--------------------------", tsList)
		// tsList中相等的ts的下标都存入dataMap
		for i, v := range tsList {
			//自己不和自己比较
			if i == minIndex {
				// 只在存最小的ts时存systime
				// 格式化时间
				dataMap["systime"] = append(dataMap["systime"], v.(time.Time).Format("2006-01-02 15:04:05"))
				//直接赋值
				// 判断是否是字符串
				if dataSlice[i][indexList[i]]["string_v"].(string) != "" {
					dataMap[attributeList[i]] = append(dataMap[attributeList[i]], dataSlice[i][indexList[i]]["string_v"].(string))
				} else {
					dataMap[attributeList[i]] = append(dataMap[attributeList[i]], dataSlice[i][indexList[i]]["number_v"].(float64))
				}
				//下标加1
				indexList[i]++
			} else {
				// tsList为空值的下标直接赋空值
				if v != nil {
					// 判断是否有相等的ts
					if v.(time.Time).Equal(tsList[minIndex].(time.Time)) {
						// 判断是否是字符串
						if dataSlice[i][indexList[i]]["string_v"].(string) != "" {
							dataMap[attributeList[i]] = append(dataMap[attributeList[i]], dataSlice[i][indexList[i]]["string_v"].(string))
						} else {
							dataMap[attributeList[i]] = append(dataMap[attributeList[i]], dataSlice[i][indexList[i]]["number_v"].(float64))
						}
						//下标加1
						indexList[i]++
					} else {
						// 添加空值
						dataMap[attributeList[i]] = append(dataMap[attributeList[i]], nil)
					}
				} else {
					// 添加空值
					dataMap[attributeList[i]] = append(dataMap[attributeList[i]], nil)
				}
			}
		}
	}
	// 将dataMap转成json字符串
	jsonStr, err := json.Marshal(dataMap)
	if err != nil {
		log.Printf("Failed to marshal dataMap: %v", err)
	}
	fmt.Println("dataMap", dataMap)
	return &pb.GetDeviceAttributesHistoryReply{Status: 1, Message: "", Data: string(jsonStr)}, nil
}

// 设备历史数据记录
func (s *server) GetDeviceHistory(ctx context.Context, in *pb.GetDeviceHistoryRequest) (*pb.GetDeviceHistoryReply, error) {
	// 时间是毫秒数字时间戳，需要转成time.Time
	startTime := time.Unix(0, in.GetStartTime()*int64(time.Millisecond))
	endTime := time.Unix(0, in.GetEndTime()*int64(time.Millisecond))

	// 最长查询时间间隔为100天，超过100天默认查询100天
	if endTime.Sub(startTime) > 100*24*time.Hour {
		startTime = endTime.Add(-100 * 24 * time.Hour)
	}
	// var query string
	var deviceId string = in.GetDeviceId()
	var limit int64 = in.GetLimit()
	if limit <= 0 {
		limit = 10
	}
	var total int64
	var err error

	var dataMapList []map[string]interface{}
	// 查询表ts_kv，获取总数
	if in.GetKey() != "" {
		finder := zorm.NewFinder()
		finder.Append(fmt.Sprintf("SELECT ts,k,bool_v,number_v,string_v,tenant_id FROM %s.%s WHERE device_id = ? AND k = ? AND ts >= ? AND ts <= ? order by ts desc",
			db.DBName, db.SuperTableTv), deviceId, in.GetKey(), startTime, endTime)

		page := zorm.NewPage()
		page.PageNo = 1            // 查询第1页,默认是1
		page.PageSize = int(limit) // 每页20条,默认是20

		// 不查询总条数
		// finder.SelectTotalCount = false
		// 如果是特别复杂的语句,造成count语句构造失败,可以手动指定count语句.或者OverrideFunc复写selectCount函数,全局替换实现
		// countFinder := zorm.NewFinder().Append("select count(*) from (")
		// countFinder.AppendFinder(finder)
		// countFinder.Append(") tempcountfinder")
		// finder.CountFinder = countFinder

		// 执行查询
		dataMapList, err = zorm.QueryMap(ctx, finder, page)
		if err != nil { // 标记测试失败
			log.Printf("Failed to get total from ts_kv")
			return &pb.GetDeviceHistoryReply{Status: 0, Message: "Failed to get total from ts_kv", Data: ""}, nil
		}
		// 打印结果
		log.Println("总条数:", page.TotalCount)
		total = int64(page.TotalCount)

		if total == 0 {
			log.Printf("total is 0")
			return &pb.GetDeviceHistoryReply{Status: 0, Message: "Failed to get total from ts_kv", Data: ""}, nil
		}
	} else {
		// 提示不支持
		return &pb.GetDeviceHistoryReply{Status: 0, Message: "Not supported", Data: ""}, nil
	}

	var retMapList []map[string]interface{}
	for _, mp := range dataMapList {
		m := make(map[string]interface{}, 0)
		if string_v, ok := mp["string_v"]; ok {
			fmt.Printf("string_v:%+v\n", string_v)
			if fmt.Sprintf("%v", string_v) != db.StringDefault {
				m["string_v"] = string_v
			}
		}

		if number_v, ok := mp["number_v"]; ok {
			fmt.Printf("number_v:%+v\n", number_v)
			if v, ok := number_v.(float64); ok && v != db.NumberDefault {
				m["number_v"] = v
			}
		}

		if bool_v, ok := mp["bool_v"]; ok {
			fmt.Printf("bool_v:%+v\n", bool_v)
			if v, ok := bool_v.(int); ok && v != db.BoolDefault {
				m["bool_v"] = v
			}
		}

		if ts, ok := mp["ts"]; ok && ts != "" {
			// 解析时间字符串
			t, err := time.Parse(layout, fmt.Sprintf("%s", ts))
			if err != nil {
				log.Println("Error parsing time:", err)
				return nil, err
			}
			m["ts"] = t.UnixNano() / int64(time.Millisecond)
		}

		if _, ok := mp["k"]; ok {
			m["key"] = mp["k"]
		}
		if len(m) > 0 {
			retMapList = append(retMapList, m)
		}
	}

	var retMap = make(map[string]interface{})
	retMap["total"] = total
	retMap["data"] = retMapList

	// 将map转成json
	dataJson, err := json.Marshal(retMap)
	if err != nil {
		log.Printf("Failed to marshal dataMap: %v", err)
		return &pb.GetDeviceHistoryReply{Status: 0, Message: "Failed to marshal dataMap", Data: ""}, nil
	}
	return &pb.GetDeviceHistoryReply{Status: 1, Message: "", Data: string(dataJson)}, nil
}

func (s *server) GetDeviceHistoryWithPageAndPage(ctx context.Context, in *pb.GetDeviceHistoryWithPageAndPageRequest) (*pb.GetDeviceHistoryWithPageAndPageReply, error) {
	var baseQuery string
	startTime := in.GetStartTime()
	endTime := in.GetEndTime()
	firstDataTime := in.GetFirstDataTime()
	endDataTime := in.GetEndDataTime()

	if in.GetFirstDataTime() == 0 {
		if in.GetEndDataTime() != 0 {
			// 向后翻页
			baseQuery = "SELECT ts,k,bool_v,number_v,string_v,tenant_id FROM %s.%s WHERE device_id = ? AND k = ? AND ts > ? AND ts <= ?"
			startTime = endDataTime
		} else {
			// 正常第一页
			baseQuery = "SELECT ts,k,bool_v,number_v,string_v,tenant_id FROM %s.%s WHERE device_id = ? AND k = ? AND ts >= ? AND ts <= ?"
		}
	} else {
		// 向前翻页
		baseQuery = "SELECT ts,k,bool_v,number_v,string_v,tenant_id FROM %s.%s WHERE device_id = ? AND k = ? AND ts >= ? AND ts < ?"
		endTime = firstDataTime
	}

	startTime2 := time.Unix(0, startTime*int64(time.Millisecond))
	endTime2 := time.Unix(0, endTime*int64(time.Millisecond))

	finder := zorm.NewFinder()
	finder.Append(fmt.Sprintf(baseQuery, db.DBName, db.SuperTableTv), in.GetDeviceId(), in.GetKey(), startTime2, endTime2)
	limit := in.GetPageRecords()
	if limit == 0 {
		limit = 20
	}
	page := zorm.NewPage()
	page.PageNo = 1            // 查询第1页,默认是1
	page.PageSize = int(limit) // 每页20条,默认是20

	result, err := zorm.QueryMap(ctx, finder, page)
	if err != nil {
		log.Printf("Failed to QueryMap err: %v\n", err)
		return &pb.GetDeviceHistoryWithPageAndPageReply{Status: 0, Message: "Failed to QueryMap", Data: ""}, nil
	}

	for _, mp := range result {
		m := make(map[string]interface{}, 0)
		if string_v, ok := mp["string_v"]; ok {
			fmt.Printf("string_v:%+v\n", string_v)
			if fmt.Sprintf("%v", string_v) != db.StringDefault {
				m["string_v"] = string_v
			}
		}

		if number_v, ok := mp["number_v"]; ok {
			fmt.Printf("number_v:%+v\n", number_v)
			if v, ok := number_v.(float64); ok && v != db.NumberDefault {
				m["number_v"] = v
			}
		}

		if bool_v, ok := mp["bool_v"]; ok {
			fmt.Printf("bool_v:%+v\n", bool_v)
			if v, ok := bool_v.(int); ok && v != db.BoolDefault {
				m["bool_v"] = v
			}
		}

		if ts, ok := mp["ts"]; ok && ts != "" {
			// 解析时间字符串
			t, err := time.Parse(layout, fmt.Sprintf("%s", ts))
			if err != nil {
				log.Println("Error parsing time:", err)
				return nil, err
			}
			m["ts"] = t.UnixNano() / int64(time.Millisecond)
		}

		if _, ok := mp["k"]; ok {
			m["key"] = mp["k"]
		}
		if len(m) > 0 {
			result = append(result, m)
		}
	}

	if firstDataTime != 0 && endDataTime == 0 {
		// 如果是向前翻页，因为结果默认是升序的，所以需要反转结果
		for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
			result[i], result[j] = result[j], result[i]
		}
	}

	dataJson, err := json.Marshal(result)
	if err != nil {
		log.Printf("Failed to marshal dataMap: %v", err)
		return &pb.GetDeviceHistoryWithPageAndPageReply{Status: 0, Message: "Failed to marshal dataMap", Data: ""}, nil
	}

	return &pb.GetDeviceHistoryWithPageAndPageReply{Status: 1, Message: "", Data: string(dataJson)}, nil
}
