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

const layout = "2006-01-02 15:04:05.999 -0700 MST"

// 设备数据当前值记录
func (s *server) GetDeviceAttributesCurrents(ctx context.Context, in *pb.GetDeviceAttributesCurrentsRequest) (*pb.GetDeviceAttributesCurrentsReply, error) {
	var deviceId string = in.GetDeviceId()
	var attributeList []string = in.GetAttribute()

	finder := zorm.NewFinder()
	var retMap = make([]map[string]interface{}, 0)
	var dataMap = make([]map[string]interface{}, 0)
	var err error

	// 查询表ts_kv
	if len(attributeList) == 0 {
		finder.Append(fmt.Sprintf("SELECT distinct k FROM %s.%s WHERE device_id = ?",
			db.DBName, db.SuperTableTv), deviceId)
		dataMaptmp1, err := zorm.QueryMap(ctx, finder, nil)
		if err != nil {
			return nil, err
		}
		log.Println("dataMaptmp1: ", dataMaptmp1)

		for _, mp := range dataMaptmp1 {
			if _, ok := mp["k"]; ok {
				attributeList = append(attributeList, fmt.Sprintf("%v", mp["k"]))
			}
		}

		log.Println("attributeList: ", attributeList)

		for i := 0; i < len(attributeList); i++ {
			finder = zorm.NewFinder()
			finder.Append(fmt.Sprintf("SELECT ts,k,bool_v,number_v,string_v,tenant_id FROM %s.%s WHERE device_id = ? AND k in (?) order by ts desc limit 1",
				db.DBName, db.SuperTableTv), deviceId, attributeList[i])
			dataMaptmp2, err := zorm.QueryMap(ctx, finder, nil)
			if err != nil {
				log.Println("QueryMap: ", err)
				continue
			}
			if len(dataMaptmp2) > 0 {
				dataMap = append(dataMap, dataMaptmp2...)
			}
		}

	} else {
		for i := 0; i < len(attributeList); i++ {
			finder = zorm.NewFinder()
			finder.Append(fmt.Sprintf("SELECT ts,k,bool_v,number_v,string_v,tenant_id FROM %s.%s WHERE device_id = ? AND k in (?) order by ts desc limit 1",
				db.DBName, db.SuperTableTv), deviceId, attributeList[i])
			dataMaptmp2, err := zorm.QueryMap(ctx, finder, nil)
			if err != nil {
				log.Println("QueryMap: ", err)
				continue
			}
			if len(dataMaptmp2) > 0 {
				dataMap = append(dataMap, dataMaptmp2...)
			}
		}
	}

	for _, mp := range dataMap {
		m := make(map[string]interface{}, 0)
		if string_v, ok := mp["string_v"]; ok {
			if fmt.Sprintf("%v", string_v) != db.StringDefault {
				m["string_v"] = string_v
			}
		}

		if number_v, ok := mp["number_v"]; ok {
			if v, ok := number_v.(float64); ok && v != db.NumberDefault {
				m["number_v"] = v
			}
		}

		if bool_v, ok := mp["bool_v"]; ok {
			// fmt.Printf("bool_v:%+v\n", bool_v)
			if v, ok := bool_v.(int); ok && v != db.BoolDefault {
				m["bool_v"] = v
			}
		}

		if ts, ok := mp["ts"]; ok && ts != "" {

			utcTimeStr := fmt.Sprintf("%s", ts)
			loc, _ := time.LoadLocation("Asia/Shanghai") // 例如，中国上海的时区

			// 解析UTC时间
			utcTime, err := time.Parse(layout, utcTimeStr)
			if err != nil {
				return nil, err
			}
			// 将UTC时间转换为本地时间
			localTime := utcTime.In(loc)
			m["ts"] = localTime
		}

		if _, ok := mp["k"]; ok {
			m["key"] = mp["k"]
		}
		m["device_id"] = deviceId
		if _, ok := mp["tenant_id"]; ok {
			m["tenant_id"] = mp["tenant_id"]
		}
		if len(m) > 0 {
			retMap = append(retMap, m)
		}
	}

	// 将map转成json
	dataJson, err := json.Marshal(retMap)
	if err != nil {
		return nil, err
	}

	log.Println("dataJson: ", string(dataJson))
	return &pb.GetDeviceAttributesCurrentsReply{Status: 1, Message: "", Data: string(dataJson)}, nil
}

// 设备数据最新记录
func (s *server) GetDeviceAttributesCurrentList(ctx context.Context, in *pb.GetDeviceAttributesCurrentListRequest) (*pb.GetDeviceAttributesCurrentListReply, error) {
	var deviceId string = in.GetDeviceId()
	var attributeList []string = in.GetAttribute()

	finder := zorm.NewFinder()
	var dataMap = make([]map[string]interface{}, 0)
	var err error

	// 查询表ts_kv
	if len(attributeList) == 0 {
		finder.Append(fmt.Sprintf("SELECT distinct k FROM %s.%s WHERE device_id = ?",
			db.DBName, db.SuperTableTv), deviceId)
		dataMap, err = zorm.QueryMap(ctx, finder, nil)
		if err != nil {
			return nil, err
		}

		for _, mp := range dataMap {
			if _, ok := mp["k"]; ok {
				attributeList = append(attributeList, fmt.Sprintf("%v", mp["k"]))
			}
		}

		finder = zorm.NewFinder()
		finder.Append(fmt.Sprintf("SELECT ts,k,bool_v,number_v,string_v,tenant_id FROM %s.%s WHERE device_id = ? AND k in (?) order by ts desc",
			db.DBName, db.SuperTableTv), deviceId, attributeList)
		dataMap, err = zorm.QueryMap(ctx, finder, nil)
		if err != nil {
			return nil, err
		}
	} else {
		finder.Append(fmt.Sprintf("SELECT ts,k,bool_v,number_v,string_v,tenant_id FROM %s.%s WHERE device_id = ? AND k in (?) order by ts desc",
			db.DBName, db.SuperTableTv), deviceId, attributeList)
		dataMap, err = zorm.QueryMap(ctx, finder, nil)
		if err != nil {
			return nil, err
		}
	}

	var dataMapList []map[string]interface{}
	for _, mp := range dataMap {
		m := make(map[string]interface{}, 0)

		if string_v, ok := mp["string_v"]; ok {
			// fmt.Printf("string_v:%+v\n", string_v)
			if fmt.Sprintf("%v", string_v) != db.StringDefault {
				m["string_v"] = string_v
			}
		}

		if number_v, ok := mp["number_v"]; ok {
			// fmt.Printf("number_v:%+v\n", number_v)
			if v, ok := number_v.(float64); ok && v != db.NumberDefault {
				m["number_v"] = v
			}
		}

		if bool_v, ok := mp["bool_v"]; ok {
			// fmt.Printf("bool_v:%+v\n", bool_v)
			if v, ok := bool_v.(int); ok && v != db.BoolDefault {
				m["bool_v"] = v
			}
		}

		if ts, ok := mp["ts"]; ok && ts != "" {
			utcTimeStr := fmt.Sprintf("%s", ts)
			loc, _ := time.LoadLocation("Asia/Shanghai") // 例如，中国上海的时区

			// 解析UTC时间
			utcTime, err := time.Parse(layout, utcTimeStr)
			if err != nil {
				return nil, err
			}
			// 将UTC时间转换为本地时间
			localTime := utcTime.In(loc)
			m["ts"] = localTime
		}

		if _, ok := mp["k"]; ok {
			m["key"] = mp["k"]
		}

		m["device_id"] = deviceId

		if _, ok := mp["tenant_id"]; ok {
			m["tenant_id"] = mp["tenant_id"]
		}

		if len(m) > 0 {
			dataMapList = append(dataMapList, m)
		}
	}

	// 将map转成json
	dataJson, err := json.Marshal(dataMapList)
	if err != nil {
		return nil, err
	}
	log.Println("dataJson: ", string(dataJson))
	return &pb.GetDeviceAttributesCurrentListReply{Status: 1, Message: "", Data: string(dataJson)}, nil
}
