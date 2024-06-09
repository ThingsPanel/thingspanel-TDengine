package db

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"

	"thingspanel-TDengine/db"

	"gitee.com/chunanyong/zorm"
	_ "github.com/taosdata/driver-go/v3/taosRestful"
)

const ShortForm = "2006-01-02 15:04:05"

type Demo struct {
	zorm.EntityStruct
	Ts        int64   `column:"ts"`
	DeviceId  string  `column:"device_id"`
	K         string  `column:"k"` //关键字key不让用
	BoolV     bool    `column:"bool_v"`
	NumberV   float64 `column:"number_v"`
	StringV   string  `column:"string_v"`
	TenantId  string  `column:"tenant_id"`
	TableName string
}

func (entity *Demo) GetTableName() string {
	return entity.TableName
}

func (entity *Demo) GetPKColumnName() string {
	return ""
}

const (
	DBName             = "things"
	SuperTableTv       = "ts_kv"
	SuperTableTvLatest = "ts_kv_latest"
)

var dbDao *zorm.DBDao
var ctx = context.Background()

func init() {
	log.Println("init db")
	if err := InitTd(); err != nil {
		log.Fatalf("Failed to initialize Cassandra: %v", err)
	}
	log.Println("init db success")
}

func InitTd() error {
	url := "root:taosdata@http(127.0.0.1:6041)/things"
	fmt.Printf("url=%v\n", url)
	dbDaoConfig := zorm.DataSourceConfig{
		//DSN 数据库的连接字符串
		DSN: url,
		//数据库驱动名称:mysql,postgres,oci8,sqlserver,sqlite3,clickhouse,dm,kingbase,aci 和Dialect对应,处理数据库有多个驱动
		//sql.Open(DriverName,DSN) DriverName就是驱动的sql.Open第一个字符串参数,根据驱动实际情况获取
		DriverName: "taosRestful",
		//数据库方言:mysql,postgresql,oracle,mssql,sqlite,clickhouse,dm,kingbase,shentong 和 DriverName 对应,处理数据库有多个驱动
		Dialect: "tdengine",
		//MaxOpenConns 数据库最大连接数 默认50
		MaxOpenConns: 10,
		//MaxIdleConns 数据库最大空闲连接数 默认50
		MaxIdleConns: 10,
		//ConnMaxLifetimeSecond 连接存活秒时间. 默认600(10分钟)后连接被销毁重建.避免数据库主动断开连接,造成死连接.MySQL默认wait_timeout 28800秒(8小时)
		ConnMaxLifetimeSecond: 600,
		DisableTransaction:    true, // 禁用全局事务
		// TDengineInsertsColumnName TDengine批量insert语句中是否有列名.默认false没有列名,插入值和数据库列顺序保持一致,减少语句长度
		// TDengineInsertsColumnName :false,
	}

	var err error
	dbDao, err = zorm.NewDBDao(&dbDaoConfig)
	if err != nil {
		fmt.Printf("%+v\n", err)
		return err
	}

	return createTdStable()
}

func createTdStable() error {
	var err error
	finder := zorm.NewFinder()
	//finder.InjectionCheck = false
	finder.Append(fmt.Sprintf("create database if not exists %s precision ? keep ?", DBName), "us", 365)
	_, err = zorm.UpdateFinder(ctx, finder)
	if err != nil {
		log.Printf("failed to create database, err:%+v\n", err)
		return err
	}

	finder = zorm.NewFinder()
	sql := fmt.Sprintf("CREATE STABLE if not exists %s.%s (ts TIMESTAMP, device_id NCHAR(64), k NCHAR(64), bool_v BOOL, number_v DOUBLE, string_v NCHAR(256), tenant_id NCHAR(64)) TAGS (model_id BINARY(64), model_name BINARY(64))", DBName, SuperTableTv)
	// fmt.Printf("sql=%s\n", sql)
	finder.Append(sql)
	_, err = zorm.UpdateFinder(ctx, finder)
	if err != nil {
		log.Printf("failed to create database, err:%+v\n", err)
		return err
	}

	createSubTables()

	log.Println("Data inserted successfully")
	return nil
}

// 创建子表
// Create tables
func createSubTables() error {

	for i := 1; i <= 3; i++ {
		finder := zorm.NewFinder()
		finder.Append(fmt.Sprintf(`create table if not exists %s.%s using %s.%s TAGS(?,?) `, DBName, fmt.Sprintf("%s00%d", SuperTableTv, i), DBName, SuperTableTv), fmt.Sprintf("00%d", i), "device")
		_, err := zorm.UpdateFinder(ctx, finder)
		if err != nil {
			log.Printf("failed to create SuperTableBigint, err: %v", err)
			return err
		}
	}
	return nil
}

// 设备数据历史记录
func Test_GetDeviceAttributesHistory(t *testing.T) {
	// 时间是毫秒数字时间戳，需要转成time.Time
	startTime, _ := time.Parse(ShortForm, "2024-05-24 23:46:37.183")
	endTime, _ := time.Parse(ShortForm, "2024-05-24 23:46:37.183")

	var dataSlice [][]map[string]interface{}
	deviceId := "123"
	attributeList := []string{"temp", "pm10"}

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

		finder.Append(fmt.Sprintf("SELECT ts,k,str_v,dbl_v FROM %s.%s WHERE device_id = ? AND k = ? AND ts >= ? AND ts <= ? order by ts asc",
			db.DBName, db.SuperTableTv), deviceId, v, startTime, endTime)
		dataList, err = zorm.QueryMap(ctx, finder, nil)
		if err != nil {
			t.Log(err)
		}
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
				if dataSlice[i][indexList[i]]["str_v"].(string) != "" {
					dataMap[attributeList[i]] = append(dataMap[attributeList[i]], dataSlice[i][indexList[i]]["str_v"].(string))
				} else {
					dataMap[attributeList[i]] = append(dataMap[attributeList[i]], dataSlice[i][indexList[i]]["dbl_v"].(float64))
				}
				//下标加1
				indexList[i]++
			} else {
				// tsList为空值的下标直接赋空值
				if v != nil {
					// 判断是否有相等的ts
					if v.(time.Time).Equal(tsList[minIndex].(time.Time)) {
						// 判断是否是字符串
						if dataSlice[i][indexList[i]]["str_v"].(string) != "" {
							dataMap[attributeList[i]] = append(dataMap[attributeList[i]], dataSlice[i][indexList[i]]["str_v"].(string))
						} else {
							dataMap[attributeList[i]] = append(dataMap[attributeList[i]], dataSlice[i][indexList[i]]["dbl_v"].(float64))
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
	log.Println("dataMap: ", string(jsonStr))
}

// 获取当前最新值
func Test_querylatest(t *testing.T) {
	finder := zorm.NewFinder()

	var dataMap = make([]map[string]interface{}, 0)
	var err error
	deviceId := "123"
	attributeList := []string{"temp", "pm10"}

	finder.Append(fmt.Sprintf("SELECT ts,str_v,dbl_v FROM %s.%s WHERE device_id = ? AND k in (?) order by ts desc limit 1",
		DBName, SuperTableTv), deviceId, attributeList)

	//max函数目前不可用先试用desc排序获取ts最大值
	// finder.Append(fmt.Sprintf("SELECT ts,k,str_v,dbl_v FROM %s.%s WHERE device_id = ? AND ts = (SELECT MAX(ts) FROM %s.%s) ",
	// 	DBName, SuperTableTv, DBName, SuperTableTv), deviceId)

	// finder.Append(fmt.Sprintf("SELECT ts,k,str_v,dbl_v FROM %s.%s WHERE device_id = ? order by ts desc limit 1",
	// 	DBName, SuperTableTv), deviceId)

	dataMap, err = zorm.QueryMap(ctx, finder, nil)
	if err != nil {
		return
	}

	var dataMapList []map[string]interface{}
	for _, mp := range dataMap {
		// 将time.Time时间转为微秒时间戳int64格式
		if _, ok := mp["ts"]; ok {
			mp["ts"] = mp["ts"].(time.Time).UnixNano() / 1e3
			dataMapList = append(dataMapList, mp)
		}
	}
	// 将map转成json
	dataJson, err := json.Marshal(dataMapList)
	if err != nil {
		return
	}
	log.Print("dataJson: ", string(dataJson))
}

// 按条件查询
func Test_query(t *testing.T) {
	var dataMap = make([]map[string]interface{}, 0)
	var retMap = make(map[string]interface{})
	defer dbDao.CloseDB()
	var err error

	finder := zorm.NewFinder()
	attributeList := []string{"temp", "pm10"}
	finder.Append(fmt.Sprintf("SELECT ts,str_v,dbl_v FROM %s.%s WHERE device_id = ? AND k in (?) order by ts desc",
		DBName, SuperTableTv), "123", attributeList)

	dataMap, err = zorm.QueryMap(ctx, finder, nil)
	if err != nil {
		return
	}

	for _, mp := range dataMap {
		if str_v, ok := mp["str_v"]; ok && str_v != "" {
			retMap["str_v"] = str_v
		} else if dbl_v, ok := mp["dbl_v"]; ok {
			retMap["dbl_v"] = dbl_v
		}

		if ts, ok := mp["ts"]; ok {
			retMap["ts"] = ts
		}
	}

	// 将map转成json
	dataJson, err := json.Marshal(dataMap)
	if err != nil {
		return
	}
	log.Print("dataJson: ", string(dataJson))
}

// Test_bulk_inset_struct 结构体插入单条
func Test_bulk_inset_struct(t *testing.T) {
	messages := make(chan map[string]interface{}, 1000)
	defer dbDao.CloseDB()

	num := 20
	for i := 0; i < num; i++ {
		m := make(map[string]interface{}, 0)

		if i%2 == 0 {
			m["ts"] = time.Now().UnixMilli()
			m["device_id"] = "123"
			m["key"] = "pm10"
			m["value"] = "1.23"
			messages <- m
		} else {
			m["ts"] = time.Now().UnixMilli()
			m["device_id"] = "123"
			m["key"] = "temp"
			m["value"] = 1.234
			messages <- m
		}
	}

	batchSize := num
	var demos = make([]zorm.IEntityStruct, 0)

	for i := 0; i < batchSize; i++ {
		message, ok := <-messages
		if !ok {
			break
		}

		if _, ok := message["device_id"]; ok {
			if value, ok := message["value"].(string); ok {
				demo1 := Demo{Ts: time.Now().UnixMilli(),
					DeviceId:  fmt.Sprintf("%v", message["device_id"]),
					K:         fmt.Sprintf("%v", message["key"]),
					StringV:   fmt.Sprintf("%v", value),
					TableName: fmt.Sprintf("%s.%s", DBName, "ts_kv001")}
				demos = append(demos, &demo1)
			} else {
				if f, ok := message["value"].(float64); ok {
					demo2 := Demo{Ts: time.Now().UnixMilli(),
						DeviceId:  fmt.Sprintf("%v", message["device_id"]),
						K:         fmt.Sprintf("%v", message["key"]),
						NumberV:   f,
						TableName: fmt.Sprintf("%s.%s", DBName, "ts_kv002")}
					demos = append(demos, &demo2)
				} else {
					t.Log("value is not float64")
				}
			}
		} else {
			fmt.Printf("device_id not exist")
		}
	}

	log.Printf("len=%v\n", len(demos))

	// 执行
	num, err := zorm.InsertSlice(ctx, demos)
	if err != nil {
		log.Printf("err:%v\n", err)
	} else {
		log.Printf("num:%v\n", num)
	}

}
