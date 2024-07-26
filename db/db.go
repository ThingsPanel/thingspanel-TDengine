package db

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/spf13/viper"
	_ "github.com/taosdata/driver-go/v3/taosRestful"
)

var Ch chan struct{}
var Num int64

type Demo struct {
	zorm.EntityStruct
	Ts        time.Time `column:"ts"`
	DeviceId  string    `column:"device_id"`
	K         string    `column:"k"` //关键字key不让用
	BoolV     int       `column:"bool_v"`
	NumberV   float64   `column:"number_v"`
	StringV   string    `column:"string_v"`
	TenantId  string    `column:"tenant_id"`
	TableName string
}

func (entity *Demo) GetTableName() string {
	return entity.TableName
}

func (entity *Demo) GetPKColumnName() string {
	return ""
}

const (
	StringDefault = "unkown"
	NumberDefault = -65535.0
	BoolDefault   = -1
	DBName        = "things"
	SuperTableTv  = "ts_kv"
)

var dbDao *zorm.DBDao
var ctx = context.Background()

func InitDb() {
	if err := InitTd(); err != nil {
		log.Fatalf("Failed to initialize db: %v", err)
	}
	log.Println("init db success")
}

func InitTd() error {
	url := fmt.Sprintf("%s:%s@http(%s:%d)/",
		viper.GetString("db.username"),
		viper.GetString("db.password"),
		viper.GetString("db.host"),
		viper.GetInt("db.port"))

	dbDaoConfig := zorm.DataSourceConfig{
		//DSN 数据库的连接字符串
		DSN: url,
		//数据库驱动名称:mysql,postgres,oci8,sqlserver,sqlite3,clickhouse,dm,kingbase,aci 和Dialect对应,处理数据库有多个驱动
		//sql.Open(DriverName,DSN) DriverName就是驱动的sql.Open第一个字符串参数,根据驱动实际情况获取
		DriverName: "taosRestful",
		//数据库方言:mysql,postgresql,oracle,mssql,sqlite,clickhouse,dm,kingbase,shentong 和 DriverName 对应,处理数据库有多个驱动
		Dialect: "tdengine",
		//MaxOpenConns 数据库最大连接数 默认50
		MaxOpenConns: 100,
		//MaxIdleConns 数据库最大空闲连接数 默认50
		MaxIdleConns: 10,
		//ConnMaxLifetimeSecond 连接存活秒时间. 默认600(10分钟)后连接被销毁重建.避免数据库主动断开连接,造成死连接.MySQL默认wait_timeout 28800秒(8小时)
		ConnMaxLifetimeSecond: 600,
		DisableTransaction:    true, // 禁用全局事务
		// TDengineInsertsColumnName TDengine批量insert语句中是否有列名.默认false没有列名,插入值和数据库列顺序保持一致,减少语句长度
		// TDengineInsertsColumnName :false,
	}

	zorm.FuncPrintSQL = func(ctx context.Context, sqlstr string, args []interface{}, execSQLMillis int64) {}

	var err error
	dbDao, err = zorm.NewDBDao(&dbDaoConfig)
	if err != nil {
		log.Printf("%+v\n", err)
		return err
	}

	return createTdStable()
}

func createTdStable() error {
	var err error
	finder := zorm.NewFinder()
	//finder.InjectionCheck = false
	finder.Append(fmt.Sprintf("create database if not exists %s precision ?  keep ?", DBName), "us", 365*2)
	_, err = zorm.UpdateFinder(ctx, finder)
	if err != nil {
		log.Printf("failed to create database, err:%+v\n", err)
		return err
	}

	finder = zorm.NewFinder()
	CreateSqlFmt := "CREATE STABLE if not exists %s.%s (ts TIMESTAMP, device_id NCHAR(64), k NCHAR(64), bool_v TINYINT, number_v DOUBLE, string_v NCHAR(256), tenant_id NCHAR(64)) TAGS (model_id BINARY(64), model_name BINARY(64))"
	sql := fmt.Sprintf(CreateSqlFmt, DBName, SuperTableTv) //超级表默认过期时间365天，超过过期时间后会自动清理所有子表数据
	finder.Append(sql)
	_, err = zorm.UpdateFinder(ctx, finder)
	if err != nil {
		log.Printf("failed to create database, err:%+v\n", err)
		return err
	}

	// err = createSubTables()
	return err
}

// 创建子表
// Create tables
func createSubTables() error {
	for i := 0; i < viper.GetInt("db.subtablenum"); i++ {
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

// 创建子表
// Create tables
func createSubTablesByName(tname, device_id string) error {
	finder := zorm.NewFinder()
	finder.Append(fmt.Sprintf(`create table if not exists %s.%s using %s.%s TAGS(?,?) `, DBName, tname, DBName, SuperTableTv), device_id, "device")
	_, err := zorm.UpdateFinder(ctx, finder)
	if err != nil {
		log.Printf("failed to create createSubTablesByName err: %v", err)
		return err
	}

	return nil
}

type Worker struct {
	Tc *time.Ticker
}

func (w *Worker) DoInsertBatch(bathlist []map[string]interface{}) {
	var err error
	var buff strings.Builder
	var demos []zorm.IEntityStruct
	for i := 0; i < len(bathlist); i++ {
		message := bathlist[i]
		if _, ok := message["device_id"]; !ok {
			log.Printf("device_id not exist in message:%v\n", message)
			continue
		}

		deviceId := fmt.Sprintf("%s", message["device_id"])

		//取deviceId中第一个-前面的作为表名
		alist := strings.Split(deviceId, "-")

		buff.WriteString(SuperTableTv)
		buff.WriteString("_")
		buff.WriteString(alist[0])
		tablename := buff.String()
		buff.Reset()

		err = createSubTablesByName(tablename, deviceId)
		if err != nil {
			log.Printf("createSubTablesByName err:%v\n", err)
			continue
		}

		if _, ok := message["device_id"]; ok {
			if value, ok := message["value"].(string); ok {
				demo1 := Demo{Ts: time.Now(),
					DeviceId:  fmt.Sprintf("%s", message["device_id"]),
					K:         fmt.Sprintf("%s", message["key"]),
					StringV:   value,
					NumberV:   NumberDefault,
					BoolV:     -1,
					TableName: DBName + "." + tablename}
				demos = append(demos, &demo1)
			} else if f, ok := message["value"].(float64); ok {
				demo2 := Demo{Ts: time.Now(),
					DeviceId:  fmt.Sprintf("%v", message["device_id"]),
					K:         fmt.Sprintf("%v", message["key"]),
					NumberV:   f,
					StringV:   StringDefault,
					BoolV:     -1,
					TableName: DBName + "." + tablename}
				demos = append(demos, &demo2)
			} else if b, ok := message["value"].(bool); ok {
				bv := 0
				if b {
					bv = 1
				}
				demo2 := Demo{Ts: time.Now(),
					DeviceId:  fmt.Sprintf("%v", message["device_id"]),
					K:         fmt.Sprintf("%v", message["key"]),
					BoolV:     bv,
					NumberV:   NumberDefault,
					StringV:   StringDefault,
					TableName: DBName + "." + tablename}
				demos = append(demos, &demo2)
			} else {
				log.Printf("err type value:%v\n", message["device_id"])
				continue
			}
		}
	}

	if len(demos) > 0 {
		// //相同结构的的子表（同一超级表下子表,如果不是必须保证类型一致）
		//tableName 是可以替换的 demo定义的是超级表结构
		num, err := zorm.InsertSlice(context.Background(), demos)
		if err != nil {
			log.Printf("err:%v\n", err)
		}

		atomic.AddInt64(&Num, int64(num))
	}
}

func (w *Worker) Bulk_inset_struct(wg *sync.WaitGroup, ctx context.Context, messages chan map[string]interface{}) {
	defer wg.Done()

	batchWaitTime := viper.GetDuration("db.batch_wait_time") * time.Second
	batchSize := viper.GetInt("db.batch_size")

	log.Printf("batchSize: %+v batchWaitTime: %+v\n", batchSize, batchWaitTime)

	w.Tc = time.NewTicker(1 * time.Second)
	defer w.Tc.Stop()
	bathlist := make([]map[string]interface{}, 0)

	for {
		select {
		case <-ctx.Done():
			if len(bathlist) > 0 {
				w.DoInsertBatch(bathlist)
			}
			return
		case message, ok := <-messages:
			if !ok {
				if len(bathlist) > 0 {
					w.DoInsertBatch(bathlist)
				}
				return
			}

			if _, ok := message["device_id"]; !ok {
				continue
			}

			deviceid := message["device_id"]
			delete(message, "device_id")

			for key, value := range message {
				info := map[string]interface{}{
					"device_id": deviceid,
					"key":       key,
					"value":     value,
					"ts":        time.Now().Nanosecond(),
				}

				bathlist = append(bathlist, info)
			}

			if (len(bathlist) >= batchSize) || (time.Since(time.Now()) >= batchWaitTime && len(bathlist) > 0) {
				w.DoInsertBatch(bathlist)
				bathlist = bathlist[0:0]
			}

		case <-w.Tc.C:
			// log.Printf("Num: %+v\n", atomic.LoadInt64(&Num))
			if len(bathlist) > 0 {
				w.DoInsertBatch(bathlist)
				bathlist = bathlist[0:0]
			}
		}
	}
}
