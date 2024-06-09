package db

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/spf13/viper"
	_ "github.com/taosdata/driver-go/v3/taosRestful"
)

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
		log.Fatalf("Failed to initialize Cassandra: %v", err)
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

	err = createSubTables()
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

func Bulk_inset_struct(messages <-chan map[string]interface{}) {
	batchWaitTime := viper.GetDuration("db.batch_wait_time") * time.Second
	batchSize := viper.GetInt("db.batch_size")
	var demos = make([]zorm.IEntityStruct, 0)

	for {
		var table string
		startTime := time.Now()
		for i := 0; i < batchSize; i++ {
			if time.Since(startTime) > batchWaitTime {
				break
			}

			message, ok := <-messages
			if !ok {
				break
			}

			//随机入表
			table = fmt.Sprintf("%s00%d", SuperTableTv, rand.Intn(viper.GetInt("db.subtablenum")))
			if _, ok := message["device_id"]; ok {
				if value, ok := message["value"].(string); ok {
					demo1 := Demo{Ts: time.Now(),
						DeviceId:  fmt.Sprintf("%v", message["device_id"]),
						K:         fmt.Sprintf("%v", message["key"]),
						StringV:   fmt.Sprintf("%v", value),
						NumberV:   NumberDefault,
						BoolV:     -1,
						TableName: fmt.Sprintf("%s.%s", DBName, table)}
					demos = append(demos, &demo1)
				} else if f, ok := message["value"].(float64); ok {
					demo2 := Demo{Ts: time.Now(),
						DeviceId:  fmt.Sprintf("%v", message["device_id"]),
						K:         fmt.Sprintf("%v", message["key"]),
						NumberV:   f,
						StringV:   StringDefault,
						BoolV:     -1,
						TableName: fmt.Sprintf("%s.%s", DBName, table)}
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
						TableName: fmt.Sprintf("%s.%s", DBName, table)}
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
			} else {
				log.Printf("len:%d, num:%v\n", len(demos), num)
			}
			demos = demos[:0]
		}
	}
}
