package main

import (
	"fmt"
	"strings"

	"thingspanel-TDengine/db"
	"thingspanel-TDengine/grpc_tptodb/server"

	mqttclient "thingspanel-TDengine/mqtt_client"

	"github.com/spf13/viper"
)

func main() {
	initConf()            // Viper初始化配置
	db.InitDb()           // 初始化数据库
	mqttclient.MqttInit() // 启动mqtt客户端
	server.GrpcInit()     // 启动grpc服务
	select {}
}

// Viper初始化配置
func initConf() {
	// Initialize Viper
	viper.SetEnvPrefix("TPCASSANDRA")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetConfigName("./conf/config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("failed to read configuration file: %s", err))
	}
}
