package mqttclient

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/viper"

	db "thingspanel-TDengine/db"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var count int64
var wg *sync.WaitGroup
var ctx context.Context
var c context.CancelFunc

type mqttPayload struct {
	Token    string `json:"token"`
	DeviceId string `json:"device_id"`
	Values   []byte `json:"values"`
}

func GenTopic(topic string) string {
	topic = path.Join("$share/mygroup", topic)
	return topic
}

func MqttInit() {
	fmt.Println("init mqtt_client")
	Connect() // 连接MQTT服务器
	fmt.Println("init mqtt_client success")

}

// 连接MQTT服务器
func Connect() {
	var c mqtt.Client
	// 掉线重连
	var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
		fmt.Printf("Mqtt Connect lost: %v", err)
		i := 0
		for {

			time.Sleep(5 * time.Second)
			if !c.IsConnectionOpen() {
				i++
				fmt.Println("Mqtt客户端掉线重连...", i)
				if token := c.Connect(); token.Wait() && token.Error() != nil {
					fmt.Println("Mqtt客户端连接失败...")
				} else {
					SubscribeTopic(c)
					break
				}
			} else {
				break
			}
		}
	}
	opts := mqtt.NewClientOptions()
	opts.SetClientID(uuid.New().String()) //设置客户端ID
	opts.SetUsername(viper.GetString("mqtt.username"))
	opts.SetPassword(viper.GetString("mqtt.password"))
	fmt.Println("MQTT连接地址", viper.GetString("mqtt.host")+":"+viper.GetString("mqtt.port"))
	opts.AddBroker(viper.GetString("mqtt.host") + ":" + viper.GetString("mqtt.port"))
	opts.SetAutoReconnect(true)                //设置自动重连
	opts.SetOrderMatters(false)                //设置为false，表示订阅的消息可以接收到所有的消息，不管订阅的顺序
	opts.OnConnectionLost = connectLostHandler //设置连接丢失的处理事件
	opts.SetOnConnectHandler(func(c mqtt.Client) {
		fmt.Println("Mqtt客户端已连接")
	}) //设置连接成功处理事件

	reconnectNumber := 0 //重连次数
	go func() {
		for { // 失败重连
			c = mqtt.NewClient(opts)
			if token := c.Connect(); token.Wait() && token.Error() != nil {
				reconnectNumber++
				fmt.Println("错误说明：", token.Error().Error())
				fmt.Println("Mqtt客户端连接失败...重试", reconnectNumber)
			} else {
				SubscribeTopic(c)
				break
			}
			time.Sleep(5 * time.Second)
		}
	}()
}

func ShutDown() {
	c()
	wg.Wait()
	log.Printf("ShutDown count: %+v\n", atomic.LoadInt64(&count))
}

// 订阅主题
func SubscribeTopic(client mqtt.Client) {
	// 启动批量写入
	// 通道缓冲区大小
	var channelBufferSize = viper.GetInt("db.channel_buffer_size")
	messages := make(chan map[string]interface{}, channelBufferSize)
	// 写入协程数
	var writeWorkers = viper.GetInt("db.write_workers")

	wg = &sync.WaitGroup{}
	ctx, c = context.WithCancel(context.Background())
	for i := 0; i < writeWorkers; i++ {
		wg.Add(1)
		w := &db.Worker{}
		go w.Bulk_inset_struct(wg, ctx, messages)
	}

	// 设置消息回调处理函数
	var qos byte = byte(viper.GetUint("mqtt.qos"))
	topic := viper.GetString("mqtt.attribute_topic")
	topic = GenTopic(topic)
	fmt.Print("topic: ", topic)
	token := client.Subscribe(topic, qos, func(client mqtt.Client, msg mqtt.Message) {
		messageHandler(messages, client, msg)
	})
	if token.Wait() && token.Error() != nil {
		fmt.Println("订阅失败")
		return
	}
	fmt.Printf("Subscribed to topic: %s\n", topic)
}

// 消息处理函数
func messageHandler(messages chan<- map[string]interface{}, _ mqtt.Client, msg mqtt.Message) {
	payload := &mqttPayload{}
	if err := json.Unmarshal(msg.Payload(), &payload); err != nil {
		log.Printf("Failed to unmarshal MQTT message: %v", err)
		return
	}

	// 将消息写入通道
	var deviceID string
	if len(payload.DeviceId) > 0 {
		deviceID = payload.DeviceId
	} else {
		log.Printf("not exist device_id in payload")
		return
	}

	var valuesMap map[string]interface{}
	//byte转map
	if err := json.Unmarshal(payload.Values, &valuesMap); err != nil {
		log.Printf("Failed to unmarshal MQTT message: %v", err)
		return
	}

	valuesMap["device_id"] = deviceID

	select {
	case messages <- valuesMap:
		// atomic.AddInt64(&count, 1)
	default:
		log.Printf("can not write msg:%+v\n", valuesMap)
	}

	// log.Printf("count: %+v\n", atomic.LoadInt64(&count))
}
