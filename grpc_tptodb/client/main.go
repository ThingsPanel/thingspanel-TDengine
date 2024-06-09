/*
 *
 * Copyright 2015 gRPC authors.
 *
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
 *
 */

// Package main implements a client for Greeter service.
package main

import (
	"context"
	"flag"
	"log"
	"time"

	pb "c/grpc_tptodb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	addr = flag.String("addr", "127.0.0.1:50052", "the address to connect to")
)

func main() {
	flag.Parse()
	// Set up a connection to the server.
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewThingsPanelClient(conn)
	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// st := time.Now()

	// r, err := c.SayHello(ctx, &pb.HelloRequest{Name: *name})

	// StartTime := time.Now().Add(-1 * time.Hour).UnixMilli()
	// EndTime := time.Now().UnixMilli()

	in := pb.GetDeviceAttributesCurrentsRequest{
		DeviceId:  "ae5a7d0c-d0fe-0d06-5c77-a47f2c771bd7",
		Attribute: make([]string, 0),
	}
	// in.Attribute = append(in.Attribute, "humidity")
	// in.Attribute = append(in.Attribute, "temperature")
	r, _ := c.GetDeviceAttributesCurrents(ctx, &in)
	log.Printf("GetDeviceAttributesCurrents: %s\n", r.GetData())

	in2 := pb.GetDeviceAttributesCurrentListRequest{
		DeviceId:  "ae5a7d0c-d0fe-0d06-5c77-a47f2c771bd7",
		Attribute: make([]string, 0),
	}
	in.Attribute = append(in.Attribute, "LightIntensity")
	in.Attribute = append(in.Attribute, "temperature")
	r2, _ := c.GetDeviceAttributesCurrentList(ctx, &in2)
	log.Printf("GetDeviceAttributesCurrentList: %s\n", r2.GetData())

	StartTime := time.Now().Add(-360 * time.Minute).UnixMilli()
	EndTime := time.Now().UnixMilli()

	// log.Printf("st: %v ed:%v\n", time.Now().String(), time.Now().String())

	in3 := pb.GetDeviceAttributesHistoryRequest{
		DeviceId:  "ae5a7d0c-d0fe-0d06-5c77-a47f2c771bd7",
		Attribute: make([]string, 0),
		StartTime: StartTime,
		EndTime:   EndTime,
	}
	in.Attribute = append(in.Attribute, "LightIntensity")
	in.Attribute = append(in.Attribute, "temperature")
	r3, _ := c.GetDeviceAttributesHistory(ctx, &in3)
	log.Printf("GetDeviceAttributesHistory: %s\n", r3.GetData())

	in4 := pb.GetDeviceHistoryRequest{
		DeviceId:  "ae5a7d0c-d0fe-0d06-5c77-a47f2c771bd7",
		Key:       "LightIntensity",
		StartTime: StartTime,
		EndTime:   EndTime,
	}
	r4, _ := c.GetDeviceHistory(ctx, &in4)
	log.Printf("GetDeviceHistory: %s\n", r4.GetData())

	in5 := pb.GetDeviceHistoryWithPageAndPageRequest{
		DeviceId:    "ae5a7d0c-d0fe-0d06-5c77-a47f2c771bd7",
		Key:         "LightIntensity",
		PageRecords: 20,
		StartTime:   StartTime,
		EndTime:     EndTime,
	}
	r5, _ := c.GetDeviceHistoryWithPageAndPage(ctx, &in5)
	log.Printf("GetDeviceHistoryWithPageAndPage: %s\n", r5.GetData())

	in6 := pb.GetDeviceKVDataWithNoAggregateRequest{
		DeviceId:  "ae5a7d0c-d0fe-0d06-5c77-a47f2c771bd7",
		Key:       "LightIntensity",
		StartTime: StartTime,
		EndTime:   EndTime,
	}
	r6, _ := c.GetDeviceKVDataWithNoAggregate(ctx, &in6)
	log.Printf("GetDeviceKVDataWithNoAggregate: %s\n", r6.GetData())

	StartTime = time.Now().Add(-1 * time.Hour).UnixMilli()
	EndTime = time.Now().UnixMilli()

	log.Printf("st: %v ed: %v\n", time.Now().String(), time.Now().String())

	in7 := pb.GetDeviceKVDataWithAggregateRequest{
		DeviceId:        "ae5a7d0c-d0fe-0d06-5c77-a47f2c771bd7",
		Key:             "LightIntensity",
		StartTime:       StartTime,
		EndTime:         EndTime,
		AggregateWindow: 30000000, //微妙
		AggregateFunc:   "AVG",
	}
	r7, _ := c.GetDeviceKVDataWithAggregate(ctx, &in7)
	log.Printf("GetDeviceKVDataWithAggregate: %s\n", r7.GetData())

	// log.Print("cost: ", time.Since(st).String())
	// if err != nil {
	// 	log.Fatalf("could not greet: %v", err)
	// }
	// log.Printf("resp: %s\n", r.GetData())
}
