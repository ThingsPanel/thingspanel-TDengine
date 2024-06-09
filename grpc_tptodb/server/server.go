package server

import (
	"flag"
	"fmt"
	"log"
	"net"

	pb "thingspanel-TDengine/grpc_tptodb"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

// server is used to implement helloworld.GreeterServer.
type server struct {
	pb.UnimplementedGreeterServer
	pb.ThingsPanelServer
}

func GrpcInit() {
	go Listen()
}

func Listen() {
	var (
		port = flag.Int("port", viper.GetInt("grpc.port"), "The server port")
	)
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &server{})
	pb.RegisterThingsPanelServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
