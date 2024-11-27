package main

import (
	"context"
	"log"
	"net"

	"../pb"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedSensorServiceServer
}

func (s *server) SendSensorData(ctx context.Context, req *pb.SensorData) (*pb.SensorResponse, error) {
	log.Printf("Received data: Device=%s, Timestamp=%s, Model=%s, Tyield=%.2f, Mode=%s",
		req.Device, req.Timestamp, req.Model, req.Status.Tyield, req.Status.Mode)
	return &pb.SensorResponse{Status: "Data received successfully"}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen on port 50051: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterSensorServiceServer(grpcServer, &server{})

	log.Println("CloudCore gRPC server is running on port 50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
