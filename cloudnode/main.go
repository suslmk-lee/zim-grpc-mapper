package main

import (
	"context"
	"database/sql"
	"log"
	"net"

	_ "github.com/lib/pq"
	pb "github.com/suslmk-lee/zim-grpc-mapper/pb"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedSensorServiceServer
	db *sql.DB // PostgreSQL 연결
}

func (s *server) SendSensorData(ctx context.Context, req *pb.SensorData) (*pb.SensorResponse, error) {
	log.Printf("Received data: Device=%s, Timestamp=%s, Model=%s, Tyield=%.2f, Mode=%s",
		req.Device, req.Timestamp, req.Model, req.Status.Tyield, req.Status.Mode)

	// 데이터 삽입 쿼리
	query := `
		INSERT INTO sensor_data (
			device, timestamp, prover, minorver, sn, model, tyield, dyield, pf, pmax, pac, sac,
			uab, ubc, uca, ia, ib, ic, freq, tmod, tamb, mode, qac,
			bus_capacitance, ac_capacitance, pdc, pmaxlim, smaxlim
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
			$13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23,
			$24, $25, $26, $27, $28
		)`

	_, err := s.db.Exec(query,
		req.Device, req.Timestamp, req.ProVer, req.MinorVer, req.SN, req.Model,
		req.Status.Tyield, req.Status.Dyield, req.Status.PF, req.Status.Pmax, req.Status.Pac, req.Status.Sac,
		req.Status.Uab, req.Status.Ubc, req.Status.Uca, req.Status.Ia, req.Status.Ib, req.Status.Ic,
		req.Status.Freq, req.Status.Tmod, req.Status.Tamb, req.Status.Mode, req.Status.Qac,
		req.Status.BusCapacitance, req.Status.AcCapacitance, req.Status.Pdc, req.Status.PmaxLim, req.Status.SmaxLim,
	)

	if err != nil {
		log.Printf("Failed to insert data into PostgreSQL: %v", err)
		return &pb.SensorResponse{Status: "Failed to save data"}, err
	}

	return &pb.SensorResponse{Status: "Data received and saved successfully"}, nil
}

func main() {
	// PostgreSQL 연결 설정
	connStr := "postgresql://user:password@localhost:5432/sensordb?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	// 데이터베이스 연결 테스트
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping PostgreSQL: %v", err)
	}

	log.Println("Connected to PostgreSQL")

	// gRPC 서버 설정
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen on port 50051: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterSensorServiceServer(grpcServer, &server{db: db})

	log.Println("CloudCore gRPC server is running on port 50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
