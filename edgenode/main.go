package main

import (
	"context"
	"log"
	"os"
	"time"

	pb "github.com/suslmk-lee/zim-grpc-mapper/pb"
	"google.golang.org/grpc"
)

func main() {
	// 환경변수에서 CloudCore URL과 포트 가져오기
	cloudCoreURL := os.Getenv("CLOUD_CORE_URL")
	if cloudCoreURL == "" {
		cloudCoreURL = "cloudcore-url:50051" // 기본값 설정
	}

	// CloudCore gRPC 서버 연결
	conn, err := grpc.Dial(cloudCoreURL, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to CloudCore: %v", err)
	}
	defer conn.Close()

	client := pb.NewSensorServiceClient(conn)

	// 예제 SensorData 생성
	data := &pb.SensorData{
		Device:    "Device_001",
		Timestamp: time.Now().Format(time.RFC3339),
		ProVer:    1,
		MinorVer:  0,
		SN:        12345678,
		Model:     "Model_XYZ",
		Status: &pb.StatusData{
			Tyield:         100.5,
			Dyield:         20.3,
			PF:             0.98,
			Pmax:           5000.0,
			Pac:            4500.0,
			Sac:            4800.0,
			Uab:            230.0,
			Ubc:            230.0,
			Uca:            230.0,
			Ia:             15.0,
			Ib:             14.5,
			Ic:             15.2,
			Freq:           50.0,
			Tmod:           45.0,
			Tamb:           25.0,
			Mode:           "Normal",
			Qac:            500,
			BusCapacitance: 200.0,
			AcCapacitance:  150.0,
			Pdc:            4500.0,
			PmaxLim:        5000.0,
			SmaxLim:        4800.0,
		},
	}

	// 데이터 전송
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := client.SendSensorData(ctx, data)
	if err != nil {
		log.Fatalf("Failed to send data: %v", err)
	}

	log.Printf("Response from CloudCore: %s", res.Status)
}
