package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"

	_ "github.com/lib/pq"
	"github.com/spf13/viper"
	pb "github.com/suslmk-lee/zim-grpc-mapper/pb"
	"google.golang.org/grpc"
)

var config *viper.Viper

func initConfig() error {
	profile := os.Getenv("PROFILE")
	config = viper.New()

	if profile == "prod" {
		// 프로덕션 모드: 환경 변수 사용
		config.SetEnvPrefix("")
		config.AutomaticEnv()
		return nil
	}

	config.SetConfigName("config")
	config.SetConfigType("json")
	config.AddConfigPath(".")

	if err := config.ReadInConfig(); err != nil {
		return fmt.Errorf("설정 파일 읽기 오류: %v", err)
	}

	return nil
}

func getConfigString(key, defaultValue string) string {
	if config.IsSet(key) {
		return config.GetString(key)
	}
	return defaultValue
}

func getDBConnStr() string {
	if os.Getenv("PROFILE") == "prod" {
		return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
			getConfigString("DB_HOST", "localhost"),
			getConfigString("DB_PORT", "5432"),
			getConfigString("DB_USER", "postgres"),
			getConfigString("DB_PASSWORD", ""),
			getConfigString("DB_NAME", "sensordb"),
			getConfigString("DB_SSLMODE", "disable"),
		)
	}

	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		config.GetString("db.host"),
		config.GetString("db.port"),
		config.GetString("db.user"),
		config.GetString("db.password"),
		config.GetString("db.name"),
		config.GetString("db.sslmode"),
	)
}

func getServerAddress() string {
	if os.Getenv("PROFILE") == "prod" {
		host := getConfigString("SERVER_HOST", "0.0.0.0")
		port := getConfigString("SERVER_PORT", "50051")
		return fmt.Sprintf("%s:%s", host, port)
	}

	host := config.GetString("server.host")
	port := config.GetString("server.port")
	return fmt.Sprintf("%s:%s", host, port)
}

type server struct {
	pb.UnimplementedSensorServiceServer
	db *sql.DB // PostgreSQL 연결
}

func (s *server) SendSensorData(ctx context.Context, req *pb.SensorData) (*pb.SensorResponse, error) {
	log.Printf("Received data: Device=%s, Timestamp=%s, Model=%s, Tyield=%.2f, Mode=%s",
		req.Device, req.Timestamp, req.Model, req.Status.Tyield, req.Status.Mode)

	query := `
		INSERT INTO iot_data (
			device, timestamp, pro_ver, minor_ver, sn, model, tyield, dyield, pf, pmax, pac, sac,
			uab, ubc, uca, ia, ib, ic, freq, tmod, tamb, mode, qac,
			bus_capacitance, ac_capacitance, pdc, pmax_lim, smax_lim, is_sent
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
			$13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23,
			$24, $25, $26, $27, $28, $29
		)`

	_, err := s.db.Exec(query,
		req.Device, req.Timestamp, req.ProVer, req.MinorVer, req.SN, req.Model,
		req.Status.Tyield, req.Status.Dyield, req.Status.PF, req.Status.Pmax, req.Status.Pac, req.Status.Sac,
		req.Status.Uab, req.Status.Ubc, req.Status.Uca, req.Status.Ia, req.Status.Ib, req.Status.Ic,
		req.Status.Freq, req.Status.Tmod, req.Status.Tamb, req.Status.Mode, req.Status.Qac,
		req.Status.BusCapacitance, req.Status.AcCapacitance, req.Status.Pdc, req.Status.PmaxLim, req.Status.SmaxLim,
		false, // is_sent 기본값 false
	)

	if err != nil {
		log.Printf("Failed to insert data into PostgreSQL: %v", err)
		return &pb.SensorResponse{Status: "Failed to save data"}, err
	}

	return &pb.SensorResponse{Status: "Data received and saved successfully"}, nil
}

func main() {
	// 설정 초기화
	if err := initConfig(); err != nil {
		log.Fatalf("설정 초기화 오류: %v", err)
	}

	db, err := sql.Open("postgres", getDBConnStr())
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	log.Println("Connected to PostgreSQL")

	// gRPC 서버 시작
	lis, err := net.Listen("tcp", getServerAddress())
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterSensorServiceServer(s, &server{db: db})
	log.Printf("Server listening at %v", lis.Addr())

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
