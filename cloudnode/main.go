package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/spf13/viper"
	pb "github.com/suslmk-lee/zim-grpc-mapper/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
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
	if p, ok := peer.FromContext(ctx); ok {
		log.Printf("[gRPC] 새로운 데이터 수신 시작 - Device: %s, Remote Address: %v",
			req.Device, p.Addr)
	} else {
		log.Printf("[gRPC] 새로운 데이터 수신 시작 - Device: %s", req.Device)
	}

	if ctx.Err() != nil {
		log.Printf("[gRPC] 컨텍스트 오류 발생 - Device: %s, Error: %v", req.Device, ctx.Err())
		return nil, ctx.Err()
	}

	log.Printf("[gRPC] 데이터 내용 - Device: %s, Timestamp: %s, Model: %s, Tyield: %.2f, Mode: %s",
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
		log.Printf("[gRPC] DB 저장 실패 - Device: %s, Error: %v", req.Device, err)
		return nil, fmt.Errorf("데이터베이스 저장 오류: %v", err)
	}

	log.Printf("[gRPC] 데이터 처리 완료 - Device: %s", req.Device)
	return &pb.SensorResponse{Status: "success"}, nil
}

func main() {
	log.Printf("[Server] 서버 시작 중...")

	// 설정 초기화
	if err := initConfig(); err != nil {
		log.Fatalf("[Server] 설정 초기화 오류: %v", err)
	}
	log.Printf("[Server] 설정 초기화 완료")

	// DB 연결
	log.Printf("[Server] PostgreSQL 연결 시도 중...")
	db, err := sql.Open("postgres", getDBConnStr())
	if err != nil {
		log.Fatalf("[Server] DB 연결 실패: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("[Server] DB Ping 실패: %v", err)
	}
	log.Printf("[Server] PostgreSQL 연결 성공")

	// gRPC 서버 설정
	serverAddress := getServerAddress()
	log.Printf("[gRPC] 서버 주소 설정: %s", serverAddress)

	lis, err := net.Listen("tcp", serverAddress)
	if err != nil {
		log.Fatalf("[gRPC] 리스닝 실패: %v", err)
	}

	// keepalive 정책 설정
	kasp := keepalive.ServerParameters{
		MaxConnectionIdle:     15 * time.Second,
		MaxConnectionAge:      30 * time.Second,
		MaxConnectionAgeGrace: 5 * time.Second,
		Time:                 5 * time.Second,
		Timeout:             1 * time.Second,
	}

	kaep := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second,
		PermitWithoutStream: true,
	}

	// gRPC 서버 옵션 설정
	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(kasp),
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			start := time.Now()
			
			// 클라이언트 정보 로깅
			if p, ok := peer.FromContext(ctx); ok {
				log.Printf("[gRPC] 새로운 요청 - Method: %s, Peer Address: %v", info.FullMethod, p.Addr)
			} else {
				log.Printf("[gRPC] 새로운 요청 - Method: %s (피어 정보 없음)", info.FullMethod)
			}

			// 요청 처리
			resp, err := handler(ctx, req)
			
			// 처리 결과 로깅
			duration := time.Since(start)
			if err != nil {
				log.Printf("[gRPC] 요청 실패 - Method: %s, Duration: %v, Error: %v", info.FullMethod, duration, err)
			} else {
				log.Printf("[gRPC] 요청 성공 - Method: %s, Duration: %v", info.FullMethod, duration)
			}
			
			return resp, err
		}),
	}

	// gRPC 서버 시작
	s := grpc.NewServer(opts...)
	pb.RegisterSensorServiceServer(s, &server{db: db})
	
	log.Printf("[gRPC] 서버 시작...")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("[gRPC] 서버 실행 실패: %v", err)
	}
}
