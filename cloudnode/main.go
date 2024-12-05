package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
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

func getConfigInt(key string, defaultValue int) int {
	if os.Getenv("PROFILE") == "prod" {
		value := os.Getenv(key)
		if value == "" {
			return defaultValue
		}
		intValue, err := strconv.Atoi(value)
		if err != nil {
			log.Printf("설정값 변환 오류 (%s): %v, 기본값 사용", key, err)
			return defaultValue
		}
		return intValue
	}

	if config.IsSet(key) {
		return config.GetInt(key)
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

type MQTTConfig struct {
	Broker   string
	Topic    string
	ClientID string
	Username string
	Password string
	QoS      byte
}

func getMQTTConfig() MQTTConfig {
	if os.Getenv("PROFILE") == "prod" {
		return MQTTConfig{
			Broker:   getConfigString("MQTT_BROKER", "localhost:1883"),
			Topic:    getConfigString("MQTT_TOPIC", "sensor/outbound"),
			ClientID: getConfigString("MQTT_CLIENT_ID", "cloud-subscriber"),
			Username: getConfigString("MQTT_USERNAME", ""),
			Password: getConfigString("MQTT_PASSWORD", ""),
			QoS:      byte(getConfigInt("MQTT_QOS", 1)),
		}
	}

	return MQTTConfig{
		Broker:   config.GetString("mqtt.broker"),
		Topic:    config.GetString("mqtt.topic"),
		ClientID: config.GetString("mqtt.client_id"),
		Username: config.GetString("mqtt.username"),
		Password: config.GetString("mqtt.password"),
		QoS:      byte(config.GetInt("mqtt.qos")),
	}
}

// UTC 타임스탬프를 밀리초로 변환하는 함수
func parseTimestampToMillis(timestamp string) (int64, error) {
	// 타임스탬프 파싱 (예상 형식: "2006-01-02T15:04:05Z" 또는 "2006-01-02T15:04:05.000Z")
	t, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		// RFC3339 형식이 아닌 경우 다른 형식 시도
		t, err = time.Parse("2006-01-02 15:04:05", timestamp)
		if err != nil {
			return 0, fmt.Errorf("타임스탬프 파싱 실패: %v", err)
		}
	}

	// UTC로 변환하고 Unix 밀리초 반환
	return t.UTC().UnixNano() / int64(time.Millisecond), nil
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

	// 타임스탬프를 UTC 밀리초로 변환
	timestampMillis, err := parseTimestampToMillis(req.Timestamp)
	if err != nil {
		log.Printf("[gRPC] 타임스탬프 변환 실패 - Device: %s, Error: %v", req.Device, err)
		return nil, fmt.Errorf("타임스탬프 변환 오류: %v", err)
	}

	log.Printf("[gRPC] 데이터 내용 - Device: %s, Timestamp: %s (%d ms), Model: %s, Tyield: %.2f, Mode: %s",
		req.Device, req.Timestamp, timestampMillis, req.Model, req.Status.Tyield, req.Status.Mode)

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

	_, err = s.db.Exec(query,
		req.Device, timestampMillis, req.ProVer, req.MinorVer, req.SN, req.Model,
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

func startMQTTSubscriber(db *sql.DB) error {
	mqttConfig := getMQTTConfig()

	// MQTT 설정 로그 출력
	log.Printf("[MQTT] 설정 정보:")
	log.Printf("  - Broker: %s", mqttConfig.Broker)
	log.Printf("  - Topic: %s", mqttConfig.Topic)
	log.Printf("  - Client ID: %s", mqttConfig.ClientID)
	log.Printf("  - QoS: %d", mqttConfig.QoS)
	log.Printf("  - Username: %s", func() string {
		if mqttConfig.Username != "" {
			return mqttConfig.Username
		}
		return "설정되지 않음"
	}())

	opts := mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://%s", mqttConfig.Broker)).
		SetClientID(mqttConfig.ClientID).
		SetCleanSession(true).
		SetOrderMatters(false).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetMaxReconnectInterval(10 * time.Second)

	// 연결 시도 로그
	log.Printf("[MQTT] 브로커 연결 시도 중... (Broker: %s)", mqttConfig.Broker)

	if mqttConfig.Username != "" {
		opts.SetUsername(mqttConfig.Username)
		opts.SetPassword(mqttConfig.Password)
		log.Printf("[MQTT] 인증 정보 설정됨 (Username: %s)", mqttConfig.Username)
	}

	// 연결 상태 콜백
	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		log.Printf("[MQTT] 연결 끊김 - Error: %v", err)
		log.Printf("[MQTT] 재연결 시도 중...")
	})

	opts.SetOnConnectHandler(func(client mqtt.Client) {
		log.Printf("[MQTT] 브로커 연결 성공 (Broker: %s)", mqttConfig.Broker)
		log.Printf("[MQTT] 토픽 구독 시도 중... (Topic: %s, QoS: %d)", mqttConfig.Topic, mqttConfig.QoS)

		if token := client.Subscribe(mqttConfig.Topic, mqttConfig.QoS, func(client mqtt.Client, msg mqtt.Message) {
			handleMQTTMessage(db, msg)
		}); token.Wait() && token.Error() != nil {
			log.Printf("[MQTT] 구독 실패 - Topic: %s, Error: %v", mqttConfig.Topic, token.Error())
		} else {
			log.Printf("[MQTT] 구독 성공 - Topic: %s", mqttConfig.Topic)
		}
	})

	// 재연결 핸들러 추가
	opts.SetReconnectingHandler(func(client mqtt.Client, opts *mqtt.ClientOptions) {
		log.Printf("[MQTT] 재연결 시도 중... (Broker: %s)", opts.Servers[0])
	})

	// MQTT 클라이언트 생성 및 연결
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("[MQTT] 연결 실패: %v", token.Error())
	}

	log.Printf("[MQTT] 초기화 완료")
	log.Printf("[MQTT] 메시지 수신 대기 중...")
	return nil
}

func handleMQTTMessage(db *sql.DB, msg mqtt.Message) {
	start := time.Now()
	log.Printf("[MQTT] 새로운 메시지 수신 - Topic: %s, QoS: %d, Size: %d bytes",
		msg.Topic(), msg.Qos(), len(msg.Payload()))

	var sensorData pb.SensorData
	if err := json.Unmarshal(msg.Payload(), &sensorData); err != nil {
		log.Printf("[MQTT] JSON 파싱 오류: %v", err)
		log.Printf("[MQTT] 원본 메시지: %s", string(msg.Payload()))
		return
	}

	// 타임스탬프를 UTC 밀리초로 변환
	timestampMillis, err := parseTimestampToMillis(sensorData.Timestamp)
	if err != nil {
		log.Printf("[MQTT] 타임스탬프 변환 실패 - Device: %s, Error: %v", sensorData.Device, err)
		return
	}

	log.Printf("[MQTT] 데이터 파싱 완료:")
	log.Printf("  - Device: %s", sensorData.Device)
	log.Printf("  - Timestamp: %s (%d ms)", sensorData.Timestamp, timestampMillis)
	log.Printf("  - Model: %s", sensorData.Model)
	log.Printf("  - Status:")
	log.Printf("    * Tyield: %.2f", sensorData.Status.Tyield)
	log.Printf("    * Mode: %s", sensorData.Status.Mode)
	log.Printf("    * PF: %.2f", sensorData.Status.PF)
	log.Printf("    * Pmax: %.2f", sensorData.Status.Pmax)

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

	_, err = db.Exec(query,
		sensorData.Device, timestampMillis, sensorData.ProVer, sensorData.MinorVer, sensorData.SN, sensorData.Model,
		sensorData.Status.Tyield, sensorData.Status.Dyield, sensorData.Status.PF, sensorData.Status.Pmax, sensorData.Status.Pac, sensorData.Status.Sac,
		sensorData.Status.Uab, sensorData.Status.Ubc, sensorData.Status.Uca, sensorData.Status.Ia, sensorData.Status.Ib, sensorData.Status.Ic,
		sensorData.Status.Freq, sensorData.Status.Tmod, sensorData.Status.Tamb, sensorData.Status.Mode, sensorData.Status.Qac,
		sensorData.Status.BusCapacitance, sensorData.Status.AcCapacitance, sensorData.Status.Pdc, sensorData.Status.PmaxLim, sensorData.Status.SmaxLim,
		false,
	)

	if err != nil {
		log.Printf("[MQTT] DB 저장 실패 - Device: %s, Error: %v", sensorData.Device, err)
		return
	}

	duration := time.Since(start)
	log.Printf("[MQTT] 데이터 처리 완료 - Device: %s (처리 시간: %v)", sensorData.Device, duration)
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

	// 전송 모드 확인
	transportMode := getConfigString("TRANSPORT_MODE", "grpc")
	log.Printf("[Server] 전송 모드: %s", transportMode)

	switch transportMode {
	case "mqtt":
		// MQTT 구독자 시작
		if err := startMQTTSubscriber(db); err != nil {
			log.Fatalf("[MQTT] 구독자 시작 실패: %v", err)
		}
		// 프로그램이 종료되지 않도록 대기
		select {}

	case "grpc":
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
			Time:                  5 * time.Second,
			Timeout:               1 * time.Second,
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

	default:
		log.Fatalf("[Server] 지원하지 않는 전송 모드: %s", transportMode)
	}
}
