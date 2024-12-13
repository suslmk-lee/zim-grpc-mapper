package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/lib/pq"
	"github.com/spf13/viper"
	pb "github.com/suslmk-lee/zim-grpc-mapper/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/connectivity"
)

const (
	TransportGRPC = "grpc"
	TransportMQTT = "mqtt"
)

var (
	dataSource    DataSource
	mqttClient    MQTT.Client // 데이터 수신용 MQTT 클라이언트
	mqttPubClient MQTT.Client // 데이터 전송용 MQTT 클라이언트
	dataChannel   = make(chan *pb.SensorData, 100)
	mu            sync.Mutex
	config        *viper.Viper
)

// 기존 MQTT 설정 (수신용)
type MQTTConfig struct {
	Broker   string
	Topic    string
	ClientID string
	Username string
	Password string
}

// 새로운 MQTT 설정 (전송용)
type MQTTPubConfig struct {
	Broker   string
	Topic    string
	ClientID string
	Username string
	Password string
	QoS      byte
}

func initConfig() error {
	profile := os.Getenv("PROFILE")
	config = viper.New()

	if profile == "prod" {
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
	if os.Getenv("PROFILE") == "prod" {
		value := os.Getenv(key)
		if value == "" {
			return defaultValue
		}
		return value
	}

	if config.IsSet(key) {
		return config.GetString(key)
	}
	return defaultValue
}

// 정수형 설정값을 가져오는 함수 추가
func getConfigInt(key string, defaultValue int) int {
	if os.Getenv("PROFILE") == "prod" {
		value := os.Getenv(key)
		if value == "" {
			return defaultValue
		}
		// 문자열을 정수로 변환
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

func getMQTTConfig() MQTTConfig {
	if os.Getenv("PROFILE") == "prod" {
		return MQTTConfig{
			Broker:   getConfigString("MQTT_BROKER", "localhost:1883"),
			Topic:    getConfigString("MQTT_TOPIC", "iot/data"),
			ClientID: getConfigString("MQTT_CLIENT_ID", "edge-node"),
			Username: getConfigString("MQTT_USERNAME", ""),
			Password: getConfigString("MQTT_PASSWORD", ""),
		}
	}

	return MQTTConfig{
		Broker:   config.GetString("mqtt.broker"),
		Topic:    config.GetString("mqtt.topic"),
		ClientID: config.GetString("mqtt.client_id"),
		Username: config.GetString("mqtt.username"),
		Password: config.GetString("mqtt.password"),
	}
}

func getDataSource() DataSource {
	var source string
	if os.Getenv("PROFILE") == "prod" {
		source = getConfigString("DATA_SOURCE", "db")
	} else {
		source = config.GetString("data_source")
	}

	if source == "mqtt" {
		return DataSourceMQTT
	}
	return DataSourceDB
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

type DataSource int

const (
	DataSourceDB DataSource = iota
	DataSourceMQTT
)

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func initMQTTClient(config MQTTConfig) error {
	opts := MQTT.NewClientOptions()
	opts.AddBroker(config.Broker)
	opts.SetClientID(config.ClientID)
	if config.Username != "" {
		opts.SetUsername(config.Username)
		opts.SetPassword(config.Password)
	}

	opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		var sensorData pb.SensorData
		if err := json.Unmarshal(msg.Payload(), &sensorData); err != nil {
			log.Printf("Error unmarshaling MQTT message: %v", err)
			return
		}
		dataChannel <- &sensorData
	})

	client := MQTT.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("MQTT connection failed: %v", token.Error())
	}

	if token := client.Subscribe(config.Topic, 0, nil); token.Wait() && token.Error() != nil {
		return fmt.Errorf("MQTT subscription failed: %v", token.Error())
	}

	mqttClient = client
	return nil
}

func fetchSensorDataFromDB(db *sql.DB) ([]*pb.SensorData, error) {
	query := `
		SELECT device, timestamp, prover, minorver, sn, model,
			   tyield, dyield, pf, pmax, pac, sac,
			   uab, ubc, uca, ia, ib, ic, freq, tmod, tamb, mode, qac,
			   bus_capacitance, ac_capacitance, pdc, pmaxlim, smaxlim
		FROM iot_data
		WHERE sent_to_cloud = false
		LIMIT 500` // 한 번에 처리할 데이터 수 증가

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %v", err)
	}
	defer rows.Close()

	var sensorDataList []*pb.SensorData
	for rows.Next() {
		var data pb.SensorData
		data.Status = &pb.StatusData{}

		err := rows.Scan(
			&data.Device, &data.Timestamp, &data.ProVer, &data.MinorVer,
			&data.SN, &data.Model,
			&data.Status.Tyield, &data.Status.Dyield, &data.Status.PF,
			&data.Status.Pmax, &data.Status.Pac, &data.Status.Sac,
			&data.Status.Uab, &data.Status.Ubc, &data.Status.Uca,
			&data.Status.Ia, &data.Status.Ib, &data.Status.Ic,
			&data.Status.Freq, &data.Status.Tmod, &data.Status.Tamb,
			&data.Status.Mode, &data.Status.Qac,
			&data.Status.BusCapacitance, &data.Status.AcCapacitance,
			&data.Status.Pdc, &data.Status.PmaxLim, &data.Status.SmaxLim,
		)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %v", err)
		}
		sensorDataList = append(sensorDataList, &data)
	}

	return sensorDataList, nil
}

func updateSentDataBatch(db *sql.DB, devices []string, timestamps []string) error {
	if len(devices) == 0 || len(timestamps) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		UPDATE iot_data 
		SET sent_to_cloud = true 
		WHERE device = $1 AND timestamp = $2`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i := range devices {
		_, err := stmt.Exec(devices[i], timestamps[i])
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

var fileWriteMutexes sync.Map

// 파일 쓰기를 위한 버퍼 관리
type FileBuffer struct {
	records []map[string]interface{}
	mutex   sync.Mutex
	lastFlush time.Time
}

var (
	fileBuffers = make(map[string]*FileBuffer)
	bufferMutex sync.Mutex
)

func getOrCreateBuffer(filePath string) *FileBuffer {
	bufferMutex.Lock()
	defer bufferMutex.Unlock()
	
	if buffer, exists := fileBuffers[filePath]; exists {
		return buffer
	}
	
	buffer := &FileBuffer{
		records: make([]map[string]interface{}, 0),
		lastFlush: time.Now(),
	}
	fileBuffers[filePath] = buffer
	return buffer
}

func writeDataToFile(data *pb.SensorData) error {
	timestamp, err := time.Parse("2006-01-02 15:04:05", data.Timestamp)
	if err != nil {
		return fmt.Errorf("timestamp 파싱 오류: %v", err)
	}

	basePath := getDataPath()
	dirPath := fmt.Sprintf("%s/%s/%s", basePath, timestamp.Format("2006-01-02"), timestamp.Format("15"))
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("디렉토리 생성 오류: %v", err)
	}

	filePath := fmt.Sprintf("%s/data_%s.json", dirPath, timestamp.Format("15"))
	buffer := getOrCreateBuffer(filePath)
	
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()

	var newData map[string]interface{}
	dataJSON, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("데이터 마샬링 오류: %v", err)
	}
	if err := json.Unmarshal(dataJSON, &newData); err != nil {
		return fmt.Errorf("데이터 언마샬링 오류: %v", err)
	}

	buffer.records = append(buffer.records, newData)
	
	// 버퍼가 100개 이상 쌓이거나 마지막 플러시로부터 10초가 지났으면 파일에 쓰기
	if len(buffer.records) >= 100 || time.Since(buffer.lastFlush) > 10*time.Second {
		if err := flushBufferToFile(filePath, buffer); err != nil {
			return fmt.Errorf("버퍼 플러시 오류: %v", err)
		}
		buffer.lastFlush = time.Now()
	}

	log.Printf("데이터가 버퍼에 추가됨: %s (현재 버퍼 크기: %d)", filePath, len(buffer.records))
	return nil
}

func flushBufferToFile(filePath string, buffer *FileBuffer) error {
	var existingRecords []map[string]interface{}
	
	if _, err := os.Stat(filePath); err == nil {
		fileData, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("파일 읽기 오류: %v", err)
		}
		if len(fileData) > 0 {
			if err := json.Unmarshal(fileData, &existingRecords); err != nil {
				return fmt.Errorf("JSON 파싱 오류: %v", err)
			}
		}
	}

	allRecords := append(existingRecords, buffer.records...)
	jsonData, err := json.MarshalIndent(allRecords, "", "  ")
	if err != nil {
		return fmt.Errorf("JSON 변환 오류: %v", err)
	}

	if err := os.WriteFile(filePath, jsonData, 0644); err != nil {
		return fmt.Errorf("파일 쓰기 오류: %v", err)
	}

	buffer.records = make([]map[string]interface{}, 0)
	log.Printf("버퍼가 파일에 플러시됨: %s (총 %d건)", filePath, len(allRecords))
	return nil
}

func getDataPath() string {
	if os.Getenv("PROFILE") == "prod" {
		return "/app/data"
	}
	return "data"
}

func getTransportMode() string {
	mode := getConfigString("TRANSPORT_MODE", TransportGRPC)
	if mode != TransportGRPC && mode != TransportMQTT {
		log.Printf("[Config] 잘못된 전송 모드 설정: %s, 기본값(gRPC)으로 설정됨", mode)
		return TransportGRPC
	}
	return mode
}

func getMQTTPubConfig() MQTTPubConfig {
	if os.Getenv("PROFILE") == "prod" {
		return MQTTPubConfig{
			Broker:   getConfigString("PUB_MQTT_BROKER", "localhost:1883"),
			Topic:    getConfigString("PUB_MQTT_TOPIC", "sensor/outbound"),
			ClientID: getConfigString("PUB_MQTT_CLIENT_ID", "edge-publisher"),
			Username: getConfigString("PUB_MQTT_USERNAME", ""),
			Password: getConfigString("PUB_MQTT_PASSWORD", ""),
			QoS:      byte(getConfigInt("PUB_MQTT_QOS", 1)),
		}
	}

	return MQTTPubConfig{
		Broker:   config.GetString("mqtt_pub.broker"),
		Topic:    config.GetString("mqtt_pub.topic"),
		ClientID: config.GetString("mqtt_pub.client_id"),
		Username: config.GetString("mqtt_pub.username"),
		Password: config.GetString("mqtt_pub.password"),
		QoS:      byte(config.GetInt("mqtt_pub.qos")),
	}
}

func startMQTTPublisher() error {
	mqttConfig := getMQTTPubConfig()

	// MQTT Publisher 설정 로그 출력
	log.Printf("[MQTT Publisher] 설정 정보:")
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

	opts := MQTT.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://%s", mqttConfig.Broker)).
		SetClientID(mqttConfig.ClientID).
		SetCleanSession(true).
		SetOrderMatters(false).
		SetAutoReconnect(true).
		SetMaxReconnectInterval(10 * time.Second)

	log.Printf("[MQTT Publisher] 브로커 연결 시도 중... (Broker: %s)", mqttConfig.Broker)

	if mqttConfig.Username != "" {
		opts.SetUsername(mqttConfig.Username)
		opts.SetPassword(mqttConfig.Password)
		log.Printf("[MQTT Publisher] 인증 정보 설정됨 (Username: %s)", mqttConfig.Username)
	}

	opts.SetConnectionLostHandler(func(client MQTT.Client, err error) {
		log.Printf("[MQTT Publisher] 연결 끊김 - Error: %v", err)
		log.Printf("[MQTT Publisher] 재연결 시도 중...")
	})

	opts.SetOnConnectHandler(func(client MQTT.Client) {
		log.Printf("[MQTT Publisher] 브로커 연결 성공 (Broker: %s)", mqttConfig.Broker)
	})

	// 재연결 핸들러 추가
	opts.SetReconnectingHandler(func(client MQTT.Client, opts *MQTT.ClientOptions) {
		log.Printf("[MQTT Publisher] 재연결 시도 중... (Broker: %s)", opts.Servers[0])
	})

	mqttPubClient = MQTT.NewClient(opts)
	if token := mqttPubClient.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("[MQTT Publisher] 연결 실패: %v", token.Error())
	}

	log.Printf("[MQTT Publisher] 초기화 완료")
	return nil
}

func publishToMQTT(data *pb.SensorData) error {
	if mqttPubClient == nil || !mqttPubClient.IsConnected() {
		return fmt.Errorf("[MQTT Publisher] 클라이언트가 연결되지 않음")
	}

	mqttConfig := getMQTTPubConfig()
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("[MQTT Publisher] JSON 변환 실패: %v", err)
	}

	start := time.Now()
	token := mqttPubClient.Publish(mqttConfig.Topic, mqttConfig.QoS, false, jsonData)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("[MQTT Publisher] 메시지 발행 실패: %v", token.Error())
	}

	duration := time.Since(start)
	log.Printf("[MQTT Publisher] 메시지 발행 완료 - Topic: %s, Size: %d bytes, Duration: %v",
		mqttConfig.Topic, len(jsonData), duration)
	return nil
}

func processMQTTData(client pb.SensorServiceClient) {
	transportMode := getTransportMode()
	log.Printf("[Transport] 전송 모드: %s", transportMode)

	workerCount := 10
	workChan := make(chan *pb.SensorData, 1000) // 버퍼 크기 증가
	resultChan := make(chan struct {
		device    string
		timestamp string
		err       error
	}, 1000)

	// 워커 종료를 위한 컨텍스트 추가
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			log.Printf("Worker %d 시작", workerID)
			for {
				select {
				case data, ok := <-workChan:
					if !ok {
						log.Printf("Worker %d: 채널이 닫혔습니다. 종료합니다.", workerID)
						return
					}
					log.Printf("Worker %d: 데이터 처리 시작 (디바이스: %s, 타임스탬프: %s)",
						workerID, data.Device, data.Timestamp)

					// 파일 쓰기 시도 (타임아웃 추가)
					fileErr := make(chan error, 1)
					done := make(chan bool)
					go func(d *pb.SensorData) {
						defer close(done)
						if err := writeDataToFile(d); err != nil {
							fileErr <- err
						} else {
							fileErr <- nil
						}
					}(data)

					// 파일 쓰기 타임아웃 처리
					select {
					case <-time.After(10 * time.Second):
						log.Printf("Worker %d: 파일 쓰기 타임아웃 (디바이스: %s)",
							workerID, data.Device)
						fileErr <- fmt.Errorf("file write timeout")
					case <-done:
						// 파일 쓰기 완료
					}

					var err error
					if transportMode == TransportMQTT {
						err = publishToMQTT(data)
					} else {
						// gRPC 타임아웃 증가
						gctx, gcancel := context.WithTimeout(ctx, 30*time.Second)
						_, err = client.SendSensorData(gctx, data)
						gcancel()
					}

					// 파일 저장 결과 확인
					if ferr := <-fileErr; ferr != nil {
						log.Printf("Worker %d: 파일 저장 실패 (디바이스: %s): %v",
							workerID, data.Device, ferr)
					}

					resultChan <- struct {
						device    string
						timestamp string
						err       error
					}{
						device:    data.Device,
						timestamp: data.Timestamp,
						err:       err,
					}
				case <-ctx.Done():
					log.Printf("Worker %d: 컨텍스트 취소로 종료합니다.", workerID)
					return
				}
			}
		}(i)
	}

	// 메인 프로세스 모니터링
	log.Printf("메인 프로세스: 데이터 채널 모니터링 시작")
	healthCheckTicker := time.NewTicker(1 * time.Minute)
	defer healthCheckTicker.Stop()

	lastDataTime := time.Now()
	for {
		select {
		case data, ok := <-dataChannel:
			if !ok {
				log.Printf("메인 프로세스: 데이터 채널이 닫혔습니다.")
				close(workChan)
				wg.Wait()
				return
			}
			lastDataTime = time.Now()
			log.Printf("메인 프로세스: 새 데이터 수신 (디바이스: %s)", data.Device)
			select {
			case workChan <- data:
			case <-time.After(5 * time.Second):
				log.Printf("메인 프로세스: 작업 채널이 가득 찼습니다. 데이터 드롭: %s", data.Device)
			}
		case <-healthCheckTicker.C:
			if time.Since(lastDataTime) > 5*time.Minute {
				log.Printf("경고: 마지막 데이터 수신 후 5분이 경과했습니다. 마지막 수신: %v", lastDataTime)
			}
		}
	}
}

func processDBData(db *sql.DB, client pb.SensorServiceClient) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		sensorDataList, err := fetchSensorDataFromDB(db)
		if err != nil {
			log.Printf("Failed to fetch sensor data: %v", err)
			continue
		}

		if len(sensorDataList) == 0 {
			continue
		}

		var successDevices []string
		var successTimestamps []string

		workerCount := 10
		workChan := make(chan *pb.SensorData, 500)
		resultChan := make(chan struct {
			device    string
			timestamp string
			err       error
		}, 500)

		for i := 0; i < workerCount; i++ {
			go func() {
				for data := range workChan {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					_, err := client.SendSensorData(ctx, data)
					resultChan <- struct {
						device    string
						timestamp string
						err       error
					}{
						device:    data.Device,
						timestamp: data.Timestamp,
						err:       err,
					}
					cancel()
				}
			}()
		}

		for _, data := range sensorDataList {
			workChan <- data
		}

		for i := 0; i < len(sensorDataList); i++ {
			result := <-resultChan
			if result.err != nil {
				log.Printf("Failed to send data for device %s: %v", result.device, result.err)
				continue
			}
			successDevices = append(successDevices, result.device)
			successTimestamps = append(successTimestamps, result.timestamp)
		}

		if len(successDevices) > 0 {
			if err := updateSentDataBatch(db, successDevices, successTimestamps); err != nil {
				log.Printf("Failed to update sent status batch: %v", err)
			}
		}
	}
}

func main() {
	if err := initConfig(); err != nil {
		log.Fatalf("설정 초기화 오류: %v", err)
	}

	transportMode := getTransportMode()
	log.Printf("전송 모드: %s", transportMode)

	// 버퍼 플러시를 위한 타이머 시작
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		
		for range ticker.C {
			bufferMutex.Lock()
			for filePath, buffer := range fileBuffers {
				buffer.mutex.Lock()
				if len(buffer.records) > 0 {
					if err := flushBufferToFile(filePath, buffer); err != nil {
						log.Printf("주기적 버퍼 플러시 오류: %v", err)
					}
				}
				buffer.mutex.Unlock()
			}
			bufferMutex.Unlock()
		}
	}()

	// gRPC 연결 관리를 위한 함수
	var client pb.SensorServiceClient
	var conn *grpc.ClientConn
	
	setupGRPCConnection := func() error {
		if conn != nil {
			conn.Close()
		}

		var cloudCoreURL string
		if os.Getenv("PROFILE") == "prod" {
			cloudCoreURL = getConfigString("CLOUD_CORE_URL", "cloudcore:50051")
		} else {
			cloudCoreURL = config.GetString("cloud_core_url")
		}

		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
			grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		}

		log.Printf("Cloud Core 연결 시도 중... (%s)", cloudCoreURL)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		var err error
		conn, err = grpc.DialContext(ctx, cloudCoreURL, opts...)
		if err != nil {
			return fmt.Errorf("Cloud Core 연결 실패: %v", err)
		}

		client = pb.NewSensorServiceClient(conn)
		return nil
	}

	// gRPC 연결 상태 모니터링 및 재연결
	if transportMode == TransportGRPC {
		if err := setupGRPCConnection(); err != nil {
			log.Fatalf("초기 gRPC 연결 실패: %v", err)
		}

		go func() {
			ticker := time.NewTicker(1 * time.Minute)
			defer ticker.Stop()

			for range ticker.C {
				if conn != nil && conn.GetState() == connectivity.TransientFailure {
					log.Println("gRPC 연결 상태 불안정, 재연결 시도...")
					if err := setupGRPCConnection(); err != nil {
						log.Printf("gRPC 재연결 실패: %v", err)
					} else {
						log.Println("gRPC 재연결 성공")
					}
				}
			}
		}()
	}

	// 전송용 MQTT 클라이언트 초기화 (MQTT 전송 모드일 때만)
	if transportMode == TransportMQTT {
		if err := startMQTTPublisher(); err != nil {
			log.Fatalf("MQTT Publisher 초기화 실패: %v", err)
		}
		defer mqttPubClient.Disconnect(250)
	}

	// 데이터 수신용 MQTT 설정 및 연결
	dataSource = getDataSource()
	log.Printf("Operating in %s mode", map[DataSource]string{
		DataSourceDB:   "Database",
		DataSourceMQTT: "MQTT",
	}[dataSource])

	switch dataSource {
	case DataSourceMQTT:
		mqttConfig := getMQTTConfig()
		fmt.Println("Broker: ", mqttConfig.Broker, " Topic: ", mqttConfig.Topic, " ClientID: ", mqttConfig.ClientID)
		if err := initMQTTClient(mqttConfig); err != nil {
			log.Fatalf("Failed to initialize MQTT client: %v", err)
		}
		defer mqttClient.Disconnect(250)
		processMQTTData(client)

	case DataSourceDB:
		db, err := sql.Open("postgres", getDBConnStr())
		if err != nil {
			log.Fatalf("Failed to connect to PostgreSQL: %v", err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			log.Fatalf("Failed to ping PostgreSQL: %v", err)
		}
		log.Println("Connected to PostgreSQL")

		processDBData(db, client)
	}
}
