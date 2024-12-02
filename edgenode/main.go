package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/lib/pq"
	"github.com/spf13/viper"
	pb "github.com/suslmk-lee/zim-grpc-mapper/pb"
	"google.golang.org/grpc"
)

var (
	dataSource  DataSource
	mqttClient  MQTT.Client
	dataChannel = make(chan *pb.SensorData, 100)
	mu          sync.Mutex
	config      *viper.Viper
)

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
	if config.IsSet(key) {
		return config.GetString(key)
	}
	return defaultValue
}

func getMQTTConfig() MQTTConfig {
	if os.Getenv("PROFILE") == "prod" {
		return MQTTConfig{
			Broker:   getConfigString("MQTT_BROKER", "localhost:1883"),
			Topic:    getConfigString("MQTT_TOPIC", "sensors/data"),
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

type MQTTConfig struct {
	Broker   string
	Topic    string
	ClientID string
	Username string
	Password string
}

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

func writeDataToFile(data *pb.SensorData) error {
	baseDir := "/app/data"
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return fmt.Errorf("failed to create base directory: %v", err)
	}

	deviceDir := fmt.Sprintf("%s/%s", baseDir, data.Device)
	if err := os.MkdirAll(deviceDir, 0755); err != nil {
		return fmt.Errorf("failed to create device directory: %v", err)
	}

	filename := fmt.Sprintf("%s/%d.json", deviceDir, time.Now().UnixNano())
	
	// Get or create mutex for this device
	var mutex sync.Mutex
	actualMutex, _ := fileWriteMutexes.LoadOrStore(data.Device, &mutex)
	deviceMutex := actualMutex.(*sync.Mutex)
	
	deviceMutex.Lock()
	defer deviceMutex.Unlock()

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(data); err != nil {
		return fmt.Errorf("failed to write data: %v", err)
	}

	return nil
}

func processMQTTData(client pb.SensorServiceClient) {
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

				go func(d *pb.SensorData) {
					if err := writeDataToFile(d); err != nil {
						log.Printf("파일 저장 오류 (디바이스: %s): %v", d.Device, err)
					}
				}(data)
			}
		}()
	}

	for data := range dataChannel {
		workChan <- data
	}

	for {
		result := <-resultChan
		if result.err != nil {
			log.Printf("데이터 전송 실패 (디바이스: %s): %v", result.device, result.err)
			continue
		}
		log.Printf("데이터 전송 성공 (디바이스: %s)", result.device)
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

	dataSource = getDataSource()
	log.Printf("Operating in %s mode", map[DataSource]string{
		DataSourceDB:   "Database",
		DataSourceMQTT: "MQTT",
	}[dataSource])

	var cloudCoreURL string
	if os.Getenv("PROFILE") == "prod" {
		cloudCoreURL = getConfigString("CLOUD_CORE_URL", "cloudcore:50051")
	} else {
		cloudCoreURL = config.GetString("cloud_core_url")
	}

	conn, err := grpc.Dial(cloudCoreURL, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to CloudCore: %v", err)
	}
	defer conn.Close()

	client := pb.NewSensorServiceClient(conn)
	log.Printf("Connected to CloudCore at %s", cloudCoreURL)

	switch dataSource {
	case DataSourceMQTT:
		mqttConfig := getMQTTConfig()
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
