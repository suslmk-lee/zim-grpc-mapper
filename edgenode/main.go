package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
	pb "github.com/suslmk-lee/zim-grpc-mapper/pb"
	"google.golang.org/grpc"
)

// getEnv 함수는 환경변수를 가져오며, 환경변수가 설정되지 않은 경우 기본값을 반환합니다.
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// 데이터베이스에서 센서 데이터를 조회하는 함수
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

// 배치로 전송 완료된 데이터 업데이트
func updateSentDataBatch(db *sql.DB, devices []string, timestamps []string) error {
	if len(devices) == 0 || len(timestamps) == 0 {
		return nil
	}

	// 트랜잭션 시작
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 배치 업데이트 쿼리 준비
	stmt, err := tx.Prepare(`
		UPDATE iot_data 
		SET sent_to_cloud = true 
		WHERE device = $1 AND timestamp = $2`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// 배치로 업데이트 실행
	for i := range devices {
		_, err := stmt.Exec(devices[i], timestamps[i])
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func main() {
	// PostgreSQL 연결 정보를 환경변수에서 가져오기
	dbHost := getEnv("DB_HOST", "localhost")
	dbPort := getEnv("DB_PORT", "5432")
	dbUser := getEnv("DB_USER", "user")
	dbPassword := getEnv("DB_PASSWORD", "password")
	dbName := getEnv("DB_NAME", "sensordb")
	dbSSLMode := getEnv("DB_SSLMODE", "disable")

	// PostgreSQL 연결 문자열 생성
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		dbHost, dbPort, dbUser, dbPassword, dbName, dbSSLMode)

	// 데이터베이스 연결
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

	// CloudCore URL 설정
	cloudCoreURL := getEnv("CLOUD_CORE_URL", "cloudcore:50051")

	// CloudCore gRPC 서버 연결
	conn, err := grpc.Dial(cloudCoreURL, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to CloudCore: %v", err)
	}
	defer conn.Close()

	client := pb.NewSensorServiceClient(conn)

	// 작업자 수 설정
	workerCount := 10
	workChan := make(chan *pb.SensorData, 500)
	resultChan := make(chan struct {
		device    string
		timestamp string
		err       error
	}, 500)

	// 작업자 고루틴 시작
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

	// 주기적으로 데이터를 전송 (1초 간격)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// 데이터베이스에서 센서 데이터 조회
		sensorDataList, err := fetchSensorDataFromDB(db)
		if err != nil {
			log.Printf("Failed to fetch sensor data: %v", err)
			continue
		}

		if len(sensorDataList) == 0 {
			continue
		}

		// 성공한 전송 결과를 저장할 슬라이스
		var successDevices []string
		var successTimestamps []string

		// 데이터를 작업 채널에 전송
		for _, data := range sensorDataList {
			workChan <- data
		}

		// 결과 수집
		for i := 0; i < len(sensorDataList); i++ {
			result := <-resultChan
			if result.err != nil {
				log.Printf("Failed to send data for device %s: %v", result.device, result.err)
				continue
			}
			successDevices = append(successDevices, result.device)
			successTimestamps = append(successTimestamps, result.timestamp)
		}

		// 성공한 데이터 배치 업데이트
		if len(successDevices) > 0 {
			if err := updateSentDataBatch(db, successDevices, successTimestamps); err != nil {
				log.Printf("Failed to update sent status batch: %v", err)
			}
		}
	}
}
