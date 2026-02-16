package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
)

type Machine struct {
	ID   uuid.UUID `json:"id"`
	Name string    `json:"name"`
}

type Simulator struct {
	apiURL        string
	machineCount  int
	metricsPerSec int
	machines      []Machine
}

func main() {
	apiURL := getEnv("API_URL", "http://localhost:8083")
	machineCount := getEnvInt("MACHINE_COUNT", 10)
	metricsPerSec := getEnvInt("METRICS_PER_SECOND", 100)

	sim := &Simulator{
		apiURL:        apiURL,
		machineCount:  machineCount,
		metricsPerSec: metricsPerSec,
	}

	if err := sim.registerMachines(); err != nil {
		log.Fatalf("Failed to register machines: %v", err)
	}

	log.Printf("Starting simulator with %d machines, %d metrics/sec", sim.machineCount, sim.metricsPerSec)
	sim.run()
}

func (s *Simulator) registerMachines() error {
	for i := 0; i < s.machineCount; i++ {
		machine := Machine{
			ID:   uuid.New(),
			Name: fmt.Sprintf("machine-%d", i+1),
		}

		reqBody, _ := json.Marshal(map[string]string{
			"name":     machine.Name,
			"type":     "simulator",
			"location": "test-lab",
		})

		resp, err := http.Post(s.apiURL+"/api/v1/machines", "application/json", bytes.NewReader(reqBody))
		if err != nil {
			return fmt.Errorf("failed to create machine: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			return fmt.Errorf("failed to create machine: status %d", resp.StatusCode)
		}

		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err == nil {
			if id, ok := result["id"].(string); ok {
				machine.ID, _ = uuid.Parse(id)
			}
		}

		_ = reqBody // silence unused warning for now
		s.machines = append(s.machines, machine)
		log.Printf("Registered machine: %s (%s)", machine.Name, machine.ID)
	}

	return nil
}

func (s *Simulator) run() {
	metricNames := []string{"temperature", "pressure", "vibration", "rpm", "voltage", "current"}
	units := map[string]string{
		"temperature": "celsius",
		"pressure":    "bar",
		"vibration":   "mm/s",
		"rpm":         "rpm",
		"voltage":     "volts",
		"current":     "amps",
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		for i := 0; i < s.metricsPerSec; i++ {
			machine := s.machines[rand.Intn(len(s.machines))]
			metricName := metricNames[rand.Intn(len(metricNames))]

			value := generateValue(metricName)

			payload := map[string]interface{}{
				"machine_id":  machine.ID.String(),
				"metric_name": metricName,
				"value":       value,
				"unit":        units[metricName],
				"quality":     "good",
			}

			go s.sendMetric(payload)
		}
	}
}

func (s *Simulator) sendMetric(payload map[string]interface{}) {
	body, _ := json.Marshal(payload)
	resp, err := http.Post(s.apiURL+"/api/v1/metrics/ingest", "application/json", bytes.NewReader(body))
	if err != nil {
		return
	}
	defer resp.Body.Close()
}

func generateValue(metric string) float64 {
	baseValues := map[string]float64{
		"temperature": 60,
		"pressure":    2.5,
		"vibration":   5,
		"rpm":         1500,
		"voltage":     220,
		"current":     15,
	}

	base := baseValues[metric]
	variation := base * 0.3
	return base + (rand.Float64()*2-1)*variation
}

func getEnv(key string, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		var intVal int
		if _, err := fmt.Sscanf(value, "%d", &intVal); err == nil {
			return intVal
		}
	}
	return defaultValue
}
