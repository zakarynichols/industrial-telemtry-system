package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"telemetry/config"
	"telemetry/db"
	"telemetry/mqtt"
	"telemetry/processing"
)

func main() {
	cfg := config.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL())
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	log.Println("Connected to TimescaleDB")

	if err := db.Migrate(ctx, pool); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}
	log.Println("Database migrations complete")

	alertService := processing.NewAlertService(pool, cfg)

	router := mux.NewRouter()
	router.HandleFunc("/health", healthHandler)
	router.HandleFunc("/api/v1/machines", machinesHandler(pool))
	router.HandleFunc("/api/v1/metrics", metricsHandler(pool))
	router.HandleFunc("/api/v1/metrics/ingest", ingestHandler(pool, alertService))
	router.HandleFunc("/api/v1/alerts", alertsHandler(pool))
	router.HandleFunc("/api/v1/alerts/{id}/acknowledge", acknowledgeAlertHandler(pool))
	router.HandleFunc("/api/v1/rules", rulesHandler(pool))
	router.HandleFunc("/api/v1/anomalies", anomaliesHandler(pool))

	go func() {
		log.Printf("Starting MQTT server on :1883")
		if err := mqtt.StartServer(alertService); err != nil {
			log.Printf("MQTT server error: %v", err)
		}
	}()

	server := &http.Server{
		Addr:         ":8083",
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("Starting HTTP server on :8083")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited")
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func machinesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == "GET" {
			rows, err := pool.Query(r.Context(), "SELECT id, name, type, location, status, created_at FROM machines ORDER BY created_at DESC")
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			defer rows.Close()

			var machines []map[string]interface{}
			for rows.Next() {
				var id uuid.UUID
				var name, machineType, location, status string
				var createdAt time.Time
				if err := rows.Scan(&id, &name, &machineType, &location, &status, &createdAt); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				machines = append(machines, map[string]interface{}{
					"id":         id,
					"name":       name,
					"type":       machineType,
					"location":   location,
					"status":     status,
					"created_at": createdAt,
				})
			}
			if machines == nil {
				machines = []map[string]interface{}{}
			}
			json.NewEncoder(w).Encode(machines)
			return
		}

		if r.Method == "POST" {
			var input struct {
				Name     string `json:"name"`
				Type     string `json:"type"`
				Location string `json:"location"`
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			var id uuid.UUID
			err := pool.QueryRow(r.Context(),
				"INSERT INTO machines (name, type, location) VALUES ($1, $2, $3) RETURNING id",
				input.Name, input.Type, input.Location,
			).Scan(&id)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			json.NewEncoder(w).Encode(map[string]interface{}{"id": id, "name": input.Name})
			return
		}

		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func metricsHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		machineID := r.URL.Query().Get("machine_id")
		limit := r.URL.Query().Get("limit")
		if limit == "" {
			limit = "100"
		}

		var rows pgx.Rows
		var err error

		if machineID != "" {
			rows, err = pool.Query(r.Context(),
				"SELECT time, machine_id, metric_name, value, unit, quality FROM metrics WHERE machine_id = $1 ORDER BY time DESC LIMIT $2",
				machineID, limit,
			)
		} else {
			rows, err = pool.Query(r.Context(),
				"SELECT time, machine_id, metric_name, value, unit, quality FROM metrics ORDER BY time DESC LIMIT $1",
				limit,
			)
		}

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var metrics []map[string]interface{}
		for rows.Next() {
			var t time.Time
			var mid uuid.UUID
			var metricName string
			var value float64
			var unit, quality string
			if err := rows.Scan(&t, &mid, &metricName, &value, &unit, &quality); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			metrics = append(metrics, map[string]interface{}{
				"time":        t,
				"machine_id":  mid,
				"metric_name": metricName,
				"value":       value,
				"unit":        unit,
				"quality":     quality,
			})
		}
		if metrics == nil {
			metrics = []map[string]interface{}{}
		}
		json.NewEncoder(w).Encode(metrics)
	}
}

type IngestRequest struct {
	MachineID  string  `json:"machine_id"`
	MetricName string  `json:"metric_name"`
	Value      float64 `json:"value"`
	Unit       string  `json:"unit,omitempty"`
	Quality    string  `json:"quality,omitempty"`
	Timestamp  string  `json:"timestamp,omitempty"`
}

func ingestHandler(pool *pgxpool.Pool, alertService *processing.AlertService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method != "POST" {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var input IngestRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		machineID, err := uuid.Parse(input.MachineID)
		if err != nil {
			http.Error(w, "invalid machine_id", http.StatusBadRequest)
			return
		}

		timestamp := time.Now()
		if input.Timestamp != "" {
			timestamp, _ = time.Parse(time.RFC3339, input.Timestamp)
		}

		quality := input.Quality
		if quality == "" {
			quality = "good"
		}

		_, err = pool.Exec(r.Context(),
			"INSERT INTO metrics (time, machine_id, metric_name, value, unit, quality) VALUES ($1, $2, $3, $4, $5, $6)",
			timestamp, machineID, input.MetricName, input.Value, input.Unit, quality,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		go alertService.CheckMetric(machineID, input.MetricName, input.Value)

		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}
}

func alertsHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		rows, err := pool.Query(r.Context(),
			"SELECT id, machine_id, severity, message, acknowledged, created_at FROM alerts ORDER BY created_at DESC LIMIT 100")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var alerts []map[string]interface{}
		for rows.Next() {
			var id, machineID uuid.UUID
			var severity, message string
			var acknowledged bool
			var createdAt time.Time
			if err := rows.Scan(&id, &machineID, &severity, &message, &acknowledged, &createdAt); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			alerts = append(alerts, map[string]interface{}{
				"id":           id,
				"machine_id":   machineID,
				"severity":     severity,
				"message":      message,
				"acknowledged": acknowledged,
				"created_at":   createdAt,
			})
		}
		if alerts == nil {
			alerts = []map[string]interface{}{}
		}
		json.NewEncoder(w).Encode(alerts)
	}
}

func rulesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == "GET" {
			rows, err := pool.Query(r.Context(),
				"SELECT id, name, metric_name, condition_type, threshold_value, operator, severity, enabled FROM alert_rules")
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			defer rows.Close()

			var rules []map[string]interface{}
			for rows.Next() {
				var id uuid.UUID
				var name, metricName, conditionType, operator, severity string
				var thresholdValue *float64
				var enabled bool
				if err := rows.Scan(&id, &name, &metricName, &conditionType, &thresholdValue, &operator, &severity, &enabled); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				rules = append(rules, map[string]interface{}{
					"id":              id,
					"name":            name,
					"metric_name":     metricName,
					"condition_type":  conditionType,
					"threshold_value": thresholdValue,
					"operator":        operator,
					"severity":        severity,
					"enabled":         enabled,
				})
			}
			if rules == nil {
				rules = []map[string]interface{}{}
			}
			json.NewEncoder(w).Encode(rules)
			return
		}

		if r.Method == "POST" {
			var input struct {
				Name           string  `json:"name"`
				MetricName     string  `json:"metric_name"`
				ConditionType  string  `json:"condition_type"`
				ThresholdValue float64 `json:"threshold_value"`
				Operator       string  `json:"operator"`
				Severity       string  `json:"severity"`
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			var id uuid.UUID
			err := pool.QueryRow(r.Context(),
				"INSERT INTO alert_rules (name, metric_name, condition_type, threshold_value, operator, severity) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
				input.Name, input.MetricName, input.ConditionType, input.ThresholdValue, input.Operator, input.Severity,
			).Scan(&id)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			json.NewEncoder(w).Encode(map[string]interface{}{"id": id})
			return
		}

		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func anomaliesHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		rows, err := pool.Query(r.Context(),
			"SELECT id, machine_id, metric_name, detected_at, severity, description FROM anomalies ORDER BY detected_at DESC LIMIT 100")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var anomalies []map[string]interface{}
		for rows.Next() {
			var id, machineID uuid.UUID
			var metricName, severity, description string
			var detectedAt time.Time
			if err := rows.Scan(&id, &machineID, &metricName, &detectedAt, &severity, &description); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			anomalies = append(anomalies, map[string]interface{}{
				"id":          id,
				"machine_id":  machineID,
				"metric_name": metricName,
				"detected_at": detectedAt,
				"severity":    severity,
				"description": description,
			})
		}
		if anomalies == nil {
			anomalies = []map[string]interface{}{}
		}
		json.NewEncoder(w).Encode(anomalies)
	}
}

func acknowledgeAlertHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		vars := mux.Vars(r)
		alertID := vars["id"]

		if alertID == "" {
			http.Error(w, "alert id required", http.StatusBadRequest)
			return
		}

		alertUUID, err := uuid.Parse(alertID)
		if err != nil {
			http.Error(w, "invalid alert id", http.StatusBadRequest)
			return
		}

		_, err = pool.Exec(r.Context(),
			"UPDATE alerts SET acknowledged = true, acknowledged_at = NOW() WHERE id = $1",
			alertUUID,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		json.NewEncoder(w).Encode(map[string]string{"status": "acknowledged"})
	}
}
