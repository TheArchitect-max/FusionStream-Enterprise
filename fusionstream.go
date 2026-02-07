package main

import (
	"bytes"
	"context"
	"container/heap"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	DefaultQueueSize     = 200000
	DefaultRateLimit     = 5000
	DefaultHealthPort    = 8080
	DefaultMetricsPort   = 9090
	DefaultPidFile       = "/tmp/fusionstream.pid"
	Version              = "v1.0"
	HealthAuthUser       = "health"
	HealthAuthPass       = "fusion123"
	MetricsAuthUser      = "metrics"
	MetricsAuthPass      = "fusion456"
	MaxWorkers           = 32
	DrainTimeout         = 5 * time.Second
	MaxDrainMessages     = 5000
)

// Config holds all configuration parameters
type Config struct {
	QueueSize         int
	RateLimitPerSource int
	HealthPort        int
	MetricsPort       int
	PidFile           string
	APIKeys           []string
	HealthAuth        struct {
		User string
		Pass string
	}
	MetricsAuth struct {
		User string
		Pass string
	}
	BindAddr string
}

// Priority represents message priority levels
type Priority int

const (
	PriorityLow Priority = iota
	PriorityNormal
	PriorityHigh
	PriorityEmergency
)

// Message represents a single message in the system
type Message struct {
	CorrelationID string
	ID            uint64
	Timestamp     int64
	SourceHash    uint32
	Proto         uint8
	Prio          Priority
	Data          []byte
}

// PriorityQueue implements heap.Interface for efficient priority operations
type PriorityQueue []*Message

func (pq PriorityQueue) Len() int           { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool { return pq[i].Prio > pq[j].Prio }
func (pq PriorityQueue) Swap(i, j int)      { pq[i], pq[j] = pq[j], pq[i] }

func (pq *PriorityQueue) Push(x interface{}) {
	*pq = append(*pq, x.(*Message))
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	msg := old[n-1]
	*pq = old[0 : n-1]
	return msg
}

// RateLimiter manages per-source rate limiting
type RateLimiter struct {
	maxPerSecond int64
	buckets      sync.Map
}

func NewRateLimiter(maxPerSecond int) *RateLimiter {
	return &RateLimiter{maxPerSecond: int64(maxPerSecond)}
}

func (rl *RateLimiter) Allow(sourceHash uint32) bool {
	now := float64(time.Now().UnixNano()) / 1e9
	v, _ := rl.buckets.LoadOrStore(sourceHash, []float64{})
	bucket := v.([]float64)
	
	cleaned := bucket[:0]
	for _, t := range bucket {
		if now-t <= 1.0 {
			cleaned = append(cleaned, t)
		}
	}
	
	if int64(len(cleaned)) < rl.maxPerSecond {
		cleaned = append(cleaned, now)
		rl.buckets.Store(sourceHash, cleaned)
		return true
	}
	return false
}

// Engine is the main application component
type Engine struct {
	config        Config
	rateLimiter   *RateLimiter
	apiKeys       sync.Map
	priorityQueue *PriorityQueue
	routeTable    sync.Map
	healthScores  sync.Map
	metrics       struct {
		processed uint64
		routed    uint64
		enqueued  uint64
		dropped   uint64
	}
	running       int32
	startTime     int64
	healthPort    atomic.Int32
	metricsPort   atomic.Int32
	wg            sync.WaitGroup
	shutdownCtx   context.Context
	shutdownCancel context.CancelFunc
	pidFile       atomic.Pointer[string]
}

func LoadConfig() Config {
	config := Config{
		QueueSize:         getEnvInt("QUEUE_SIZE", DefaultQueueSize),
		RateLimitPerSource: getEnvInt("RATE_LIMIT", DefaultRateLimit),
		HealthPort:        getEnvInt("HEALTH_PORT", DefaultHealthPort),
		MetricsPort:       getEnvInt("METRICS_PORT", DefaultMetricsPort),
		PidFile:           os.Getenv("PID_FILE"),
		BindAddr:          getEnv("BIND_ADDR", "0.0.0.0"),
	}
	
	if config.PidFile == "" {
		config.PidFile = DefaultPidFile
	}
	
	config.APIKeys = strings.FieldsFunc(getEnv("API_KEYS", "prod-main-001"), func(r rune) bool {
		return r == ',' || r == ';'
	})
	
	config.HealthAuth.User = getEnv("HEALTH_USER", HealthAuthUser)
	config.HealthAuth.Pass = getEnv("HEALTH_PASS", HealthAuthPass)
	config.MetricsAuth.User = getEnv("METRICS_USER", MetricsAuthUser)
	config.MetricsAuth.Pass = getEnv("METRICS_PASS", MetricsAuthPass)
	
	return config
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return fallback
}

func NewEngine() *Engine {
	config := LoadConfig()
	e := &Engine{
		config:        config,
		rateLimiter:   NewRateLimiter(config.RateLimitPerSource),
		priorityQueue: &PriorityQueue{},
		shutdownCtx:   context.Background(),
	}
	
	heap.Init(e.priorityQueue)
	
	// Load API keys
	for _, key := range config.APIKeys {
		key = strings.TrimSpace(key)
		if key != "" {
			e.apiKeys.Store(key, struct{}{})
		}
	}
	
	e.writePID()
	log.Printf("FusionStream %s started", Version)
	return e
}

func (e *Engine) writePID() {
	pid := os.Getpid()
	data := []byte(strconv.Itoa(pid))
	pidFile := e.config.PidFile
	
	if err := os.WriteFile(pidFile, data, 0600); err != nil {
		log.Printf("WARN: PID file write failed: %v", err)
	}
	e.pidFile.Store(&pidFile)
}

func (e *Engine) Start() {
	atomic.StoreInt32(&e.running, 1)
	atomic.StoreInt64(&e.startTime, time.Now().Unix())
	
	ctx, cancel := context.WithCancel(context.Background())
	e.shutdownCtx = ctx
	e.shutdownCancel = cancel
	
	e.wg.Add(6)
	
	go e.workerPool()
	go e.healthMonitor()
	go e.metricsReporter()
	go e.shutdownWatcher()
	go e.serveHealth()
	go e.serveMetrics()
}

func (e *Engine) Stop() {
	if !atomic.CompareAndSwapInt32(&e.running, 1, 0) {
		return
	}
	
	log.Println("Shutdown initiated")
	e.shutdownCancel()
	
	// Drain queue
	drained := uint32(0)
	drainCtx, cancel := context.WithTimeout(context.Background(), DrainTimeout)
	defer cancel()
	
	for drained < MaxDrainMessages && len(*e.priorityQueue) > 0 {
		select {
		case <-drainCtx.Done():
			log.Println("Drain timeout reached")
			return
		default:
			if msg := heap.Pop(e.priorityQueue).(*Message); msg != nil {
				e.processMessage(msg)
				atomic.AddUint32(&drained, 1)
			}
		}
		runtime.Gosched()
	}
	
	e.cleanup()
	e.wg.Wait()
	log.Printf("Shutdown complete - drained %d messages", drained)
}

// Worker pool with bounded concurrency
func (e *Engine) workerPool() {
	defer e.wg.Done()
	
	sem := make(chan struct{}, MaxWorkers)
	
	for {
		select {
		case <-e.shutdownCtx.Done():
			return
		default:
			if len(*e.priorityQueue) == 0 {
				time.Sleep(time.Millisecond)
				continue
			}
			
			select {
			case msg := heap.Pop(e.priorityQueue).(*Message):
				sem <- struct{}{}
				go func(m *Message) {
					defer func() {
						<-sem
						if r := recover(); r != nil {
							log.Printf("Pipeline panic: %v", r)
						}
					}()
					e.processMessage(m)
				}(msg)
			default:
				time.Sleep(time.Millisecond)
			}
		}
	}
}

func (e *Engine) processMessage(msg *Message) {
	atomic.AddUint64(&e.metrics.processed, 1)
	
	routes := e.getRoutes(msg.SourceHash)
	if len(routes) == 0 {
		return
	}
	
	// Route to up to 2 healthy destinations
	var healthyRoutes []uint32
	for _, route := range routes {
		if e.getHealthScore(route) > 0.7 {
			healthyRoutes = append(healthyRoutes, route)
			if len(healthyRoutes) == 2 {
				break
			}
		}
	}
	
	if len(healthyRoutes) == 0 {
		healthyRoutes = routes[:1]
	}
	
	for _, route := range healthyRoutes {
		atomic.AddUint64(&e.metrics.routed, 1)
	}
}

func (e *Engine) getRoutes(sourceHash uint32) []uint32 {
	v, ok := e.routeTable.Load(sourceHash)
	if !ok {
		return nil
	}
	return v.([]uint32)
}

func (e *Engine) getHealthScore(nodeID uint32) float64 {
	v, ok := e.healthScores.Load(nodeID)
	if !ok {
		return 1.0
	}
	score := v.(float64)*0.97 + 0.03
	score = math.Max(0.0, math.Min(1.0, score))
	e.healthScores.Store(nodeID, score)
	return score
}

func (e *Engine) healthMonitor() {
	defer e.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-e.shutdownCtx.Done():
			return
		case <-ticker.C:
			var degraded int
			e.healthScores.Range(func(_, value interface{}) bool {
				if value.(float64) < 0.7 {
					degraded++
				}
				return true
			})
			if degraded > 0 {
				log.Printf("WARN: %d degraded nodes", degraded)
			}
		}
	}
}

func (e *Engine) metricsReporter() {
	defer e.wg.Done()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-e.shutdownCtx.Done():
			return
		case <-ticker.C:
			processed := atomic.LoadUint64(&e.metrics.processed)
			rate := float64(processed) / 15
			log.Printf("METRICS: %.1f msg/s, queue: %d/%d", 
				rate, len(*e.priorityQueue), e.config.QueueSize)
		}
	}
}

func (e *Engine) shutdownWatcher() {
	defer e.wg.Done()
	<-e.shutdownCtx.Done()
}

func (e *Engine) cleanup() {
	if pidFile := e.pidFile.Load(); pidFile != nil {
		os.Remove(*pidFile)
	}
}

// HTTP Server utilities
func (e *Engine) findPort(ports []int) int {
	for _, port := range ports {
		addr := fmt.Sprintf("%s:%d", e.config.BindAddr, port)
		if ln, err := net.Listen("tcp", addr); err == nil {
			ln.Close()
			return port
		}
	}
	return ports[0]
}

func (e *Engine) basicAuthMiddleware(next http.HandlerFunc, user, pass string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userPass := user + ":" + pass
		auth := r.Header.Get("Authorization")
		if auth == "" || subtle.ConstantTimeCompare([]byte(auth), []byte("Basic "+basicAuthEncode(userPass))) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
			http.Error(w, "401 Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	}
}

func basicAuthEncode(creds string) string {
	return base64.StdEncoding.EncodeToString([]byte(creds))
}

func (e *Engine) serveHealth() {
	defer e.wg.Done()
	
	ports := []int{e.config.HealthPort, 8081, 8082, 8083}
	port := e.findPort(ports)
	e.healthPort.Store(int32(port))
	
	mux := http.NewServeMux()
	mux.HandleFunc("/health", e.basicAuthMiddleware(e.healthHandler, 
		e.config.HealthAuth.User, e.config.HealthAuth.Pass))
	
	server := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", e.config.BindAddr, port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}
	
	log.Printf("Health server started: %s:%d", e.config.BindAddr, port)
	server.ListenAndServe()
}

func (e *Engine) serveMetrics() {
	defer e.wg.Done()
	
	ports := []int{e.config.MetricsPort, 9091, 9092}
	port := e.findPort(ports)
	e.metricsPort.Store(int32(port))
	
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", e.basicAuthMiddleware(e.metricsHandler, 
		e.config.MetricsAuth.User, e.config.MetricsAuth.Pass))
	
	server := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", e.config.BindAddr, port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}
	
	log.Printf("Metrics server started: %s:%d", e.config.BindAddr, port)
	server.ListenAndServe()
}

func (e *Engine) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	
	health := map[string]interface{}{
		"status":     "healthy",
		"queue_depth": len(*e.priorityQueue),
		"queue_max":  e.config.QueueSize,
		"uptime":     time.Now().Unix() - atomic.LoadInt64(&e.startTime),
		"version":    Version,
	}
	
	json.NewEncoder(w).Encode(health)
}

func (e *Engine) metricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	
	uptime := time.Now().Unix() - atomic.LoadInt64(&e.startTime)
	processed := atomic.LoadUint64(&e.metrics.processed)
	
	fmt.Fprint(w, e.prometheusMetrics(uptime, processed))
}

func (e *Engine) prometheusMetrics(uptime int64, processed uint64) string {
	var routes int32
	e.routeTable.Range(func(_, _ interface{}) bool {
		atomic.AddInt32(&routes, 1)
		return true
	})
	
	return fmt.Sprintf(`# HELP fusionstream_queue_depth Queue depth
fusionstream_queue_depth %d
# HELP fusionstream_processed_total Total processed
fusionstream_processed_total %d
# HELP fusionstream_uptime_seconds Uptime seconds
fusionstream_uptime_seconds %d
# HELP fusionstream_active_routes Routes
fusionstream_active_routes %d
`, len(*e.priorityQueue), processed, uptime, routes)
}

// Public API methods
func (e *Engine) AddRoute(sourceID, destID, apiKey string) bool {
	if _, ok := e.apiKeys.Load(apiKey); !ok {
		log.Printf("WARN: Unauthorized route add: %s", apiKey[:8])
		return false
	}
	
	srcHash := hashString(sourceID)
	dstHash := hashString(destID)
	
	v, loaded := e.routeTable.LoadOrStore(srcHash, []uint32{dstHash})
	if loaded {
		routes := v.([]uint32)
		for _, r := range routes {
			if r == dstHash {
				return false
			}
		}
		newRoutes := append(routes, dstHash)
		e.routeTable.Store(srcHash, newRoutes)
	}
	return true
}

func (e *Engine) Publish(sourceID string, proto uint8, apiKey string, data []byte, prio Priority) (bool, string) {
	if atomic.LoadInt32(&e.running) == 0 {
		return false, "SHUTDOWN"
	}
	
	if _, ok := e.apiKeys.Load(apiKey); !ok {
		return false, "UNAUTHORIZED"
	}
	
	sourceHash := hashString(sourceID)
	if !e.rateLimiter.Allow(sourceHash) {
		return false, "RATE_LIMITED"
	}
	
	idBytes := make([]byte, 8)
	rand.Read(idBytes)
	
	msg := &Message{
		CorrelationID: fmt.Sprintf("%x", idBytes),
		ID:            uint64(time.Now().UnixNano()),
		Timestamp:     time.Now().UnixNano(),
		SourceHash:    sourceHash,
		Proto:         proto,
		Prio:          prio,
		Data:          data,
	}
	
	if len(*e.priorityQueue) < e.config.QueueSize {
		heap.Push(e.priorityQueue, msg)
		atomic.AddUint64(&e.metrics.enqueued, 1)
		return true, msg.CorrelationID
	}
	
	atomic.AddUint64(&e.metrics.dropped, 1)
	return false, "QUEUE_FULL"
}

func hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func main() {
	engine := NewEngine()
	
	// Demo routes
	apiKey := engine.config.APIKeys[0]
	for i := 0; i < 10; i++ {
		engine.AddRoute(fmt.Sprintf("source_%d", i), fmt.Sprintf("dest_%d", i), apiKey)
	}
	
	engine.Start()
	
	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	
	log.Printf("FusionStream %s running. Health: :%d Metrics: :%d", 
		Version, engine.healthPort.Load(), engine.metricsPort.Load())
	
	<-sigCh
	engine.Stop()
}
