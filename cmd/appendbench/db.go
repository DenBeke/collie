package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	gormpg "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// openDB returns a pool-configured *gorm.DB, a human-readable description of the
// target, and a cleanup func.
//
//   - cfg.LatencyMs > 0: an ephemeral Postgres + Toxiproxy are started and the
//     connection is routed through Toxiproxy with a latency toxic (see
//     openDBWithLatency).
//   - cfg.DSN set: connect to that external database.
//   - otherwise: a throwaway postgres:17 testcontainer (co-located with the
//     client, so latency is inflated — prefer an external -dsn for clean numbers).
func openDB(ctx context.Context, cfg *Config) (db *gorm.DB, target string, cleanup func(), err error) {
	if cfg.LatencyMs > 0 {
		return openDBWithLatency(ctx, cfg)
	}

	dsn := cfg.DSN
	cleanup = func() {}
	target = dsn

	if dsn == "" {
		container, cerr := tcpostgres.Run(ctx, "postgres:17",
			tcpostgres.WithDatabase("appendbench"),
			tcpostgres.WithUsername("bench"),
			tcpostgres.WithPassword("bench"),
			testcontainers.WithWaitStrategy(
				wait.ForAll(
					wait.ForLog("database system is ready to accept connections").
						WithOccurrence(2).WithStartupTimeout(60*time.Second),
					wait.ForListeningPort("5432/tcp"),
				),
			),
		)
		if cerr != nil {
			return nil, "", nil, fmt.Errorf("start testcontainer: %w", cerr)
		}
		cleanup = func() { _ = container.Terminate(context.Background()) }

		connStr, serr := container.ConnectionString(ctx, "sslmode=disable")
		if serr != nil {
			cleanup()
			return nil, "", nil, fmt.Errorf("container connection string: %w", serr)
		}
		dsn = connStr
		target = "testcontainer postgres:17 (co-located)"
	}

	db, err = openGorm(dsn)
	if err != nil {
		cleanup()
		return nil, "", nil, err
	}
	if err = configurePool(db, cfg); err != nil {
		cleanup()
		return nil, "", nil, err
	}
	return db, target, cleanup, nil
}

// openDBWithLatency starts an ephemeral Postgres and a Toxiproxy container on a
// shared Docker network, then routes the client connection through Toxiproxy with
// a downstream "latency" toxic. Every DB round-trip then pays cfg.LatencyMs —
// including each statement inside the locked Append transaction (advisory-lock
// acquire, COUNT, inserts, commit), which is the effect worth measuring: latency
// multiplies while the global lock is held. All resources are torn down by cleanup.
func openDBWithLatency(ctx context.Context, cfg *Config) (*gorm.DB, string, func(), error) {
	var cleanups []func()
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}
	fail := func(format string, args ...any) (*gorm.DB, string, func(), error) {
		cleanup()
		return nil, "", nil, fmt.Errorf(format, args...)
	}

	const (
		pgAlias      = "appendbench-pg"
		pgUser       = "bench"
		pgPass       = "bench"
		pgDB         = "appendbench"
		adminPortInt = "8474"
		proxyPortInt = "8666"
	)

	nw, err := network.New(ctx)
	if err != nil {
		return fail("create network: %w", err)
	}
	cleanups = append(cleanups, func() { _ = nw.Remove(context.Background()) })

	pg, err := tcpostgres.Run(ctx, "postgres:17",
		tcpostgres.WithDatabase(pgDB),
		tcpostgres.WithUsername(pgUser),
		tcpostgres.WithPassword(pgPass),
		network.WithNetwork([]string{pgAlias}, nw),
		testcontainers.WithWaitStrategy(
			wait.ForAll(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).WithStartupTimeout(60*time.Second),
				wait.ForListeningPort("5432/tcp"),
			),
		),
	)
	if err != nil {
		return fail("start postgres: %w", err)
	}
	cleanups = append(cleanups, func() { _ = pg.Terminate(context.Background()) })

	toxi, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        cfg.ToxiproxyImage,
			ExposedPorts: []string{adminPortInt + "/tcp", proxyPortInt + "/tcp"},
			Networks:     []string{nw.Name},
			WaitingFor: wait.ForHTTP("/version").
				WithPort(adminPortInt + "/tcp").
				WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return fail("start toxiproxy: %w", err)
	}
	cleanups = append(cleanups, func() { _ = toxi.Terminate(context.Background()) })

	host, err := toxi.Host(ctx)
	if err != nil {
		return fail("toxiproxy host: %w", err)
	}
	adminPort, err := toxi.MappedPort(ctx, adminPortInt+"/tcp")
	if err != nil {
		return fail("toxiproxy admin port: %w", err)
	}
	proxyPort, err := toxi.MappedPort(ctx, proxyPortInt+"/tcp")
	if err != nil {
		return fail("toxiproxy proxy port: %w", err)
	}

	adminURL := fmt.Sprintf("http://%s:%s", host, adminPort.Port())
	upstream := fmt.Sprintf("%s:5432", pgAlias)
	if err := createLatencyProxy(adminURL, "appendbench", "0.0.0.0:"+proxyPortInt, upstream, cfg.LatencyMs, cfg.LatencyJitterMs); err != nil {
		return fail("configure toxiproxy: %w", err)
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", pgUser, pgPass, host, proxyPort.Port(), pgDB)
	db, err := openGorm(dsn)
	if err != nil {
		return fail("%w", err)
	}
	if err := configurePool(db, cfg); err != nil {
		return fail("%w", err)
	}
	target := fmt.Sprintf("testcontainer postgres:17 via toxiproxy (+%dms/round-trip)", cfg.LatencyMs)
	return db, target, cleanup, nil
}

var toxiHTTP = &http.Client{Timeout: 10 * time.Second}

// createLatencyProxy creates a Toxiproxy proxy (postgres upstream) and attaches a
// downstream latency toxic. The proxy POST is retried since the admin API may
// accept connections a moment before it is fully ready.
func createLatencyProxy(adminURL, name, listen, upstream string, latencyMs, jitterMs int) error {
	proxyBody := map[string]any{"name": name, "listen": listen, "upstream": upstream, "enabled": true}
	var lastErr error
	for range 15 {
		if lastErr = toxiPost(adminURL+"/proxies", proxyBody); lastErr == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if lastErr != nil {
		return fmt.Errorf("create proxy: %w", lastErr)
	}

	// stream=downstream applies the delay to server->client packets, so each
	// query round-trip incurs ~latencyMs.
	toxicBody := map[string]any{
		"name": "latency_downstream", "type": "latency", "stream": "downstream", "toxicity": 1.0,
		"attributes": map[string]any{"latency": latencyMs, "jitter": jitterMs},
	}
	if err := toxiPost(adminURL+"/proxies/"+name+"/toxics", toxicBody); err != nil {
		return fmt.Errorf("add latency toxic: %w", err)
	}
	return nil
}

func toxiPost(url string, body any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := toxiHTTP.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		return fmt.Errorf("POST %s: status %d: %s", url, resp.StatusCode, buf.String())
	}
	return nil
}

func configurePool(db *gorm.DB, cfg *Config) error {
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("get sql.DB: %w", err)
	}
	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(0)
	return nil
}

// openGorm dials the DSN with retries (Colima/container port forwarding can lag)
// and a Silent logger so per-statement logging never dominates timing.
func openGorm(dsn string) (*gorm.DB, error) {
	gcfg := &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)}
	var db *gorm.DB
	var err error
	for range 15 {
		db, err = gorm.Open(gormpg.Open(dsn), gcfg)
		if err == nil {
			sqlDB, e := db.DB()
			if e == nil {
				if e = sqlDB.Ping(); e == nil {
					return db, nil
				}
			}
			err = e
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil, fmt.Errorf("connect to database: %w", err)
}
