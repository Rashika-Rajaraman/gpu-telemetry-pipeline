// Command apigateway exposes the telemetry REST API and can emit the OpenAPI spec.
//
// Flags:
//
//	--dump-openapi   print the generated OpenAPI document to stdout and exit
//	                 (used by `make openapi`).
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/cisco-interview/telemetry-pipeline/apigateway/internal/api"
	"github.com/cisco-interview/telemetry-pipeline/apigateway/internal/config"
	"github.com/cisco-interview/telemetry-pipeline/apigateway/internal/openapi"
	"github.com/cisco-interview/telemetry-pipeline/apigateway/internal/store"
)

// defaultDBBackoff is the wait between database connection attempts.
const defaultDBBackoff = 2 * time.Second

func main() {
	dumpOpenAPI := flag.Bool("dump-openapi", false, "print the OpenAPI spec and exit")
	flag.Parse()

	if *dumpOpenAPI {
		out, err := openapi.YAML()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Print(string(out))
		return
	}

	cfg := config.Load()
	logger := config.NewLogger(cfg)
	logger.WithField("listen", cfg.ListenAddr).Info("apigateway starting")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := connectDB(ctx, cfg.DBDSN, logger)
	if err != nil {
		logger.Info("shutting down before database connection established")
		return
	}
	defer st.Close()

	handler := api.New(st, logger)
	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	logger.WithField("addr", cfg.ListenAddr).Info("apigateway listening")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.WithError(err).Fatal("server error")
	}
	logger.Info("apigateway shut down cleanly")
}

// connectDB connects to PostgreSQL, retrying with a fixed backoff because the
// database pod may start after the API gateway in Kubernetes.
func connectDB(ctx context.Context, dsn string, logger *logrus.Logger) (*store.Postgres, error) {
	for attempt := 1; ; attempt++ {
		st, err := store.NewPostgres(ctx, dsn)
		if err == nil {
			logger.Info("connected to database")
			return st, nil
		}
		logger.WithError(err).WithField("attempt", attempt).Warn("database not ready, retrying")
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(defaultDBBackoff):
		}
	}
}
