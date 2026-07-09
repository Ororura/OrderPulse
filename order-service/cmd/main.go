package main

import (
	"context"
	"log"
	"net/http"
	"order-service/internal/cache"
	"order-service/internal/config"
	"order-service/internal/database"
	"order-service/internal/handler"
	"order-service/internal/kafka"
	"order-service/internal/repository"
	"order-service/internal/service"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg := config.MustLoad()

	db, err := database.NewPostgres(cfg.DBURL)
	if err != nil {
		log.Fatal("failed to connect postgres:", err)
	}
	defer db.Close()

	producer := kafka.NewProducer(cfg.KafkaBrokets, cfg.KafkaOrderCreatedTopic)
	defer producer.Close()

	orderCache := cache.NewOrderCache(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB, cfg.OrderCacheTTL)
	defer orderCache.Close()

	orderRepo := repository.NewOrderRepository(db)
	orderService := service.NewOrderService(orderRepo, producer, orderCache)
	orderHandler := handler.NewOrderHandler(orderService)

	mux := http.NewServeMux()

	mux.HandleFunc("POST /orders", orderHandler.CreateOrder)
	mux.HandleFunc("GET /orders/{id}", orderHandler.GetOrder)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	server := &http.Server{
		Addr:         ":" + cfg.HTTPPort,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Println("order-service started on port", cfg.HTTPPort)

		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("server error:", err)
		}
	}()

	waitFotShutDown(server)
}

func waitFotShutDown(server *http.Server) {
	quit := make(chan os.Signal, 1)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	log.Println("shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Println("server shutdown error:", err)
	}

	log.Println("server stopped")
}
