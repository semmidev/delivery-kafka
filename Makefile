# ─────────────────────────────────────────────────────────────────
# Delivery Kafka — Makefile
# ─────────────────────────────────────────────────────────────────

.PHONY: up down init-topics producer consumer logs-kafka ps help

## Jalankan 3-broker Kafka cluster + Kafka UI
up:
	docker compose up -d
	@echo ""
	@echo "⏳ Menunggu cluster stabilisasi (15 detik)..."
	@sleep 15
	@echo "✅ Cluster UP. Jalankan: make init-topics"

## Matikan cluster
down:
	docker compose down

## Hapus cluster + semua data (reset total)
reset:
	docker compose down -v

## Buat semua Kafka topics
init-topics:
	@chmod +x scripts/init-topics.sh
	@bash scripts/init-topics.sh

## Jalankan producer (simulasi driver)
producer:
	@cd producer && go run .

## Jalankan consumer + API
consumer:
	@cd consumer && go run .

## Download dependencies
deps:
	@cd producer && go mod tidy
	@cd consumer && go mod tidy

## Lihat log kafka broker
logs-kafka:
	docker compose logs -f kafka-1 kafka-2 kafka-3

## Status container
ps:
	docker compose ps

## List topics
list-topics:
	docker exec kafka-1 kafka-topics.sh \
		--bootstrap-server kafka-1:9092 --list

## Describe topic lokasi
describe-location:
	docker exec kafka-1 kafka-topics.sh \
		--bootstrap-server kafka-1:9092 \
		--describe --topic delivery.driver.location-updated

## Consume langsung dari terminal (debug)
peek-location:
	docker exec kafka-1 kafka-console-consumer.sh \
		--bootstrap-server kafka-1:9092 \
		--topic delivery.driver.location-updated \
		--from-beginning --max-messages 5

peek-status:
	docker exec kafka-1 kafka-console-consumer.sh \
		--bootstrap-server kafka-1:9092 \
		--topic delivery.order.status-changed \
		--from-beginning --max-messages 5

## Consumer group status
consumer-groups:
	docker exec kafka-1 kafka-consumer-groups.sh \
		--bootstrap-server kafka-1:9092 \
		--describe --group tracking-api-service

help:
	@echo ""
	@echo "Delivery Kafka — Perintah tersedia:"
	@echo "  make up              Jalankan Kafka cluster (3 broker KRaft)"
	@echo "  make init-topics     Buat semua topics"
	@echo "  make producer        Jalankan producer (simulasi driver)"
	@echo "  make consumer        Jalankan consumer + REST API"
	@echo "  make down            Matikan cluster"
	@echo "  make reset           Reset total (hapus data)"
	@echo "  make list-topics     Lihat semua topics"
	@echo "  make consumer-groups Lihat consumer group lag"
	@echo "  make peek-location   Debug: baca lokasi dari terminal"
	@echo "  make peek-status     Debug: baca status dari terminal"
	@echo ""
	@echo "Kafka UI: http://localhost:8080"
	@echo "API:      http://localhost:8081"
	@echo ""
