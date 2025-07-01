SHELL := /bin/bash
PWD := $(shell pwd)

.PHONY: default build
default: docker-compose-up

.PHONY: docker-compose-up
docker-compose-up:
	docker compose -f docker-compose.yaml up -d --build

.PHONY: docker-compose-down
docker-compose-down:
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f docker-compose.yaml down

.PHONY: rabbitmq-up
rabbitmq-up:
	docker compose -f docker-compose-rabbit.yaml up -d --build



.PHONY: rabbitmq-down
rabbitmq-down:
	docker compose -f docker-compose-rabbit.yaml stop -t 1
	docker compose -f docker-compose-rabbit.yaml down

.PHONY: docker-compose-logs
docker-compose-logs:
	docker compose -f docker-compose.yaml logs -f		
