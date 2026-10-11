.DEFAULT_GOAL := help
SHELL := /bin/bash

help: ## Hien thi danh sach lenh
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

env: ## Tao .env tu mau
	@test -f .env || cp .env.example .env
	@echo ".env da san sang"

up: ## Dung toan bo ha tang
	cd docker && docker compose up -d

down: ## Dung ha tang (giu du lieu)
	cd docker && docker compose down

clean: ## Dung va xoa container + volume (MAT DU LIEU)
	cd docker && docker compose down -v --remove-orphans

ps: ## Xem trang thai container
	cd docker && docker compose ps

logs: ## Xem log: make logs SVC=namenode01
	cd docker && docker compose logs -f $(SVC)

shell: ## Vao shell: make shell SVC=namenode01
	cd docker && docker compose exec $(SVC) bash

dbt-run: ## Chay model dbt
	cd dbt && dbt run

dbt-test: ## Test du lieu dbt
	cd dbt && dbt test

.PHONY: help env up down clean ps logs shell dbt-run dbt-test
