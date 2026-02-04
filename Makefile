# Airflow Playground Management

.PHONY: up down restart logs ps shell-scheduler shell-apiserver build trigger-dag list-dags

# Start Airflow in the background
up:
	docker compose up -d

# Stop and remove containers
down:
	docker compose down

# Restart all services
restart: down up

# Stream logs from the scheduler
logs:
	docker compose logs -f airflow-scheduler

# Check status of containers
ps:
	docker compose ps

# Access the scheduler shell
shell-scheduler:
	docker compose exec airflow-scheduler bash

# Access the API server shell
shell-apiserver:
	docker compose exec airflow-apiserver bash

# Rebuild the image (useful after updating requirements.txt)
build:
	docker compose build

# List all DAGs
list-dags:
	docker compose exec airflow-scheduler airflow dags list

# Trigger a DAG (usage: make trigger-dag dag=user_processing)
trigger-dag:
	@if [ -z "$(dag)" ]; then \
		echo "Error: dag variable is required. usage: make trigger-dag dag=id"; \
		exit 1; \
	fi
	docker compose exec airflow-scheduler airflow dags trigger $(dag)

# Clean up logs and plugins (use with caution)
clean-volumes:
	docker compose down -v
