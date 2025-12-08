# Start the full stack
start: compose

# Start containers in detached mode
compose:
	docker compose up -d

# Stop and remove containers
stop:
	docker compose down --volumes

# Run tests inside the Airflow worker container
test:
	docker exec airflow-worker pytest $(path)
