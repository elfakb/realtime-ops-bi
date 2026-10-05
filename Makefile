up:
	docker compose up -d

down:
	docker compose down

reset:
	docker compose down -v

produce:
	python -m src.producer.event_generator

alerts:
	python -m src.alerts.alert_checker

logs-spark:
	docker compose logs -f spark

psql:
	docker compose exec postgres psql -U ops -d opsdb

test:
	pytest -q