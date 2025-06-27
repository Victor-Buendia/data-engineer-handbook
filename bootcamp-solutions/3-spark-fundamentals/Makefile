up:
	docker compose up -d

down:
	docker compose down

chispa-setup:
	docker build -t chispa .
	docker run --name chispa -d --rm -it -v $(shell pwd)/src:/opt/bitnami/spark/src chispa

pytest:
	docker exec -it chispa /bin/bash -c "cd /opt/bitnami/spark/src && python3 -m pytest --maxfail=1 --disable-warnings -v"

.PHONY: up down chispa