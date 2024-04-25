.PHONY: all
all: build

build:
	go build -o platon .

run-deps: make-pod run-clickhouse run-superset run-nginx

make-pod:
	podman pod create --replace -p 8123:8123 -p 9040:9000 -p 8088:8088 -p 8080:8080 platon

run-clickhouse:
	podman run --pod platon -d --name platon-db --replace --ulimit nofile=262144:262144 clickhouse/clickhouse-server

run-nginx:
	podman run --pod platon -d --name platon-nginx --replace -v $(shell pwd)/nginx/nginx.conf:/etc/nginx/nginx.conf:ro nginx

build-superset:
	cp superset/superset_config_nosecret.py superset/superset_config.py
	echo "SECRET_KEY = '$(shell openssl rand -base64 42)'" >> superset/superset_config.py
	cd superset && podman build -t platon-superset .

run-superset:
	podman run -d --pod platon --replace --name platon-superset platon-superset
