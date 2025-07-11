docker compose -f docker-compose.yaml down --remove-orphans
docker compose -f docker-compose-rabbit.yaml down --remove-orphans
docker compose down
docker network create tpp-network
make rabbitmq-up
sleep 5
make docker-compose-up
make docker-compose-logs