make docker-compose-down
make rabbitmq-up
sleep 5
make docker-compose-up
make docker-compose-logs