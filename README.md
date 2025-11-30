# Calibration Service

![workflow](https://github.com/TogetherForABetterAI/calibration-service/actions/workflows/test.yml/badge.svg) [![codecov](https://codecov.io/gh/TogetherForABetterAI/calibration-service/graph/badge.svg?token=KGS364DRTC)](https://codecov.io/gh/TogetherForABetterAI/calibration-service)

## Overview

The `calibration-service` supports **multiple clients simultaneously** through a gRPC-based client registration system. Each client gets its own dedicated processing thread that coordinates data streams from both the data-dispatcher-service and the client SDK.

## Architecture

### Multi-Client

- **gRPC server** for client registration
- **Per-client dedicated threads** for processing
- **Isolated data streams** per client

## Project Structure

```
src/
├── api/                          # API Layer - gRPC services
├── config/                       # Configuration management
├── core/                         # Core business logic
├── middleware/                   # Infrastructure - RabbitMQ
├── proto/                        # Protocol definitions
├── service/                      # Service layer - data processing
└── main.py                       # Application entry point
```

## How It Works

### 1. Client Registration

```python
# Client calls gRPC endpoint
NotifyNewClient({
    user_id: "client_123",
    inter_queue_name: "data_dispatcher_to_client_123",
    client_queue_name: "sdk_to_client_123"
})
```

### 2. Thread Spawning

- Service immediately returns `OK` status
- Spawns dedicated thread for `client_123`
- Thread connects to both queues:
  - `data_dispatcher_to_client_123` (data from data-dispatcher-service)
  - `sdk_to_client_123` (probabilities from SDK)

### 3. Data Coordination

- Thread listens to both queues simultaneously
- Coordinates incoming data and probabilities by batch index
- Processes complete batches with ML model
- Logs metrics to client-specific MLflow experiment

### 4. Isolation

- Each client has its own:
  - RabbitMQ connections
  - Processing thread
  - MLflow experiment
  - Data storage

## Configuration

### Environment Variables

```bash
# RabbitMQ Configuration
RABBITMQ_HOST=rabbitmq
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASSWORD=guest

# gRPC Configuration
GRPC_PORT=50051

# MLflow Configuration
MLFLOW_TRACKING_URI=optional_mlflow_server
MLFLOW_EXPERIMENT_NAME=Calibration Experiment for MNIST

# Application Configuration
LOG_LEVEL=INFO
ARTIFACTS_PATH=artifacts
```

### RabbitMQ Requirements

- **Queues must be pre-declared** by the users service
- **Service does NOT declare queues**, only consumes from them
- Uses `auto_ack=False` and `prefetch_count=1` for reliable delivery
- Manual acknowledgment after successful processing

## Usage

### Starting the Service

To start the service from your console, run:

```bash
# (Optional) Make the script executable the first time
chmod +x start.sh

# Start everything (RabbitMQ, build, and calibration-service) and follow logs
./start.sh
```

- This script will:
  - Stop any running containers
  - Start RabbitMQ
  - Build and start the calibration-service (and any other services in docker-compose)
  - Show the logs

**Note:**

- The service will be available on the gRPC port you set in `docker-compose.yaml` (e.g., 50052).

### Alternative Commands

```bash
# Start only the calibration service (requires RabbitMQ to be running)
make docker-compose-up

# Start only RabbitMQ
make rabbitmq-up

# Stop all services
make docker-compose-down

# View logs
make docker-compose-logs
```

## Development

### Adding New Features

1. **API Changes**: Add to `src/api/`
2. **Business Logic**: Add to `src/core/`
3. **Data Processing**: Add to `src/service/`
4. **External Connections**: Add to `src/middleware/`
