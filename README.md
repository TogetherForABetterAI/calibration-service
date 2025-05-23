# Calibration Service

This repository contains the server-side component for the TPP application.

## Docker Container Setup Instructions

Follow these steps to build and run the server-side container:

### Building the Docker Image

```bash
# Build the Docker image with a tag
sudo docker build -t calibration-service .
```

### Running the Container

```bash
# Run the container, exposing port 8001
sudo docker run -p 8001:8001 calibration-service
```

### Stopping All Running Containers

To stop all running Docker containers at once:

```bash
sudo docker stop $(sudo docker ps -q)
```

### Accessing the API

Once running, the API will be available at:

- http://localhost:8001
