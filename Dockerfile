FROM python:3.10.12-slim


WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY src/ /app/src/

# Set PYTHONPATH
ENV PYTHONPATH=/app

# Expose the gRPC port
EXPOSE 50052

# Install protoc for proto compilation
RUN apt-get update && apt-get install -y unzip wget \
  && wget https://github.com/protocolbuffers/protobuf/releases/download/v31.1/protoc-31.1-linux-x86_64.zip \
  && unzip protoc-31.1-linux-x86_64.zip -d /usr/local \
  && rm protoc-31.1-linux-x86_64.zip \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# RUN python -m grpc_tools.protoc -I/app/src/proto --python_out=/app/src/pb --pyi_out=/app/src/pb --grpc_python_out=/app/src/pb /app/src/proto/new-client-service.proto


# Compile proto files
RUN protoc --proto_path=/app/src/proto --python_out=/app/src/proto /app/src/proto/calibration.proto
RUN protoc --proto_path=/app/src/proto --python_out=/app/src/proto /app/src/proto/dataset.proto

# Run the calibration service
CMD ["python", "src/main.py"]