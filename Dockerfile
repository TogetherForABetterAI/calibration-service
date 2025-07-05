FROM python:3.10.12-slim


WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY src/ /app/

# Set PYTHONPATH
ENV PYTHONPATH=/app/

# Expose the port the app runs on
EXPOSE 8001

RUN apt-get update && apt-get install -y unzip wget \
  && wget https://github.com/protocolbuffers/protobuf/releases/download/v31.1/protoc-31.1-linux-x86_64.zip \
  && unzip protoc-31.1-linux-x86_64.zip -d /usr/local \
  && rm protoc-31.1-linux-x86_64.zip

RUN protoc --proto_path=/app/proto --python_out=/app/proto /app/proto/calibration.proto
RUN protoc --proto_path=/app/proto --python_out=/app/proto /app/proto/dataset.proto

# Fix the path to the main module
CMD ["uvicorn", "main:app", "--port", "8001", "--host", "0.0.0.0", "--reload"]