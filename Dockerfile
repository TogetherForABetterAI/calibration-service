FROM python:3.13-slim


WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

RUN apt-get update && apt-get install -y build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY src/ /app/src/
COPY utrace/ /app/utrace/

# Set PYTHONPATH
ENV PYTHONPATH=/app

# Expose the gRPC port
EXPOSE 50052

# Run the calibration service
CMD ["python", "src/main.py"]