FROM python:3.14-alpine


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

# Run the calibration service
CMD ["python", "src/main.py"]