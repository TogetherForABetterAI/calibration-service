FROM python:3.10.12-slim


WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir uvicorn

# Copy the source code
COPY src/ /app/

# Set PYTHONPATH
ENV PYTHONPATH=/app/

# Expose the port the app runs on
EXPOSE 8001

# Fix the path to the main module
CMD ["uvicorn", "main:app", "--port", "8001", "--host", "0.0.0.0", "--reload"]