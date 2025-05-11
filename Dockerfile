FROM python:3.10.12-slim


WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

COPY /src /app/
ENV PYTHONPATH=/app/

CMD ["uvicorn", "main:app", "--port", "8000", "--host", "0.0.0.0", "--reload"]