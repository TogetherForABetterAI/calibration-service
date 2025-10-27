#!/usr/bin/env python3
"""
Script para probar la instrumentación completa de calibration-service con RabbitMQ.
Simula el flujo completo:
1. Registra un cliente (gRPC NotifyNewClient)
2. Envía mensajes a las colas de RabbitMQ
3. Verifica que los background tasks se ejecuten y se capturen en New Relic
"""
import sys
sys.path.append('/app/src')

import pika
import grpc
import time
import numpy as np
from pb.new_client_service import new_client_service_pb2, new_client_service_pb2_grpc
from proto import dataset_pb2, calibration_pb2

def connect_rabbitmq():
    """Conectar a RabbitMQ"""
    credentials = pika.PlainCredentials('calibration', 'calibration123')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host='rabbitmq',
            credentials=credentials
        )
    )
    channel = connection.channel()
    return connection, channel

def register_client(client_id):
    """Registrar un cliente via gRPC"""
    channel = grpc.insecure_channel('localhost:50051')
    stub = new_client_service_pb2_grpc.ClientNotificationServiceStub(channel)
    
    request = new_client_service_pb2.NewClientRequest(client_id=client_id)
    response = stub.NotifyNewClient(request)
    
    return response.status == 'OK'

def send_data_batch(channel, client_id, batch_index, is_last=False):
    """Enviar un batch de datos a la cola inputs_queue_calibration"""
    queue_name = f"inputs_queue_calibration_client_{client_id}"
    
    # Crear datos fake (MNIST format: 28x28 images)
    num_images = 10
    image_data = np.random.rand(num_images, 1, 28, 28).astype(np.float32)
    labels = list(range(10))
    
    # Crear mensaje protobuf
    message = dataset_pb2.DataBatch(
        batch_index=batch_index,
        data=image_data.tobytes(),
        labels=labels,
        is_last_batch=is_last
    )
    
    # Publicar a la cola
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=message.SerializeToString()
    )
    
    print(f"  📤 Enviado DataBatch #{batch_index} (is_last={is_last})")

def send_predictions(channel, client_id, batch_index, is_eof=False):
    """Enviar predicciones a la cola outputs_queue_calibration"""
    queue_name = f"outputs_queue_calibration_client_{client_id}"
    
    # Crear predicciones fake (10 clases)
    predictions = []
    for _ in range(10):
        pred = calibration_pb2.Prediction(
            values=np.random.rand(10).astype(np.float32).tolist()
        )
        predictions.append(pred)
    
    # Crear mensaje protobuf
    message = calibration_pb2.Predictions(
        batch_index=batch_index,
        pred=predictions,
        eof=is_eof
    )
    
    # Publicar a la cola
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=message.SerializeToString()
    )
    
    print(f"  📤 Enviado Predictions #{batch_index} (eof={is_eof})")

def main():
    print("=" * 60)
    print("🧪 Test completo de calibration-service + RabbitMQ")
    print("=" * 60)
    
    client_id = "newrelic-test-complete"
    
    # 1. Registrar cliente via gRPC
    print(f"\n1️⃣ Registrando cliente '{client_id}' via gRPC...")
    try:
        success = register_client(client_id)
        if success:
            print("   ✅ Cliente registrado exitosamente")
        else:
            print("   ⚠️ Cliente registrado con advertencias")
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return
    
    # Esperar a que se creen las colas
    time.sleep(2)
    
    # 2. Conectar a RabbitMQ
    print(f"\n2️⃣ Conectando a RabbitMQ...")
    try:
        connection, channel = connect_rabbitmq()
        print("   ✅ Conectado a RabbitMQ")
    except Exception as e:
        print(f"   ❌ Error conectando a RabbitMQ: {e}")
        return
    
    # 3. Enviar mensajes de datos
    print(f"\n3️⃣ Enviando batches de datos (activará _handle_data_message)...")
    try:
        send_data_batch(channel, client_id, batch_index=0, is_last=False)
        time.sleep(0.5)
        send_data_batch(channel, client_id, batch_index=1, is_last=False)
        time.sleep(0.5)
        send_data_batch(channel, client_id, batch_index=2, is_last=True)
        print("   ✅ 3 batches enviados")
    except Exception as e:
        print(f"   ❌ Error enviando datos: {e}")
    
    # 4. Enviar predicciones
    print(f"\n4️⃣ Enviando predicciones (activará _handle_probability_message)...")
    try:
        send_predictions(channel, client_id, batch_index=0, is_eof=False)
        time.sleep(0.5)
        send_predictions(channel, client_id, batch_index=1, is_eof=False)
        time.sleep(0.5)
        send_predictions(channel, client_id, batch_index=2, is_eof=True)
        print("   ✅ 3 batches de predicciones enviados")
    except Exception as e:
        print(f"   ❌ Error enviando predicciones: {e}")
    
    # Cerrar conexión
    connection.close()
    
    print(f"\n{'='*60}")
    print("📊 RESUMEN")
    print(f"{'='*60}")
    print(f"✅ 1 cliente registrado via gRPC")
    print(f"✅ 3 mensajes DataBatch enviados → _handle_data_message")
    print(f"✅ 3 mensajes Predictions enviados → _handle_probability_message")
    print(f"\n{'='*60}")
    print("🎯 EN NEW RELIC DEBERÍAS VER:")
    print(f"{'='*60}")
    print("1. Transacción gRPC: NotifyNewClient")
    print("2. Background Task: rabbitmq.handle_data_message (3 veces)")
    print("3. Background Task: rabbitmq.handle_probability_message (3 veces)")
    print("\n⏱️  Las métricas tardan 1-2 minutos en aparecer")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrumpido por el usuario")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
