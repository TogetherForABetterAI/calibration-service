#!/usr/bin/env python3
"""
Script para probar que calibration-service esté respondiendo
y generar tráfico gRPC para verificar en New Relic.
"""
import grpc
import sys
import time

# Importar los protobuf generados
sys.path.append('./src')
from pb.new_client_service import new_client_service_pb2, new_client_service_pb2_grpc


def test_health_check(channel):
    """Test HealthCheck endpoint"""
    print("🔍 Testeando HealthCheck...")
    stub = new_client_service_pb2_grpc.ClientNotificationServiceStub(channel)
    
    try:
        request = new_client_service_pb2.HealthCheckRequest()
        response = stub.HealthCheck(request)
        print(f"✅ HealthCheck - Status: {response.status}, Message: {response.message}")
        return True
    except grpc.RpcError as e:
        print(f"❌ HealthCheck failed: {e.code()} - {e.details()}")
        return False


def test_notify_new_client(channel, client_id):
    """Test NotifyNewClient endpoint"""
    print(f"\n📝 Testeando NotifyNewClient (client_id={client_id})...")
    stub = new_client_service_pb2_grpc.ClientNotificationServiceStub(channel)
    
    try:
        request = new_client_service_pb2.NewClientRequest(
            client_id=client_id
        )
        response = stub.NotifyNewClient(request)
        print(f"✅ NotifyNewClient - Status: {response.status}, Message: {response.message}")
        return True
    except grpc.RpcError as e:
        print(f"❌ NotifyNewClient failed: {e.code()} - {e.details()}")
        return False


def main():
    server_address = 'localhost:50052'
    print(f"🚀 Conectando a calibration-service en {server_address}...")
    print("=" * 60)
    
    # Crear canal gRPC
    channel = grpc.insecure_channel(server_address)
    
    try:
        # Esperar a que el servidor esté listo
        grpc.channel_ready_future(channel).result(timeout=10)
        print(f"✅ Conexión establecida con {server_address}\n")
    except grpc.FutureTimeoutError:
        print(f"❌ No se pudo conectar a {server_address}")
        sys.exit(1)
    
    success_count = 0
    total_requests = 0
    
    # Test 1: HealthCheck (10 requests)
    print("\n" + "=" * 60)
    print("TEST 1: HealthCheck Endpoint")
    print("=" * 60)
    for i in range(10):
        total_requests += 1
        if test_health_check(channel):
            success_count += 1
        time.sleep(0.5)
    
    # Test 2: NotifyNewClient (5 clientes diferentes)
    print("\n" + "=" * 60)
    print("TEST 2: NotifyNewClient Endpoint")
    print("=" * 60)
    for i in range(1, 6):
        total_requests += 1
        client_id = f"test-client-{i}"
        if test_notify_new_client(channel, client_id):
            success_count += 1
        time.sleep(1)
    
    # Resumen
    print("\n" + "=" * 60)
    print("📊 RESUMEN")
    print("=" * 60)
    print(f"Total de requests: {total_requests}")
    print(f"Exitosos: {success_count}")
    print(f"Fallidos: {total_requests - success_count}")
    print(f"Success rate: {(success_count/total_requests)*100:.1f}%")
    
    print("\n" + "=" * 60)
    print("🎯 PRÓXIMOS PASOS:")
    print("=" * 60)
    print("1. Ve a New Relic: https://one.newrelic.com/")
    print("2. Busca 'calibration-service' en APM & Services")
    print("3. Deberías ver:")
    print("   • Transacciones gRPC: HealthCheck, NotifyNewClient")
    print("   • Response times")
    print("   • Throughput (requests/min)")
    print("   • Error rate")
    print("\n⏱️  Las métricas pueden tardar 1-2 minutos en aparecer")
    print("=" * 60)
    
    channel.close()


if __name__ == "__main__":
    main()
