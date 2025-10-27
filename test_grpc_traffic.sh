#!/bin/bash
# Script para generar tráfico gRPC al calibration-service
# Usa grpcurl para hacer llamadas desde fuera del contenedor

echo "=========================================="
echo "Generando tráfico gRPC para calibration-service"
echo "=========================================="

BASE_URL="localhost:50052"

# Verificar si grpcurl está instalado
if ! command -v grpcurl &> /dev/null; then
    echo "⚠️  grpcurl no está instalado."
    echo ""
    echo "Para instalar en Ubuntu/Debian:"
    echo "  sudo apt install grpcurl"
    echo ""
    echo "O descárgalo de: https://github.com/fullstorydev/grpcurl/releases"
    echo ""
    echo "Alternativa: Usar connection-service para generar tráfico"
    echo "  1. Levanta connection-service"
    echo "  2. Crea un usuario y conéctalo"
    echo "  3. connection-service llamará a NotifyNewClient()"
    exit 1
fi

echo ""
echo "✓ grpcurl encontrado"
echo "✓ Servidor: $BASE_URL"
echo ""

# Contador de requests
total=0
success=0

# Test 1: HealthCheck (10 requests)
echo "=========================================="
echo "TEST 1: HealthCheck (10 requests)"
echo "=========================================="
for i in {1..10}; do
    echo -n "Request $i... "
    response=$(grpcurl -plaintext -d '{}' $BASE_URL ClientNotificationService/HealthCheck 2>&1)
    
    if echo "$response" | grep -q "SERVING\|OK"; then
        echo "✅ OK"
        ((success++))
    else
        echo "❌ Error"
    fi
    ((total++))
    sleep 0.5
done

echo ""
echo "=========================================="
echo "TEST 2: NotifyNewClient (5 requests)"
echo "=========================================="

# Test 2: NotifyNewClient (5 clientes)
for i in {1..5}; do
    client_id="test-client-$i"
    echo -n "Registering $client_id... "
    
    response=$(grpcurl -plaintext -d "{\"client_id\": \"$client_id\"}" \
        $BASE_URL ClientNotificationService/NotifyNewClient 2>&1)
    
    if echo "$response" | grep -q "OK\|Successfully"; then
        echo "✅ OK"
        ((success++))
    else
        echo "❌ Error"
    fi
    ((total++))
    sleep 1
done

echo ""
echo "=========================================="
echo "📊 RESUMEN"
echo "=========================================="
echo "Total requests: $total"
echo "Exitosos: $success"
echo "Fallidos: $((total - success))"
echo ""
echo "=========================================="
echo "🎯 PRÓXIMOS PASOS"
echo "=========================================="
echo ""
echo "1. Ve a New Relic: https://one.newrelic.com/"
echo "2. Busca 'calibration-service' en APM & Services"
echo "3. Deberías ver:"
echo "   • Transacciones gRPC"
echo "   • Response times"
echo "   • Throughput"
echo ""
echo "⏱️  Las métricas tardan 1-2 minutos en aparecer"
echo "=========================================="
