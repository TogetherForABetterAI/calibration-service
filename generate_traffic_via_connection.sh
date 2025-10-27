#!/bin/bash
# Script para generar tráfico que active calibration-service
# Crea usuarios y los conecta, lo que genera llamadas gRPC a NotifyNewClient

echo "=========================================="
echo "Generando tráfico para calibration-service"
echo "vía connection-service"
echo "=========================================="

USERS_SERVICE="http://localhost:8000"
CONNECTION_SERVICE="http://localhost:80"

total=0
success=0

echo ""
echo "Generando 5 conexiones de clientes..."
echo ""

for i in {1..5}; do
    echo "=========================================="
    echo "Cliente $i de 5"
    echo "=========================================="
    
    # 1. Crear usuario
    echo -n "1. Creando usuario... "
    user_response=$(curl -s -X POST "$USERS_SERVICE/users" \
        -H "Content-Type: application/json" \
        -d "{\"name\": \"Test User $i\", \"email\": \"test$i@example.com\"}")
    
    user_id=$(echo $user_response | grep -o '"user_id":"[^"]*' | cut -d'"' -f4)
    
    if [ -n "$user_id" ]; then
        echo "✅ OK (ID: ${user_id:0:8}...)"
    else
        echo "❌ Error"
        continue
    fi
    
    # 2. Crear token
    echo -n "2. Generando token... "
    token_response=$(curl -s -X POST "$CONNECTION_SERVICE/tokens/create" \
        -H "Content-Type: application/json" \
        -d "{\"user_id\": \"$user_id\"}")
    
    token=$(echo $token_response | grep -o '"token":"[^"]*' | cut -d'"' -f4)
    
    if [ -n "$token" ]; then
        echo "✅ OK"
    else
        echo "❌ Error"
        continue
    fi
    
    # 3. Conectar usuario (ESTO LLAMA A CALIBRATION-SERVICE)
    echo -n "3. Conectando usuario... "
    connect_response=$(curl -s -X POST "$CONNECTION_SERVICE/users/connect" \
        -H "Content-Type: application/json" \
        -d "{\"user_id\": \"$user_id\", \"token\": \"$token\"}")
    
    if echo "$connect_response" | grep -q "client_id"; then
        client_id=$(echo $connect_response | grep -o '"client_id":"[^"]*' | cut -d'"' -f4)
        echo "✅ OK (client_id: $client_id)"
        ((success++))
        
        # Verificar logs de calibration-service
        echo "   📋 Verificando calibration-service..."
        sleep 1
        if docker logs calibration-service 2>&1 | tail -5 | grep -q "NotifyNewClient.*$client_id"; then
            echo "   ✅ calibration-service recibió la notificación!"
        fi
    else
        echo "❌ Error: $connect_response"
    fi
    
    ((total++))
    echo ""
    sleep 2
done

echo "=========================================="
echo "📊 RESUMEN"
echo "=========================================="
echo "Total de conexiones: $total"
echo "Exitosas: $success"
echo "Fallidas: $((total - success))"
echo ""

# Mostrar logs recientes de calibration-service
echo "=========================================="
echo "📋 Últimos logs de calibration-service:"
echo "=========================================="
docker logs calibration-service 2>&1 | tail -20

echo ""
echo "=========================================="
echo "🎯 PRÓXIMOS PASOS"
echo "=========================================="
echo ""
echo "1. Ve a New Relic: https://one.newrelic.com/"
echo "2. Busca 'calibration-service' en APM & Services"
echo "3. Deberías ver:"
echo "   • Transacción: NotifyNewClient"
echo "   • $success requests gRPC procesados"
echo "   • Response times y throughput"
echo ""
echo "⏱️  Las métricas tardan 1-2 minutos en aparecer"
echo "=========================================="
