#!/usr/bin/env python3
"""
Script simple para generar tráfico a calibration-service
mediante el flujo completo: crear usuario -> token -> conectar
"""
import requests
import time
import sys

USERS_SERVICE = "http://localhost:8000"
CONNECTION_SERVICE = "http://localhost:80"

def create_user(username, email):
    """Crear usuario en users-service"""
    response = requests.post(
        f"{USERS_SERVICE}/users/create",
        json={
            "username": username,
            "email": email,
            "inputs_format": "(1,28,28)",
            "outputs_format": "(10,)",
            "model_type": "mnist"
        }
    )
    if response.status_code == 201:
        return response.json()["client_id"]
    else:
        print(f"❌ Error creando usuario: {response.status_code}")
        print(response.text)
        return None

def create_token(user_id):
    """Crear token en connection-service"""
    response = requests.post(
        f"{CONNECTION_SERVICE}/tokens/create",
        json={"user_id": user_id}
    )
    # connection-service devuelve 500 pero con token válido
    if response.status_code in [200, 500]:
        data = response.json()
        token = data.get("token", "")
        if token:
            return token
    
    print(f"❌ Error creando token: {response.status_code}")
    print(response.text)
    return None

def connect_user(user_id, token):
    """Conectar usuario - ESTO LLAMA A calibration-service.NotifyNewClient()"""
    response = requests.post(
        f"{CONNECTION_SERVICE}/users/connect",
        json={"user_id": user_id, "token": token}
    )
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Error conectando usuario: {response.status_code}")
        print(response.text)
        return None

def main():
    print("=" * 60)
    print("Generando tráfico para calibration-service")
    print("=" * 60)
    
    success = 0
    total = 5
    
    for i in range(1, total + 1):
        print(f"\n{'='*60}")
        print(f"Cliente {i} de {total}")
        print(f"{'='*60}")
        
        # 1. Crear usuario
        email = f"testcalib{i}_{int(time.time())}@example.com"
        print(f"1. Creando usuario: {email}... ", end="", flush=True)
        user_id = create_user(f"Test Calibration {i}", email)
        
        if not user_id:
            continue
        print(f"✅ OK (ID: {user_id[:8]}...)")
        
        # 2. Crear token
        print(f"2. Creando token... ", end="", flush=True)
        token = create_token(user_id)
        
        if not token:
            continue
        print(f"✅ OK")
        
        # 3. Conectar usuario (llama a calibration-service)
        print(f"3. Conectando usuario... ", end="", flush=True)
        result = connect_user(user_id, token)
        
        if result and "client_id" in result:
            client_id = result["client_id"]
            print(f"✅ OK (client_id: {client_id})")
            success += 1
            
            # Pequeña pausa para que los logs se procesen
            time.sleep(1)
        else:
            print("❌ Error")
        
        time.sleep(2)
    
    print(f"\n{'='*60}")
    print("📊 RESUMEN")
    print(f"{'='*60}")
    print(f"Total: {total}")
    print(f"Exitosos: {success}")
    print(f"Fallidos: {total - success}")
    
    print(f"\n{'='*60}")
    print("🎯 VERIFICACIÓN")
    print(f"{'='*60}")
    print("\n1. Ve a New Relic: https://one.newrelic.com/")
    print("2. Busca 'calibration-service' en APM & Services")
    print("3. Deberías ver:")
    print(f"   • {success} transacciones gRPC: NotifyNewClient")
    print("   • Response times")
    print("   • Throughput")
    print("\n⏱️  Las métricas pueden tardar 1-2 minutos en aparecer")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrumpido por el usuario")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        sys.exit(1)
