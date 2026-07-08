import pika
import json
import time
import threading
import random
import sys
import os

# Configurações do Broker via variáveis de ambiente
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'guest')
SENSOR_ID = os.getenv('SENSOR_ID', str(random.randint(1, 100)))

class TrafficSensor:
    def __init__(self, sensor_id):
        self.sensor_id = sensor_id
        
        # Relógio Vetorial (Ordenação Causal)
        self.vector_clock = {self.sensor_id: 0}
        
        # Simulação de Deriva de Relógio Físico (Restrição C)
        self.drift_rate = random.uniform(0.90, 1.10) 
        self.startup_time = time.time()
        self.sync_offset = 0.0 
        
        # Conexão AMQP
        self.connection = None
        self.channel = None
        
        print(f"[INIT] Sensor {self.sensor_id} inicializado com Deriva de {self.drift_rate:.2f}x")

    def local_physical_time(self):
        """Calcula o tempo físico simulando a deriva de hardware, corrigido pelo offset."""
        elapsed_real = time.time() - self.startup_time
        drifted_time = self.startup_time + (elapsed_real * self.drift_rate)
        return drifted_time + self.sync_offset

    def connect_rabbitmq(self):
        """Estabelece conexão resiliente com o RabbitMQ, lidando com injeção de caos."""
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        # Heartbeat alto e timeouts relaxados para sobreviver a injeção de latência brutal
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST, 
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300,
            retry_delay=5,
            connection_attempts=10
        )
        
        while True:
            try:
                print(f"[REDE] Tentando conectar ao Broker {RABBITMQ_HOST}...")
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                
                # Declaração das exchanges e filas
                self.channel.exchange_declare(exchange='traffic_data', exchange_type='fanout')
                self.channel.queue_declare(queue='time_requests')
                self.channel.queue_declare(queue=f'time_responses_{self.sensor_id}')
                
                print("[REDE] Conectado com sucesso ao RabbitMQ!")
                break
            except Exception as e:
                print(f"[FALHA] Erro crítico de conexão de rede ({type(e).__name__}). Retentando em 5s...")
                time.sleep(5)

    def cristian_time_server_worker(self):
        """Thread exclusiva do Sensor '0' para atuar como Servidor NTP distribuído."""
        def on_time_request(ch, method, properties, body):
            req = json.loads(body)
            client_id = req['client_id']
            server_time = time.time() 
            
            response = json.dumps({
                'server_time': server_time,
                'client_id': client_id
            })
            
            ch.basic_publish(
                exchange='',
                routing_key=f'time_responses_{client_id}',
                body=response
            )
            print(f"[NTP-SERVER] Respondeu requisição do Sensor {client_id} com T_srv={server_time:.3f}")

        try:
            self.channel.basic_consume(queue='time_requests', on_message_callback=on_time_request, auto_ack=True)
            print("[NTP-SERVER] Servidor de Tempo (Algoritmo de Cristian) rodando no Sensor 0...")
            self.channel.start_consuming()
        except Exception as e:
            print(f"[NTP-SERVER-ERRO] Falha na thread do servidor de tempo: {e}")

    def sync_physical_clock_cristian(self):
        """Solicita o tempo ao Servidor e ajusta o offset local (Algoritmo de Cristian)."""
        while True:
            time.sleep(15) # Sincroniza a cada 15 segundos
            
            if not self.channel or self.channel.is_closed:
                continue

            t0 = self.local_physical_time()
            request_payload = json.dumps({'client_id': self.sensor_id})
            
            try:
                self.channel.basic_publish(exchange='', routing_key='time_requests', body=request_payload)
                print(f"[CRISTIAN] Sensor {self.sensor_id} enviou requisição de tempo (T0={t0:.3f})")
                
                method, properties, body = next(self.channel.consume(queue=f'time_responses_{self.sensor_id}', inactivity_timeout=10))
                
                if body:
                    self.channel.basic_ack(method.delivery_tag)
                    res = json.loads(body)
                    t1 = res['server_time']
                    t2 = self.local_physical_time()
                    
                    rtt = t2 - t0
                    estimated_server_time = t1 + (rtt / 2)
                    correction = estimated_server_time - t2
                    self.sync_offset += correction
                    
                    print(f"[CRISTIAN-SYNC] RTT: {rtt:.3f}s | Correção: {correction:.3f}s | Novo Offset: {self.sync_offset:.3f}s")
                else:
                    print("[CRISTIAN-FALHA] Timeout aguardando servidor de tempo (Rede instável).")
                    
            except Exception as e:
                print(f"[CRISTIAN-ERRO] Falha ao sincronizar relógio: {e}")

    def publish_data(self):
        """Loop principal de envio de dados de tráfego."""
        while True:
            try:
                if self.connection is None or self.connection.is_closed:
                    self.connect_rabbitmq()
                
                # 1. Atualização do Relógio Vetorial (Evento local)
                self.vector_clock[self.sensor_id] += 1
                
                # 2. Geração do Dado Simulado
                fluxo = random.randint(5, 50)
                timestamp = self.local_physical_time()
                
                payload = {
                    "sensor_id": self.sensor_id,
                    "fluxo_veiculos": fluxo,
                    "vector_clock": self.vector_clock.copy(),
                    "physical_timestamp": timestamp
                }
                
                # 3. Publicação no Broker
                self.channel.basic_publish(
                    exchange='traffic_data',
                    routing_key='',
                    body=json.dumps(payload),
                    properties=pika.BasicProperties(
                        delivery_mode=2 # Mensagem persistente
                    )
                )
                
                print(f"[PUBLISH] Sensor: {self.sensor_id} | Fluxo: {fluxo} | Vetor: {self.vector_clock} | Tempo Físico: {timestamp:.3f}")
                time.sleep(random.uniform(2, 5)) 
                
            except (pika.exceptions.ConnectionClosedByBroker, pika.exceptions.AMQPChannelError, pika.exceptions.AMQPConnectionError) as e:
                print(f"[ERRO-REDE] Conexão perdida durante publicação: {e}")
                self.connection = None 
                time.sleep(2)

if __name__ == '__main__':
    while True:
        try:
            sensor = TrafficSensor(SENSOR_ID)
            sensor.connect_rabbitmq()
            
            # Inicializa a thread de acordo com o papel do Sensor
            if SENSOR_ID == "0":
                ntp_thread = threading.Thread(target=sensor.cristian_time_server_worker, daemon=True)
                ntp_thread.start()
            else:
                sync_thread = threading.Thread(target=sensor.sync_physical_clock_cristian, daemon=True)
                sync_thread.start()
                
            # Trava a execução no loop de publicação
            sensor.publish_data()
            
        # Captura genérica blinda o container inteiro contra a falha interna da biblioteca Pika sob condições de caos extremo
        except Exception as e:
            print(f"\n[CAOS DETECTADO] Exceção crítica fatal não tratada na biblioteca Pika: {e}")
            print("[RECOVERY] Reiniciando todo o contexto do sensor em 5 segundos...\n")
            time.sleep(5)
            continue # Reinicia o loop principal, criando uma nova instância limpa e sobrevivendo à falha
            
        except KeyboardInterrupt:
            print("\n[SHUTDOWN] Sensor encerrado pelo usuário.")
            sys.exit(0)