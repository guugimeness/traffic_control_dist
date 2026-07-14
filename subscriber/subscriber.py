import pika
import json
import time
import threading
import os
import sys

# Configuração via variáveis de ambiente
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'guest')
NODE_ID       = int(os.getenv('NODE_ID', '1'))
TOTAL_NODES   = int(os.getenv('TOTAL_NODES', '4'))

# Timeouts e intervalos (em segundos)
HEARTBEAT_INTERVAL  = 5    # Líder envia heartbeat a cada 5s
HEARTBEAT_TIMEOUT   = 25   # Follower inicia eleição se não receber em 25s (Tolera latência unidirecional alta)
ELECTION_TIMEOUT    = 20   # Aguarda OK de vizinhos antes de se declarar líder (RTT máximo sob caos)
QUORUM_TIMEOUT      = 20   # Aguarda ACKs de quórum antes de tentar eleição (RTT máximo sob caos)

# Estados possíveis do nó
FOLLOWER   = "FOLLOWER"    # Estado padrão: recebe heartbeats do líder e processa dados de tráfego normalmente
CANDIDATE  = "CANDIDATE"   # Eleição em andamento: aguardando OK de nós com ID maior ou confirmação de quórum
LEADER     = "LEADER"      # Coordenador único: envia heartbeats e tem autoridade sobre os semáforos
SAFE_MODE  = "SAFE_MODE"   # Partição detectada: quórum insuficiente, nó congela comandos para evitar split-brain


class SmartTrafficLight:
    """
    Classe que implementa um semáforo inteligente (subscriber).

    Responsabilidades:
    - Consumir dados de tráfego dos sensores (exchange 'traffic_data').
    - Reordenar mensagens com base nos Relógios Vetoriais (ordenação causal).
    - Executar o Algoritmo de Eleição de Bully com verificação de Quórum.
    - Prevenir split-brain: sem maioria estrita -> entra em SAFE_MODE.
    """

    def __init__(self):
        self.node_id     = NODE_ID
        self.total_nodes = TOTAL_NODES
        self.quorum      = (self.total_nodes // 2) + 1  # maioria estrita

        # Estado de eleição
        self.state        = FOLLOWER
        self.leader_id    = None
        self.state_lock   = threading.Lock()

        # Relógio vetorial local (usado para merge ao processar mensagens)
        self.local_vc     = {}
        self.vc_lock      = threading.Lock()

        # Buffer causal: lista de payloads pendentes
        self.causal_buffer = []
        self.buffer_lock   = threading.Lock()

        # Controle de heartbeat
        self.last_heartbeat   = time.time()
        self.hb_lock          = threading.Lock()
        self.election_in_progress = False
        self.election_lock    = threading.Lock()

        # Respostas pendentes de eleição/quórum
        self.ok_received      = False
        self.quorum_acks      = set()
        self.quorum_ack_lock  = threading.Lock()

        # Conexões AMQP (canal separado por thread para thread-safety)
        self.connection       = None
        self.channel          = None          # canal principal (consume)
        self.pub_connection   = None
        self.pub_channel      = None          # canal de publicação
        # pika.BlockingConnection NAO e thread-safe: toda publicação
        # deve ser serializada com este lock
        self.pub_lock         = threading.Lock()

        print(f"[INIT] Semáforo {self.node_id} inicializado | Total de nós: {self.total_nodes} | Quórum necessário: {self.quorum}")

    # Conexão com RabbitMQ
    def _build_params(self):
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        return pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300,
            retry_delay=5,
            connection_attempts=10
        )

    def connect_rabbitmq(self):
        # Estabelece conexão resiliente (canal principal + canal de publicação)
        while True:
            try:
                print(f"[REDE] Nó {self.node_id} tentando conectar ao Broker {RABBITMQ_HOST}...")
                params = self._build_params()

                # Canal principal — para consumo (blocking)
                self.connection = pika.BlockingConnection(params)
                self.channel    = self.connection.channel()

                # Canal secundário — para publicações seguras em threads
                self.pub_connection = pika.BlockingConnection(params)
                self.pub_channel    = self.pub_connection.channel()

                self._declare_topology(self.channel)
                self._declare_exchanges(self.pub_channel)

                print(f"[REDE] Nó {self.node_id} conectado ao RabbitMQ com sucesso!")
                break
            except Exception as e:
                print(f"[FALHA] Erro de conexão ({type(e).__name__}): {e}. Retentando em 5s...")
                time.sleep(5)

    def _declare_topology(self, channel):
        # Declara exchanges, filas duraveis e filas exclusivas de consumo
        # Deve ser chamado APENAS no canal principal (self.channel)

        # Exchange fanout de dados de tráfego (criada pelos publishers)
        channel.exchange_declare(exchange='traffic_data', exchange_type='fanout')

        # Exchange fanout de eleição (broadcast entre subscribers)
        channel.exchange_declare(exchange='election', exchange_type='fanout')

        # Fila durável para mensagens diretas a este nó (caixa postal)
        channel.queue_declare(queue=f'sub_{self.node_id}', durable=True)

        # Fila durável desta CONEXÃO para receber dados de tráfego
        # Assim, se a conexão cair pelo caos, o RabbitMQ guarda as mensagens até o nó reconectar
        self.traffic_queue = f'traffic_data_sub_{self.node_id}'
        channel.queue_declare(queue=self.traffic_queue, durable=True)
        channel.queue_bind(exchange='traffic_data', queue=self.traffic_queue)

        # Fila durável desta CONEXÃO para receber broadcasts de eleição
        self.election_queue = f'election_data_sub_{self.node_id}'
        channel.queue_declare(queue=self.election_queue, durable=True)
        channel.queue_bind(exchange='election', queue=self.election_queue)

    def _declare_exchanges(self, channel):
        # Declara apenas as exchanges no canal de publicação
        # O pub_channel não consome filas, portanto NAO deve declarar filas exclusive=True
        channel.exchange_declare(exchange='traffic_data', exchange_type='fanout')
        channel.exchange_declare(exchange='election', exchange_type='fanout')

    # Publicação de mensagens de controle
    def _publish_control(self, routing_key, payload):
        # Publica mensagem de controle direta para a caixa postal de outro nó
        # Lock obrigatório: pub_channel é acessado por várias threads (heartbeat, eleição, callbacks)
        with self.pub_lock:
            try:
                self.pub_channel.basic_publish(
                    exchange='',
                    routing_key=routing_key,
                    body=json.dumps(payload)
                )
            except Exception as e:
                print(f"[PUB-ERRO] Falha ao publicar mensagem de controle: {e}")

    def _broadcast_election(self, payload):
        # Broadcast de mensagem de eleição para todos os nós via fanout
        # Lock obrigatório: mesmo motivo de _publish_control
        with self.pub_lock:
            try:
                self.pub_channel.basic_publish(
                    exchange='election',
                    routing_key='',
                    body=json.dumps(payload)
                )
            except Exception as e:
                print(f"[PUB-ERRO] Falha ao broadcast de eleição: {e}")

    # Ordenação Causal (Relógio Vetorial)
    def _merge_vc(self, remote_vc):
        # Atualiza o relógio vetorial local com merge do relógio remoto
        with self.vc_lock:
            for key, val in remote_vc.items():
                self.local_vc[key] = max(self.local_vc.get(key, 0), val)

    def _is_causally_ready(self, msg_vc, sender_id):
        """
        Verifica se a mensagem pode ser processada agora.

        Uma mensagem M de sensor S com vetor V pode ser processada quando,
        para todo sensor X != S: local_vc[X] >= V[X].
        Para o próprio remetente: local_vc[S] >= V[S] - 1.
        """
        with self.vc_lock:
            for node, clock in msg_vc.items():
                if node == sender_id:
                    if self.local_vc.get(node, 0) < clock - 1:
                        return False
                else:
                    if self.local_vc.get(node, 0) < clock:
                        return False
        return True

    def _try_flush_buffer(self):
        """
        Tenta processar mensagens do buffer causal que já estejam desbloqueadas.
        Continua em loop até nenhuma mensagem poder ser processada.
        """
        flushed = True
        while flushed:
            flushed = False
            with self.buffer_lock:
                for msg in list(self.causal_buffer):
                    sender = str(msg['sensor_id'])
                    vc     = msg['vector_clock']
                    if self._is_causally_ready(vc, sender):
                        self.causal_buffer.remove(msg)
                        self._process_traffic(msg)
                        self._merge_vc(vc)
                        flushed = True
                        break  # recomeça a varredura com buffer atualizado

    def _buffer_traffic_message(self, msg):
        # Adiciona mensagem ao buffer causal e tenta desbloquear pendentes.
        # Em SAFE_MODE, não podemos pular a mensagem, senão causamos um buraco eterno
        # no relógio vetorial. Vamos processá-la normalmente para atualizar o estado interno,
        # mas a ação no mundo real (ex: mudar a luz do semáforo) é retida no _process_traffic.
        
        sender = str(msg['sensor_id'])
        vc = msg['vector_clock']

        with self.vc_lock:
            if self.local_vc.get(sender, 0) >= vc.get(sender, 0):
                print(f"[DESCARTADO] Mensagem antiga/duplicada do Sensor {sender} (VC: {vc})")
                return

        with self.buffer_lock:
            # Não adicionar se já existe no buffer
            if any(m['sensor_id'] == msg['sensor_id'] and m['vector_clock'] == msg['vector_clock'] for m in self.causal_buffer):
                return
            self.causal_buffer.append(msg)
            buf_size = len(self.causal_buffer)

        print(f"[CAUSAL-BUFFER] Mensagem do Sensor {sender} "
              f"(VC: {vc}) adicionada ao buffer. "
              f"Buffer atual: {buf_size} mensagem(ns).")

        self._try_flush_buffer()

    def _process_traffic(self, msg):
        # Efetivamente processa um dado de tráfego na ordem causal correta.
        with self.state_lock:
            state = self.state
            leader = self.leader_id

        if state == SAFE_MODE:
            # Mantém o relógio rodando, mas atua com segurança (sem alterar semáforos reais)
            print(f"[PROCESSADO-SAFE] Sensor: {msg['sensor_id']} | "
                  f"VC: {msg['vector_clock']} | "
                  f"(Luzes congeladas para evitar acidentes)")
        else:
            role = f"LÍDER" if state == LEADER else f"FOLLOWER (líder={leader})"
            print(f"[PROCESSADO] [{role}] Sensor: {msg['sensor_id']} | "
                  f"Fluxo: {msg['fluxo_veiculos']} veíc/min | "
                  f"VC: {msg['vector_clock']} | "
                  f"Tempo Físico: {msg['physical_timestamp']:.3f}")

    # Algoritmo de Eleição de Bully
    def _check_quorum(self):
        """
        Verifica se há nós suficientes para formar quórum antes de iniciar eleição.
        Retorna True se quórum foi atingido, False caso contrário.
        """
        print(f"[QUÓRUM] Nó {self.node_id} verificando quórum "
              f"(necessário: {self.quorum}/{self.total_nodes})...")

        with self.quorum_ack_lock:
            self.quorum_acks = {self.node_id}  # conta a si mesmo (usando Set)

        # Broadcast QUORUM_CHECK
        self._broadcast_election({
            'type':      'QUORUM_CHECK',
            'sender_id': self.node_id
        })

        # Aguarda respostas
        time.sleep(QUORUM_TIMEOUT)

        with self.quorum_ack_lock:
            alive = len(self.quorum_acks)

        print(f"[QUÓRUM] {alive}/{self.total_nodes} nós responderam.")

        if alive >= self.quorum:
            print(f"[QUÓRUM] Quórum atingido ({alive} >= {self.quorum}). Prosseguindo com eleição.")
            return True
        else:
            print(f"[QUÓRUM] Quórum NAO atingido ({alive} < {self.quorum}). Entrando em SAFE_MODE.")
            self._enter_safe_mode()
            return False

    def start_election(self):
        # Dispara o algoritmo de Bully após confirmar quórum
        with self.election_lock:
            if self.election_in_progress:
                return
            self.election_in_progress = True

        print(f"[ELEIÇÃO] Nó {self.node_id} iniciando processo de eleição...")

        # Etapa 1: verificação de quórum
        if not self._check_quorum():
            with self.election_lock:
                self.election_in_progress = False
            return

        # Etapa 2: Bully — envia ELECTION para nós com ID maior
        higher_nodes = [i for i in range(1, self.total_nodes + 1) if i > self.node_id]

        if not higher_nodes:
            # Sou o nó com maior ID -> me declaro líder imediatamente
            self._declare_leader()
            return

        self.ok_received = False
        for nid in higher_nodes:
            print(f"[ELEIÇÃO] Enviando ELECTION para nó {nid}...")
            self._publish_control(f'sub_{nid}', {
                'type':      'ELECTION',
                'sender_id': self.node_id
            })

        # Etapa 3: aguarda OK de algum nó com ID maior
        time.sleep(ELECTION_TIMEOUT)

        if not self.ok_received:
            # Nenhum nó com ID maior respondeu -> me declaro líder
            self._declare_leader()
        else:
            print(f"[ELEIÇÃO] OK recebido. Aguardando COORDINATOR...")
            with self.election_lock:
                self.election_in_progress = False

    def _declare_leader(self):
        # Declara-se como líder e notifica todos os outros nós.
        with self.state_lock:
            self.state     = LEADER
            self.leader_id = self.node_id

        print(f"[LÍDER] Nó {self.node_id} declarado LÍDER! Anunciando para todos os nós.")

        self._broadcast_election({
            'type':      'COORDINATOR',
            'sender_id': self.node_id
        })

        # Reinicia o envio de heartbeats
        hb_thread = threading.Thread(target=self._heartbeat_sender, daemon=True)
        hb_thread.start()

        with self.election_lock:
            self.election_in_progress = False

    def _enter_safe_mode(self):
        # Nó entra em modo de segurança por falta de quórum (partição de rede)
        with self.state_lock:
            self.state     = SAFE_MODE
            self.leader_id = None
        print(f"[SAFE-MODE] Nó {self.node_id} entrou em SAFE_MODE. "
              f"Sem quórum detectado — possível partição de rede. "
              f"Nenhum comando de tráfego será emitido.")

    # Handlers de mensagens de controle (eleição)
    def _handle_election_msg(self, msg):
        # Processa mensagens recebidas da exchange de eleição
        mtype     = msg.get('type')
        sender_id = msg.get('sender_id')

        # Ignora mensagens próprias
        if sender_id == self.node_id:
            return

        if mtype == 'QUORUM_CHECK':
            # Responde ao solicitante confirmando que está vivo
            self._publish_control(f'sub_{sender_id}', {
                'type':      'QUORUM_ACK',
                'sender_id': self.node_id
            })

        elif mtype == 'COORDINATOR':
            # Um nó de maior prioridade se declarou líder
            with self.state_lock:
                self.state     = FOLLOWER
                self.leader_id = sender_id
            with self.hb_lock:
                self.last_heartbeat = time.time()
            with self.election_lock:
                self.election_in_progress = False
            print(f"[LÍDER] Nó {sender_id} reconhecido como novo LÍDER.")

        elif mtype == 'HEARTBEAT':
            with self.hb_lock:
                self.last_heartbeat = time.time()
            with self.state_lock:
                if self.state == SAFE_MODE:
                    self.state     = FOLLOWER
                    self.leader_id = sender_id
                    print(f"[SAFE-MODE] Heartbeat recebido do líder {sender_id}. Saindo do SAFE_MODE.")

    def _handle_direct_msg(self, msg):
        # Processa mensagens diretas recebidas na caixa postal do nó
        mtype     = msg.get('type')
        sender_id = msg.get('sender_id')

        if mtype == 'QUORUM_ACK':
            with self.quorum_ack_lock:
                self.quorum_acks.add(sender_id)
                current_total = len(self.quorum_acks)
            print(f"[QUÓRUM] ACK recebido do nó {sender_id}. Total: {current_total}")

        elif mtype == 'ELECTION':
            # Recebi ELECTION de um nó com ID menor -> envio OK e inicio minha eleição
            print(f"[ELEIÇÃO] ELECTION recebido do nó {sender_id}. Enviando OK e iniciando contra-eleição.")
            self._publish_control(f'sub_{sender_id}', {
                'type':      'OK',
                'sender_id': self.node_id
            })
            # Disparo minha própria eleição em thread separada
            threading.Thread(target=self.start_election, daemon=True).start()

        elif mtype == 'OK':
            # Um nó com ID maior está vivo -> paro de me declarar líder
            print(f"[ELEIÇÃO] OK recebido do nó {sender_id}. Aguardando COORDINATOR.")
            self.ok_received = True

    # Heartbeat
    def _heartbeat_sender(self):
        # Thread do líder: envia HEARTBEAT periódico para todos os nós
        print(f"[HEARTBEAT] Nó {self.node_id} (LÍDER) iniciando envio de heartbeats a cada {HEARTBEAT_INTERVAL}s.")
        while True:
            with self.state_lock:
                if self.state != LEADER:
                    print(f"[HEARTBEAT] Nó {self.node_id} não é mais líder. Parando heartbeat sender.")
                    break
            self._broadcast_election({
                'type':      'HEARTBEAT',
                'sender_id': self.node_id
            })
            time.sleep(HEARTBEAT_INTERVAL)

    def _heartbeat_monitor(self):
        # Thread dos followers: detecta ausência de heartbeat e dispara eleição.
        print(f"[HEARTBEAT] Nó {self.node_id} monitorando heartbeat do líder (timeout={HEARTBEAT_TIMEOUT}s).")
        while True:
            time.sleep(HEARTBEAT_TIMEOUT / 3)  # verifica 3x no intervalo de timeout
            with self.state_lock:
                current_state = self.state
            if current_state == LEADER:
                continue
            with self.hb_lock:
                elapsed = time.time() - self.last_heartbeat
            if elapsed > HEARTBEAT_TIMEOUT:
                print(f"[HEARTBEAT] Timeout! Sem heartbeat há {elapsed:.1f}s. "
                      f"Líder {self.leader_id} presumido morto.")
                with self.election_lock:
                    already = self.election_in_progress
                if not already:
                    threading.Thread(target=self.start_election, daemon=True).start()

    # Loop de consumo principal
    def _on_traffic_message(self, ch, method, properties, body):
        # Callback para mensagens de dados de tráfego
        try:
            msg = json.loads(body)
            self._buffer_traffic_message(msg)
        except Exception as e:
            print(f"[ERRO] Falha ao processar mensagem de tráfego: {e}")
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def _on_election_message(self, ch, method, properties, body):
        # Callback para broadcasts de eleição.
        try:
            msg = json.loads(body)
            self._handle_election_msg(msg)
        except Exception as e:
            print(f"[ERRO] Falha ao processar mensagem de eleição: {e}")
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def _on_direct_message(self, ch, method, properties, body):
        # Callback para mensagens diretas na caixa postal do nó
        try:
            msg = json.loads(body)
            self._handle_direct_msg(msg)
        except Exception as e:
            print(f"[ERRO] Falha ao processar mensagem direta: {e}")
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        # Ponto de entrada principal: conecta, registra consumers e bloqueia.
        self.connect_rabbitmq()

        # Registra os 3 consumers no canal principal
        self.channel.basic_consume(
            queue=self.traffic_queue,
            on_message_callback=self._on_traffic_message
        )
        self.channel.basic_consume(
            queue=self.election_queue,
            on_message_callback=self._on_election_message
        )
        self.channel.basic_consume(
            queue=f'sub_{self.node_id}',
            on_message_callback=self._on_direct_message
        )

        # Inicia threads auxiliares
        threading.Thread(target=self._heartbeat_monitor, daemon=True).start()

        # Aguarda um pouco para todos os nós subirem e então inicia eleição inicial
        threading.Thread(target=self._initial_election, daemon=True).start()

        print(f"[RUN] Nó {self.node_id} aguardando mensagens...")
        self.channel.start_consuming()

    def _initial_election(self):
        # Dispara eleição inicial após todos os containers subirem.
        startup_wait = 10 + (self.node_id * 1)  # escalonado para evitar colisão
        print(f"[INIT] Aguardando {startup_wait}s para eleição inicial...")
        time.sleep(startup_wait)
        with self.state_lock:
            already_has_leader = self.leader_id is not None
        if not already_has_leader:
            print(f"[INIT] Nenhum líder detectado. Iniciando eleição inicial.")
            self.start_election()

if __name__ == '__main__':
    while True:
        try:
            node = SmartTrafficLight()
            node.run()
        except KeyboardInterrupt:
            print(f"\n[SHUTDOWN] Nó {NODE_ID} encerrado pelo usuário.")
            sys.exit(0)
        except Exception as e:
            print(f"\n[CAOS DETECTADO] Exceção crítica: {e}")
            print("[RECOVERY] Reiniciando nó em 5 segundos...\n")
            time.sleep(5)
