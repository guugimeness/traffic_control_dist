# Sistemas Distribuídos - Traffic Control

O sistema simula um ambiente de controle de tráfego utilizando comunicação distribuída via RabbitMQ, além de mecanismos de Chaos Engineering para validação de tolerância a falhas.

## Módulo 1

Este módulo é responsável pela infraestrutura do sistema distribuído de controle de tráfego.

As principais responsabilidades são:

* Configuração do ambiente utilizando Docker Compose;
* Disponibilização do RabbitMQ como broker de mensagens;
* Configuração da rede Docker utilizada pelos demais módulos;
* Implementação do container de Chaos Engineering para simulação de falhas de rede.

### Como executar

```bash
docker compose up --build
```

Para encerrar os containers:

```bash
docker compose down
```

### RabbitMQ

Para acessar o RabbitMQ:

```
http://localhost:15672
```

Credenciais:
- User: guest
- Password :guest

### Estrutura da Rede

Os módulos devem utilizar a rede:

```
trafego
```

### Chaos Engineering

O container `chaos` utiliza o utilitário Linux `tc/netem` para simular degradações de rede durante a execução do sistema.

Essas alterações permitem validar o comportamento dos demais módulos em condições adversas de comunicação.

Atualmente são simulados:
* Latência aleatória entre **10 ms** e **4000 ms**, através do `netem delay`
* Perda de **5%** dos pacotes, através do `loss 5%`

Essas alterações são aplicadas automaticamente enquanto o sistema está em execução.

## Módulo 2

Este módulo aborda os Produtores (Publishers) e Tempo Lógico. 

As principais responsabilidades da implementação são:

* *Sensores de Tráfego:* Desenvolvimento dos nós sensores de tráfego em Python.
* *Simulação de Dados:* O sistema simula múltiplas instâncias que enviam dados de fluxo de veículos de forma autônoma.
* *Ordenação Causal:* Como a rede será instável, implementa Relógios Vetoriais anexados a cada mensagem enviada, garantindo a ordenação causal dos eventos no destino.
* *Deriva de Relógio Físico Isolado (Restrição C):* Cada contêiner de sensor introduzirá uma taxa de erro artificial em seu relógio de sistema (ex: acelerar ou atrasar artificialmente em relação ao tempo real do host). Como é proibido consultar servidores NTP externos da internet, foi implementado um mecanismo nativo de correção.
* *Sincronização de Tempo Distribuída:* O módulo codifica manualmente uma variação do Algoritmo de Cristian utilizando o próprio middleware Pub-Sub para calcular o offset de tempo de cada contêiner e sincronizar os relógios internamente de forma distribuída.
* *Resiliência Extrema e Auto-Healing:* A arquitetura de conexão inclui tratamento avançado de exceções de rede e socket. Isso permite blindar o sistema contra as corrupções de frames e quebras de AMQP geradas pelos picos de latência (até 4000ms), recuperando e reiniciando a publicação de dados automaticamente.

## Módulo 3

Este módulo implementa os **Semáforos Inteligentes** — os nós consumidores (Subscribers) do sistema.

As principais responsabilidades são:

* **Consumo de Dados de Tráfego:** Cada instância assina a exchange `traffic_data` e recebe dados dos sensores em tempo real.
* **Ordenação Causal (Restrição A):** As mensagens chegam fora de ordem devido à rede instável. O módulo implementa um **buffer causal** baseado nos Relógios Vetoriais enviados pelos publishers. Uma mensagem só é processada quando todos os eventos causalmente anteriores já foram recebidos.
* **Eleição de Líder — Algoritmo de Bully (Restrição B):** O controle dos semáforos exige um único coordenador. O módulo implementa o Algoritmo de Bully: ao detectar a ausência do líder (via timeout de heartbeat), o nó dispara uma eleição, enviando mensagens `ELECTION` para nós de maior ID. O nó de maior ID ativo vence e transmite `COORDINATOR` para todos.
* **Quórum Absoluto e Prevenção de Split-Brain (Restrição B):** Antes de qualquer eleição, o nó executa um `QUORUM_CHECK`: verifica quantos dos 4 nós estão ativos. Se menos de 3 nós (maioria estrita de 4) responderem, o nó entra em **SAFE_MODE** e para de emitir comandos de tráfego — evitando a existência de múltiplos líderes em caso de partição de rede.
* **Heartbeats:** O líder eleito envia `HEARTBEAT` a cada 5 segundos. Os followers monitoram o timeout (15s) e disparam nova eleição se o líder sumir.

### Topologia de Instâncias

São executadas **4 instâncias** (`sub_1` a `sub_4`), com IDs de 1 a 4. O quórum exige **3 nós ativos** (maioria estrita). Em uma partição 2-2, nenhum lado tem maioria e ambos entram em SAFE_MODE.

## Módulo 4

Este módulo implementa os mecanismos de Estado, Tolerância a Falhas e Recuperação do sistema distribuído.

As principais responsabilidades são:

Detecção de Falhas: Os subscribers monitoram continuamente a presença do líder através de mensagens de Heartbeat. Caso o heartbeat deixe de ser recebido dentro do tempo limite, uma nova eleição é iniciada automaticamente.
Persistência de Estado: Publishers e Subscribers realizam checkpoints locais periódicos, armazenando informações importantes como relógios vetoriais, estado do nó, mensagens pendentes e histórico de processamento.
Recuperação Automática: Em caso de falha abrupta (por exemplo, utilizando docker kill), o Docker reinicia automaticamente o contêiner. Durante a inicialização, o nó recupera seu último checkpoint válido e continua sua execução sem reiniciar completamente seu estado.
Processamento Seguro de Mensagens: Cada mensagem possui um identificador único, permitindo detectar mensagens duplicadas após uma recuperação e evitando reprocessamentos indevidos.
Filas Persistentes: As filas de dados dos subscribers são duráveis, permitindo que mensagens permaneçam disponíveis mesmo durante a reinicialização de um nó consumidor.
Persistência

Cada Publisher e Subscriber possui um volume Docker exclusivo para armazenamento dos checkpoints, garantindo que o estado seja preservado mesmo após a recriação do contêiner.

Teste de Recuperação

Para simular uma falha, basta finalizar qualquer publisher ou subscriber:

docker kill sub_2

ou

docker kill sensor_a

Como os serviços utilizam restart: always, o Docker reiniciará automaticamente o contêiner. Durante a inicialização, o nó restaurará seu último checkpoint e continuará a operação normalmente, preservando o estado previamente armazenado.
