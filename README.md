# Sistemas Distribuídos - Traffic Contro

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

## Módulo 3

## Módulo 4
