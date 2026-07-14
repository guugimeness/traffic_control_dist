FROM python:3.11-slim

WORKDIR /app

# Instala dependências
COPY subscriber/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código do subscriber
COPY subscriber/subscriber.py .

# Copia o módulo de recuperação (Módulo 4)
COPY recover ./recover

# Executa o subscriber
CMD ["python", "-u", "subscriber.py"]
