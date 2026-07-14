FROM python:3.11-slim

WORKDIR /app

# Instala as dependências do publisher
COPY publisher/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código do publisher
COPY publisher/publisher.py .

# Copia o pacote compartilhado do Módulo 4
COPY recover ./recover

# Executa o produtor com logs em tempo real
CMD ["python", "-u", "publisher.py"]
