FROM python:3.11-slim
WORKDIR /app

# Copia o arquivo de dependências e instala
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código-fonte
COPY subscriber.py .

# Executa o semáforo com output desativado para logs em tempo real
CMD ["python", "-u", "subscriber.py"]
