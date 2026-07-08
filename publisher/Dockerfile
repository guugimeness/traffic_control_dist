FROM python:3.9-slim
WORKDIR /app

# Copia o arquivo de dependências e instala
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código-fonte do sensor
COPY publisher.py .

# Executa o produtor com output desativado para logs em tempo real
CMD ["python", "-u", "publisher.py"]