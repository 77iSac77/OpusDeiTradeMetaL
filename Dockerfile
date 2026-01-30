# OpusDeiTradeMetaL - Dockerfile
# ==============================

FROM python:3.11-slim

# Metadados
LABEL maintainer="OpusDeiTrade"
LABEL version="1.0"
LABEL description="Bot de alertas de metais preciosos e industriais"

# Variáveis de ambiente
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Diretório de trabalho
WORKDIR /app

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements primeiro (cache de layer)
COPY requirements.txt .

# Instalar dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY . .

# Criar diretório de dados
RUN mkdir -p /app/data

# Expor porta para health check (opcional)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=5m --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import sqlite3; sqlite3.connect('/app/data/opusdei.db').execute('SELECT 1')" || exit 1

# Comando de inicialização
CMD ["python", "main.py"]
