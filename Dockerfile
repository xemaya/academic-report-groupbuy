FROM a2h/agent-base:python-3.12-http

WORKDIR /opt/agent

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
