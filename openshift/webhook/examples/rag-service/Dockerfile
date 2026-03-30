FROM registry.access.redhat.com/ubi9/python-311:latest

USER 0

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir -r requirements.txt

# Copy Feast configuration (can be overridden by ConfigMap mount)
COPY feast_config/ /app/feast_repo/

# Copy application
COPY app.py .

RUN chown -R 1001:0 /app && chmod -R g=u /app

USER 1001

EXPOSE 8000

ENV PYTHONUNBUFFERED=1

CMD ["python3", "app.py"]
