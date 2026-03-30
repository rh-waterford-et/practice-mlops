FROM registry.access.redhat.com/ubi9/python-311:latest

USER 0

WORKDIR /app

# Install Flask
RUN python3 -m pip install --no-cache-dir \
    'Flask>=2.3.0'

# Copy webhook server
COPY webhook.py .

RUN chown -R 1001:0 /app && chmod -R g=u /app

USER 1001

EXPOSE 8443

ENTRYPOINT ["python3", "webhook.py"]
