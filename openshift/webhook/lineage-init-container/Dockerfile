FROM registry.access.redhat.com/ubi9/python-311:latest

USER 0

WORKDIR /app

# Install only OpenLineage client
RUN python3 -m pip install --no-cache-dir \
    'openlineage-python>=1.0.0'

# Copy the registration script
COPY register_lineage.py .

RUN chown -R 1001:0 /app && chmod -R g=u /app

USER 1001

ENTRYPOINT ["python3", "register_lineage.py"]
