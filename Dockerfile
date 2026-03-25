FROM registry.access.redhat.com/ubi9/python-311:latest

USER 0

WORKDIR /app

COPY requirements.txt .
COPY wheels/ wheels/
RUN pip3 install --no-cache-dir -r requirements.txt

COPY configs/ configs/
COPY src/ src/
COPY data/customers.csv data/customers.csv
COPY data/sample_docs/ data/sample_docs/

RUN chown -R 1001:0 /app && chmod -R g=u /app

USER 1001

ENV FEAST_REPO_PATH=/app/src/feature_store
ENV PYTHONPATH=/app

CMD ["python3", "--version"]
