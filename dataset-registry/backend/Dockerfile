FROM registry.access.redhat.com/ubi9/python-311:latest

USER 0
WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY *.py .

RUN chown -R 1001:0 /app && chmod -R g=u /app
USER 1001

EXPOSE 8080

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
