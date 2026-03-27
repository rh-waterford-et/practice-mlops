#!/usr/bin/env bash
# Generate TLS certificates for the lineage webhook
set -euo pipefail

NAMESPACE=${1:-lineage}
SERVICE_NAME="lineage-webhook"
SECRET_NAME="lineage-webhook-certs"

echo "Generating TLS certificates for webhook in namespace: $NAMESPACE"

# Create a temporary directory
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

cd $TMPDIR

# Generate CA private key
openssl genrsa -out ca.key 2048

# Generate CA certificate
openssl req -x509 -new -nodes -key ca.key -subj "/CN=${SERVICE_NAME}.${NAMESPACE}.svc" -days 3650 -out ca.crt

# Generate server private key
openssl genrsa -out tls.key 2048

# Create CSR configuration
cat > csr.conf <<EOF
[req]
req_extensions = v3_req
distinguished_name = req_distinguished_name
[req_distinguished_name]
[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = ${SERVICE_NAME}
DNS.2 = ${SERVICE_NAME}.${NAMESPACE}
DNS.3 = ${SERVICE_NAME}.${NAMESPACE}.svc
DNS.4 = ${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local
EOF

# Generate server CSR
openssl req -new -key tls.key -subj "/CN=${SERVICE_NAME}.${NAMESPACE}.svc" -config csr.conf -out server.csr

# Sign the CSR with the CA
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out tls.crt -days 3650 -extensions v3_req -extfile csr.conf

echo "✓ Certificates generated"

# Create or update the secret in Kubernetes
if oc get secret $SECRET_NAME -n $NAMESPACE &>/dev/null; then
    echo "Deleting existing secret: $SECRET_NAME"
    oc delete secret $SECRET_NAME -n $NAMESPACE
fi

echo "Creating secret: $SECRET_NAME"
oc create secret tls $SECRET_NAME \
    --cert=tls.crt \
    --key=tls.key \
    -n $NAMESPACE

# Output the CA bundle for the MutatingWebhookConfiguration
CA_BUNDLE=$(base64 < ca.crt | tr -d '\n')

echo ""
echo "✓ Secret created: $SECRET_NAME"
echo ""
echo "========================================"
echo "CA Bundle for MutatingWebhookConfiguration:"
echo "========================================"
echo "$CA_BUNDLE"
echo ""
echo "Save this value and use it in the webhook configuration"
