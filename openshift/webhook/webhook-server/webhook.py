#!/usr/bin/env python3
"""
OpenLineage Lineage Mutating Admission Webhook

Injects lineage registration initContainer into pods annotated with:
  ai.platform/lineage-enabled: "true"
"""

import base64
import json
import logging
import os
from flask import Flask, request, jsonify

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LINEAGE_INIT_IMAGE = os.getenv(
    "LINEAGE_INIT_IMAGE",
    "image-registry.openshift-image-registry.svc:5000/lineage/lineage-init-container:latest"
)
OPENLINEAGE_URL = os.getenv("OPENLINEAGE_URL", "http://marquez")


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy"}), 200


@app.route('/mutate', methods=['POST'])
def mutate():
    """Handle AdmissionReview requests."""
    try:
        admission_review = request.get_json()
        logger.info(f"Received AdmissionReview: {admission_review.get('request', {}).get('uid')}")

        # Extract the pod from the request
        admission_request = admission_review.get("request", {})
        pod = admission_request.get("object", {})

        # Check if lineage is enabled via label
        labels = pod.get("metadata", {}).get("labels", {})
        if labels.get("lineage-enabled") != "true":
            logger.info("Lineage not enabled, skipping mutation")
            return create_admission_response(admission_review, allowed=True, patch=None)

        logger.info(f"Lineage enabled for pod: {pod.get('metadata', {}).get('name')}")

        # Create the mutation patch
        patch = create_lineage_patch(pod)

        # Return the mutation response
        return create_admission_response(admission_review, allowed=True, patch=patch)

    except Exception as e:
        logger.error(f"Error processing webhook: {e}", exc_info=True)
        return create_admission_response(
            admission_review,
            allowed=False,
            status={"message": str(e)}
        )


def create_lineage_patch(pod):
    """
    Create JSON Patch to inject lineage initContainer and environment.

    Adds:
    1. InitContainer running lineage registration script
    2. Volume with Downward API for pod annotations
    3. Environment variables (OPENLINEAGE_NAMESPACE, OPENLINEAGE_URL, etc.)
    """
    namespace = pod.get("metadata", {}).get("namespace", "default")
    pod_name = pod.get("metadata", {}).get("name", "unknown")
    labels = pod.get("metadata", {}).get("labels", {})

    # Use app name from metadata labels (prefer app.kubernetes.io/name, fallback to app)
    job_name = labels.get("app.kubernetes.io/name") or labels.get("app") or pod_name

    # Build the initContainer
    init_container = {
        "name": "lineage-registration",
        "image": LINEAGE_INIT_IMAGE,
        "imagePullPolicy": "Always",
        "env": [
            {
                "name": "OPENLINEAGE_NAMESPACE",
                "value": namespace,
            },
            {
                "name": "OPENLINEAGE_URL",
                "value": OPENLINEAGE_URL,
            },
            {
                "name": "POD_NAMESPACE",
                "valueFrom": {
                    "fieldRef": {
                        "fieldPath": "metadata.namespace"
                    }
                }
            },
            {
                "name": "POD_NAME",
                "valueFrom": {
                    "fieldRef": {
                        "fieldPath": "metadata.name"
                    }
                }
            },
            {
                "name": "OWNER_NAME",
                "value": job_name,
            },
        ],
        "volumeMounts": [
            {
                "name": "podinfo",
                "mountPath": "/etc/podinfo/annotations",
                "readOnly": True,
            }
        ],
    }

    # Build the Downward API volume for annotations
    downward_volume = {
        "name": "podinfo",
        "downwardAPI": {
            "items": [
                {
                    "path": "annotations",
                    "fieldRef": {
                        "fieldPath": "metadata.annotations"
                    }
                }
            ]
        }
    }

    # Create JSON Patch operations
    patch = []

    # Check if initContainers exist
    init_containers = pod.get("spec", {}).get("initContainers", [])
    if init_containers:
        # Add to existing initContainers array
        patch.append({
            "op": "add",
            "path": "/spec/initContainers/-",
            "value": init_container,
        })
    else:
        # Create initContainers array
        patch.append({
            "op": "add",
            "path": "/spec/initContainers",
            "value": [init_container],
        })

    # Check if volumes exist
    volumes = pod.get("spec", {}).get("volumes", [])
    if volumes:
        # Add to existing volumes array
        patch.append({
            "op": "add",
            "path": "/spec/volumes/-",
            "value": downward_volume,
        })
    else:
        # Create volumes array
        patch.append({
            "op": "add",
            "path": "/spec/volumes",
            "value": [downward_volume],
        })

    return patch


def create_admission_response(admission_review, allowed, patch=None, status=None):
    """Create AdmissionReview response."""
    admission_request = admission_review.get("request", {})
    uid = admission_request.get("uid")

    response = {
        "apiVersion": "admission.k8s.io/v1",
        "kind": "AdmissionReview",
        "response": {
            "uid": uid,
            "allowed": allowed,
        }
    }

    if patch:
        # Encode patch as base64
        patch_json = json.dumps(patch)
        patch_b64 = base64.b64encode(patch_json.encode()).decode()

        response["response"]["patchType"] = "JSONPatch"
        response["response"]["patch"] = patch_b64

    if status:
        response["response"]["status"] = status

    return jsonify(response), 200


if __name__ == '__main__':
    # Run on HTTPS (required by Kubernetes admission webhooks)
    cert_file = os.getenv("TLS_CERT_FILE", "/etc/webhook/certs/tls.crt")
    key_file = os.getenv("TLS_KEY_FILE", "/etc/webhook/certs/tls.key")

    if os.path.exists(cert_file) and os.path.exists(key_file):
        logger.info("Starting webhook server with TLS")
        app.run(host='0.0.0.0', port=8443, ssl_context=(cert_file, key_file))
    else:
        logger.warning("TLS certificates not found, running without TLS (for testing only)")
        app.run(host='0.0.0.0', port=8080)
