#!/usr/bin/env bash
# Quick status check for the lineage namespace
set -euo pipefail

NS="lineage"

echo "══════ PODS ══════"
oc get pods -n "$NS" -o wide

echo ""
echo "══════ SERVICES ══════"
oc get svc -n "$NS"

echo ""
echo "══════ ROUTES ══════"
oc get routes -n "$NS"

echo ""
echo "══════ JOBS ══════"
oc get jobs -n "$NS"

echo ""
echo "══════ PVCS ══════"
oc get pvc -n "$NS"

echo ""
echo "══════ RECENT EVENTS ══════"
oc get events -n "$NS" --sort-by='.lastTimestamp' | tail -20
