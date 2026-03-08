"""
Lightweight OpenLineage-compatible HTTP receiver.

Replaces Marquez to avoid Docker Hub dependency.
All lineage events are stored in the existing PostgreSQL 'warehouse' database.

Endpoints
---------
POST /api/v1/lineage       – ingest events from openlineage-python clients
GET  /api/v1/namespaces    – Marquez-compat health / namespace list
GET  /api/v1/jobs          – job summary with latest-run state
GET  /health               – liveness probe
GET  /                     – HTML dashboard (auto-refreshes every 30 s)
"""

import json
import logging
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Query, Request, Response
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from configs.settings import PG_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── Schema ────────────────────────────────────────────────────────────────
_DDL = """
CREATE TABLE IF NOT EXISTS ol_runs (
    run_id        TEXT        PRIMARY KEY,
    job_namespace TEXT        NOT NULL,
    job_name      TEXT        NOT NULL,
    state         TEXT        NOT NULL DEFAULT 'RUNNING',
    started_at    TIMESTAMPTZ,
    ended_at      TIMESTAMPTZ,
    duration_ms   BIGINT,
    inputs        JSONB       NOT NULL DEFAULT '[]',
    outputs       JSONB       NOT NULL DEFAULT '[]',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ol_runs_job     ON ol_runs (job_namespace, job_name);
CREATE INDEX IF NOT EXISTS ol_runs_updated ON ol_runs (updated_at DESC);
"""

engine = create_engine(PG_URL, pool_pre_ping=True)


@asynccontextmanager
async def lifespan(_: FastAPI):
    with engine.begin() as conn:
        conn.execute(text(_DDL))
    log.info("ol_runs table ready")
    yield


app = FastAPI(title="OpenLineage Receiver", lifespan=lifespan)


# ── Ingest ────────────────────────────────────────────────────────────────
_STATE = {"START": "RUNNING", "COMPLETE": "COMPLETE", "FAIL": "FAILED", "ABORT": "ABORTED"}


@app.post("/api/v1/lineage", status_code=201)
async def receive_lineage(request: Request):
    try:
        ev = await request.json()
    except Exception:
        return Response(status_code=400)

    run    = ev.get("run") or {}
    job    = ev.get("job") or {}
    etype  = ev.get("eventType", "UNKNOWN")
    etime  = ev.get("eventTime") or datetime.now(timezone.utc).isoformat()
    run_id = run.get("runId") or ""
    ns     = job.get("namespace") or ""
    name   = job.get("name") or ""
    inputs  = json.dumps(ev.get("inputs") or [])
    outputs = json.dumps(ev.get("outputs") or [])
    state   = _STATE.get(etype, etype)
    terminal = state in ("COMPLETE", "FAILED", "ABORTED")

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO ol_runs
                (run_id, job_namespace, job_name, state,
                 started_at, inputs, outputs)
            VALUES (
                :run_id, :ns, :name, :state,
                CASE WHEN :etype = 'START' THEN CAST(:etime AS timestamptz) END,
                CAST(:inputs AS jsonb), CAST(:outputs AS jsonb)
            )
            ON CONFLICT (run_id) DO UPDATE SET
                state       = CASE WHEN :terminal THEN :state ELSE ol_runs.state END,
                ended_at    = CASE WHEN :terminal THEN CAST(:etime AS timestamptz) END,
                duration_ms = CASE
                    WHEN :terminal AND ol_runs.started_at IS NOT NULL
                    THEN CAST(
                        EXTRACT(EPOCH FROM (CAST(:etime AS timestamptz) - ol_runs.started_at)) * 1000
                        AS BIGINT)
                    END,
                inputs  = CASE WHEN jsonb_array_length(CAST(:inputs  AS jsonb)) > 0
                               THEN CAST(:inputs  AS jsonb) ELSE ol_runs.inputs  END,
                outputs = CASE WHEN jsonb_array_length(CAST(:outputs AS jsonb)) > 0
                               THEN CAST(:outputs AS jsonb) ELSE ol_runs.outputs END,
                updated_at  = NOW()
        """), dict(run_id=run_id, ns=ns, name=name, state=state, etype=etype,
                   etime=etime, inputs=inputs, outputs=outputs, terminal=terminal))

    log.info("OL event stored: %s %s  %s/%s", etype, run_id[:8], ns, name)
    return Response(status_code=201)


# ── Marquez-compatible query API ─────────────────────────────────────────
@app.get("/api/v1/namespaces")
def list_namespaces():
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT DISTINCT job_namespace FROM ol_runs ORDER BY job_namespace"
        )).fetchall()
    ns_list = [{"name": r[0], "createdAt": "", "updatedAt": "", "description": None}
               for r in rows]
    return {"totalCount": len(ns_list), "namespaces": ns_list}


@app.get("/api/v1/jobs")
def list_jobs(namespace: str = Query(None), limit: int = Query(100)):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT ON (job_namespace, job_name)
                run_id, job_namespace, job_name, state,
                started_at, ended_at, duration_ms,
                inputs, outputs, updated_at
            FROM ol_runs
            WHERE (:ns IS NULL OR job_namespace = :ns)
            ORDER BY job_namespace, job_name, updated_at DESC
            LIMIT :limit
        """), {"ns": namespace, "limit": limit}).fetchall()

    jobs = []
    for r in rows:
        jobs.append({
            "id":        {"namespace": r.job_namespace, "name": r.job_name},
            "namespace": r.job_namespace,
            "name":      r.job_name,
            "updatedAt": r.updated_at.isoformat() if r.updated_at else "",
            "inputs":    [{"namespace": d["namespace"], "name": d["name"]}
                          for d in (r.inputs or [])],
            "outputs":   [{"namespace": d["namespace"], "name": d["name"]}
                          for d in (r.outputs or [])],
            "latestRun": {
                "id":         r.run_id,
                "state":      r.state,
                "startedAt":  r.started_at.isoformat() if r.started_at else "",
                "endedAt":    r.ended_at.isoformat()   if r.ended_at   else "",
                "durationMs": r.duration_ms,
            },
        })
    return {"totalCount": len(jobs), "jobs": jobs}


@app.get("/health")
def health():
    return {"status": "ok"}


# ── HTML dashboard ────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
def dashboard():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT run_id, job_namespace, job_name, state,
                   started_at, ended_at, duration_ms, inputs, outputs
            FROM ol_runs
            ORDER BY updated_at DESC
            LIMIT 200
        """)).fetchall()

    # ── Build lineage graph data ──────────────────────────────────────────
    # Collect unique datasets and jobs, deduplicated by identity
    ds_set: dict = {}   # ds_id → label
    job_set: dict = {}  # job_name → {inputs, outputs, state}
    for r in rows:
        jkey = f"{r.job_namespace}/{r.job_name}"
        if jkey not in job_set:
            job_set[jkey] = {"name": r.job_name, "ns": r.job_namespace,
                             "state": r.state, "inputs": r.inputs or [],
                             "outputs": r.outputs or []}
        for d in (r.inputs or []):
            did = f"{d.get('namespace','')}/{d.get('name','')}"
            ds_set[did] = d.get("name", did).split("/")[-1]
        for d in (r.outputs or []):
            did = f"{d.get('namespace','')}/{d.get('name','')}"
            ds_set[did] = d.get("name", did).split("/")[-1]

    # Assign positions: datasets left (col 0), jobs middle (col 1), outputs right (col 2)
    # Identify which datasets are outputs (right side) vs inputs (left side)
    output_ds: set = set()
    for j in job_set.values():
        for d in j["outputs"]:
            output_ds.add(f"{d.get('namespace','')}/{d.get('name','')}")

    left_ds  = [k for k in ds_set if k not in output_ds]
    right_ds = [k for k in ds_set if k in output_ds]
    jobs     = list(job_set.values())

    W, H       = 860, max(220, max(len(left_ds), len(jobs), len(right_ds)) * 90 + 60)
    COL        = [80, 340, 620]   # x centres for left-ds, jobs, right-ds
    NODE_W     = 180
    NODE_H     = 48
    JOB_COLOR  = {"COMPLETE": "#2e7d32", "FAILED": "#c62828",
                  "ABORTED": "#e65100", "RUNNING": "#1565c0"}

    def cy(idx, total):
        if total == 0:
            return H // 2
        step = H / (total + 1)
        return int(step * (idx + 1))

    def rect_svg(x, y, label, fill, text_col="#fff", rx=6):
        x0, y0 = x - NODE_W // 2, y - NODE_H // 2
        short = label if len(label) <= 22 else label[:20] + "…"
        return (f'<rect x="{x0}" y="{y0}" width="{NODE_W}" height="{NODE_H}" '
                f'rx="{rx}" fill="{fill}" />'
                f'<text x="{x}" y="{y + 5}" text-anchor="middle" '
                f'font-size="12" fill="{text_col}" font-family="sans-serif">'
                f'{short}</text>')

    def arrow(x1, y1, x2, y2):
        mx = (x1 + x2) // 2
        return (f'<path d="M{x1},{y1} C{mx},{y1} {mx},{y2} {x2},{y2}" '
                f'fill="none" stroke="#90a4ae" stroke-width="1.8" '
                f'marker-end="url(#arr)"/>')

    # Node position maps
    left_pos  = {k: (COL[0], cy(i, len(left_ds)))  for i, k in enumerate(left_ds)}
    right_pos = {k: (COL[2], cy(i, len(right_ds))) for i, k in enumerate(right_ds)}
    job_pos   = {f"{j['ns']}/{j['name']}": (COL[1], cy(i, len(jobs)))
                 for i, j in enumerate(jobs)}

    svg_nodes, svg_edges = [], []
    for k, (x, y) in left_pos.items():
        svg_nodes.append(rect_svg(x, y, ds_set[k], "#3f51b5", rx=24))
    for k, (x, y) in right_pos.items():
        svg_nodes.append(rect_svg(x, y, ds_set[k], "#00796b", rx=24))
    for j in jobs:
        jkey = f"{j['ns']}/{j['name']}"
        x, y = job_pos[jkey]
        col  = JOB_COLOR.get(j["state"], "#546e7a")
        svg_nodes.append(rect_svg(x, y, j["name"], col, rx=6))
        for d in j["inputs"]:
            dk = f"{d.get('namespace','')}/{d.get('name','')}"
            if dk in left_pos:
                dx, dy = left_pos[dk]
                svg_edges.append(arrow(dx + NODE_W // 2, dy, x - NODE_W // 2, y))
        for d in j["outputs"]:
            dk = f"{d.get('namespace','')}/{d.get('name','')}"
            if dk in right_pos:
                dx, dy = right_pos[dk]
                svg_edges.append(arrow(x + NODE_W // 2, y, dx - NODE_W // 2, dy))

    graph_svg = f"""
    <svg width="{W}" height="{H}" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <marker id="arr" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
          <path d="M0,0 L0,6 L8,3 z" fill="#90a4ae"/>
        </marker>
      </defs>
      <!-- column labels -->
      <text x="{COL[0]}" y="22" text-anchor="middle" font-size="11"
            fill="#888" font-family="sans-serif">INPUTS</text>
      <text x="{COL[1]}" y="22" text-anchor="middle" font-size="11"
            fill="#888" font-family="sans-serif">JOBS</text>
      <text x="{COL[2]}" y="22" text-anchor="middle" font-size="11"
            fill="#888" font-family="sans-serif">OUTPUTS</text>
      {''.join(svg_edges)}
      {''.join(svg_nodes)}
    </svg>""" if job_set else '<p style="color:#999;padding:2rem">No lineage data yet.</p>'

    # ── Runs table ────────────────────────────────────────────────────────
    def badge(state: str) -> str:
        c = {"COMPLETE": "#2e7d32", "FAILED": "#c62828",
             "ABORTED": "#e65100"}.get(state, "#1565c0")
        return (f'<span style="background:{c};color:#fff;padding:2px 10px;'
                f'border-radius:12px;font-size:.8rem">{state}</span>')

    def chips(datasets) -> str:
        if not datasets:
            return "<span style='color:#999'>—</span>"
        out = []
        for d in datasets:
            ns, name = d.get("namespace", ""), d.get("name", "")
            label = f"{ns.split('://')[-1]}/{name}" if ns else name
            out.append(f'<span title="{ns}" style="display:inline-block;background:#e8eaf6;'
                       f'border-radius:4px;padding:1px 6px;margin:1px;font-size:.78rem">'
                       f'{label}</span>')
        return "".join(out)

    trs = ""
    for r in rows:
        dur     = f"{r.duration_ms:,} ms" if r.duration_ms else "—"
        started = r.started_at.strftime("%Y-%m-%d %H:%M:%S") if r.started_at else "—"
        trs += (f'<tr><td style="color:#666;font-size:.85rem">{r.job_namespace}</td>'
                f'<td><strong>{r.job_name}</strong></td><td>{badge(r.state)}</td>'
                f'<td style="font-size:.85rem">{started}</td>'
                f'<td style="font-size:.85rem">{dur}</td>'
                f'<td>{chips(r.inputs)}</td><td>{chips(r.outputs)}</td></tr>')

    empty = ('<tr><td colspan="7" style="text-align:center;color:#999;padding:3rem">'
             'No lineage runs recorded yet.</td></tr>')

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="30">
  <title>OpenLineage</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0 }}
    body  {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
             background: #f0f2f5; padding: 2rem; color: #333 }}
    h1    {{ font-size: 1.5rem; color: #1a1a2e; margin-bottom: .3rem }}
    .sub  {{ font-size: .85rem; color: #888; margin-bottom: 1.5rem }}
    .card {{ background: #fff; border-radius: 10px;
             box-shadow: 0 1px 8px rgba(0,0,0,.1); overflow: hidden;
             margin-bottom: 1.5rem }}
    .card-head {{ padding: 14px 18px; border-bottom: 1px solid #f0f2f5;
                  font-weight: 600; font-size: .95rem; color: #1a1a2e }}
    .graph-wrap {{ padding: 1rem; overflow-x: auto }}
    table {{ width: 100%; border-collapse: collapse }}
    th {{ background: #1a1a2e; color: #fff; padding: 11px 14px; text-align: left;
          font-size: .8rem; text-transform: uppercase; letter-spacing: .06em }}
    td {{ padding: 10px 14px; border-bottom: 1px solid #f0f2f5; vertical-align: middle }}
    tr:last-child td {{ border-bottom: none }}
    tr:hover td {{ background: #fafbff }}
    .legend {{ display: flex; gap: 1rem; padding: .75rem 1rem;
               border-top: 1px solid #f0f2f5; font-size: .8rem; color: #666 }}
    .dot {{ width: 12px; height: 12px; border-radius: 50%;
            display: inline-block; margin-right: 4px; vertical-align: middle }}
  </style>
</head>
<body>
  <h1>&#x1F4CA; OpenLineage</h1>
  <p class="sub">Auto-refreshes every 30 s &nbsp;·&nbsp; {len(rows)} runs recorded</p>

  <div class="card">
    <div class="card-head">Lineage Graph</div>
    <div class="graph-wrap">{graph_svg}</div>
    <div class="legend">
      <span><span class="dot" style="background:#3f51b5"></span>Input dataset</span>
      <span><span class="dot" style="background:#00796b"></span>Output dataset</span>
      <span><span class="dot" style="background:#2e7d32;border-radius:3px"></span>Job (complete)</span>
      <span><span class="dot" style="background:#c62828;border-radius:3px"></span>Job (failed)</span>
      <span><span class="dot" style="background:#1565c0;border-radius:3px"></span>Job (running)</span>
    </div>
  </div>

  <div class="card">
    <div class="card-head">Run History</div>
    <table>
      <thead><tr>
        <th>Namespace</th><th>Job</th><th>State</th>
        <th>Started</th><th>Duration</th><th>Inputs</th><th>Outputs</th>
      </tr></thead>
      <tbody>{trs or empty}</tbody>
    </table>
  </div>
</body>
</html>"""
