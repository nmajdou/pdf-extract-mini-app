"""Minimal Databricks App: upload a PDF → UC Volume → ai_extract."""

import io
import json
import os
import re
import tempfile
import time
import uuid

import fitz

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse

app = FastAPI(title="Batch records Extract Mock")

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _client() -> WorkspaceClient:
    return WorkspaceClient()


def _catalog() -> str:
    return os.environ["UC_CATALOG"]


def _schema() -> str:
    return os.environ["UC_SCHEMA"]


def _volume() -> str:
    return os.environ["UC_VOLUME_NAME"]


def _warehouse_id() -> str:
    wid = os.environ.get("SQL_WAREHOUSE_ID", "").strip()
    if not wid or wid == "REPLACE_WITH_WAREHOUSE_ID":
        raise HTTPException(500, "SQL_WAREHOUSE_ID is not configured.")
    return wid


def _extract_schema() -> str:
    """Return the JSON schema for ai_extract."""
    custom = os.environ.get("EXTRACT_SCHEMA", "").strip()
    if custom:
        return custom
    return json.dumps({
        "batch": {"type": "string", "description": "Identifier of the batch"},
        "article_number": {"type": "string", "description": "Article number"},
        "printing_number": {"type": "string", "description": "Printing number"},
        "start_time_hhmm": {"type": "string", "description": "Start time in HH:mm format"},
        "finish_time_hhmm": {"type": "string", "description": "Finish time in HH:mm format"},
        "performed_date": {"type": "string", "description": "Date when the activity was performed"},
        "witnessed_date": {"type": "string", "description": "Date when the activity was witnessed"},
        "volume_collected_ml": {"type": "number", "description": "Volume collected in milliliters"},
        "total_time_min": {"type": "number", "description": "Total time in minutes"},
        "flow_rate_ml_per_min": {"type": "number", "description": "Flow rate in milliliters per minute"},
        "recorded_date_1": {"type": "string", "description": "First recorded date"},
        "verified_date_1": {"type": "string", "description": "First verified date"},
        "equilibration_end_time_hhmm": {"type": "string", "description": "Equilibration end time in HH:mm format"},
        "volume_buffer_pumped_l": {"type": "number", "description": "Volume of buffer pumped through the column in liters"},
        "recorded_date_2": {"type": "string", "description": "Second recorded date"},
        "verified_date_2": {"type": "string", "description": "Second verified date"},
        "fraction_numbers_to_be_pooled": {"type": "string", "description": "Fraction numbers that are to be pooled"},
        "recorded_date_3": {"type": "string", "description": "Third recorded date"},
        "verified_date_3": {"type": "string", "description": "Third verified date"},
        "end_time_addition_completion_hhmm": {"type": "string", "description": "End time of addition completion in HH:mm format"},
        "total_time_addition_hhmm": {"type": "string", "description": "Total time of addition in HH:mm format"},
        "total_weight_salt_added_g": {"type": "number", "description": "Total weight of salt added in grams"},
        "performed_date_2": {"type": "string", "description": "Second performed date"},
        "verified_date_2b": {"type": "string", "description": "Second verified date (additional instance)"},
        "volume_material_transferred_1st_tryout_ml": {"type": "number", "description": "Volume of material transferred in first tryout (mL)"},
        "volume_material_transferred_2nd_tryout_ml": {"type": "number", "description": "Volume of material transferred in second tryout (mL)"},
        "total_volume_ml": {"type": "number", "description": "Total volume in milliliters"},
        "performed_date_3": {"type": "string", "description": "Third performed date"},
        "total_volume_product_suspension_ml": {"type": "number", "description": "Total volume of product suspension (mL)"},
        "volume_samples_removed_ml": {"type": "number", "description": "Volume of samples removed (mL)"},
        "final_volume_ml": {"type": "number", "description": "Final volume (mL)"},
        "recorded_date_4": {"type": "string", "description": "Fourth recorded date"},
        "verified_date_4": {"type": "string", "description": "Fourth verified date"},
        "magnetic_stirrer_plant_number": {"type": "string", "description": "Magnetic stirrer plant number"},
        "actual_stirrer_speed_setting_rpm": {"type": "number", "description": "Actual stirrer speed setting in RPM"},
        "start_time_first_stage_hhmm": {"type": "string", "description": "Start time of first stage in HH:mm format"},
        "calculated_latest_finish_time_hhmm": {"type": "string", "description": "Calculated latest finish time in HH:mm format"},
        "recorded_date_5": {"type": "string", "description": "Fifth recorded date"},
        "verified_date_5": {"type": "string", "description": "Fifth verified date"},
        "orange_juice_batch_number": {"type": "string", "description": "Orange juice batch number"},
        "orange_juice_expiry_date": {"type": "string", "description": "Orange juice expiry date"},
        "start_time_2_hhmm": {"type": "string", "description": "Second start time in HH:mm format"},
        "finish_time_2_hhmm": {"type": "string", "description": "Second finish time in HH:mm format"},
        "total_period_mins": {"type": "number", "description": "Total period in minutes"},
        "wfi_flush_20ml_carried_out": {"type": "string", "description": "Indicates if 20mL WFI flush was carried out"},
        "performed_date_4": {"type": "string", "description": "Fourth performed date"},
        "verified_date_6": {"type": "string", "description": "Sixth verified date"},
        "orange_juice_batch_number_2": {"type": "string", "description": "Second orange juice batch number"},
        "orange_juice_expiry_date_2": {"type": "string", "description": "Second orange juice expiry date"},
        "start_time_3_hhmm": {"type": "string", "description": "Third start time in HH:mm format"},
        "finish_time_3_hhmm": {"type": "string", "description": "Third finish time in HH:mm format"},
        "total_period_mins_2": {"type": "number", "description": "Second total period in minutes"},
        "wfi_flush_20ml_carried_out_2": {"type": "string", "description": "Indicates if second 20mL WFI flush was carried out"},
        "performed_date_5": {"type": "string", "description": "Fifth performed date"},
        "verified_date_7": {"type": "string", "description": "Seventh verified date"},
    })


# ---------------------------------------------------------------------------
# Preprocess scanned PDF (render pages as images → rebuild clean PDF)
# ---------------------------------------------------------------------------

def _rebuild_scanned_pdf(content: bytes) -> bytes:
    src = fitz.open(stream=content, filetype="pdf")
    out = fitz.open()

    for page in src:
        pix = page.get_pixmap(dpi=300)
        img_pdf = fitz.open(
            "pdf",
            fitz.open(stream=pix.tobytes("png"), filetype="png").convert_to_pdf(),
        )
        out.insert_pdf(img_pdf)
        img_pdf.close()

    src.close()
    clean_bytes = out.tobytes()
    out.close()
    return clean_bytes


# ---------------------------------------------------------------------------
# Upload PDF to UC Volume
# ---------------------------------------------------------------------------

def _upload_to_volume(w: WorkspaceClient, filename: str, content: bytes) -> str:
    # Preprocess: rebuild scanned PDF so ai_parse_document can read it
    content = _rebuild_scanned_pdf(content)

    safe = re.sub(r"[^a-zA-Z0-9._-]", "_", filename)
    unique = f"{uuid.uuid4().hex[:8]}_{safe}"
    path = f"/Volumes/{_catalog()}/{_schema()}/{_volume()}/{unique}"
    w.files.upload(path, io.BytesIO(content))
    return path


# ---------------------------------------------------------------------------
# Run ai_extract via SQL
# ---------------------------------------------------------------------------

def _run_extract(w: WorkspaceClient, volume_path: str) -> dict:
    schema_json = _extract_schema()
    escaped = schema_json.replace("'", "\\'")
    extract_expr = f"ai_extract(parsed, '{escaped}', options => map('version', '2.0'))"

    sql = f"""
    WITH parsed_documents AS (
        SELECT
            path,
            ai_parse_document(
                content,
                map('version', '2.0', 'descriptionElementTypes', '*')
            ) AS parsed
        FROM read_files('{volume_path}', format => 'binaryFile')
    )
    SELECT {extract_expr} AS extracted
    FROM parsed_documents
    WHERE parsed IS NOT NULL
    """

    resp = w.statement_execution.execute_statement(
        warehouse_id=_warehouse_id(),
        statement=sql,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        wait_timeout="50s",
    )

    # Poll if still running (AI functions can take minutes)
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(5)
        resp = w.statement_execution.get_statement(resp.statement_id)

    if resp.status.state != StatementState.SUCCEEDED:
        raise HTTPException(500, f"SQL failed: {resp.status.error}")

    rows = resp.result.data_array
    if not rows or not rows[0]:
        raise HTTPException(500, "No extraction result returned.")

    raw = json.loads(rows[0][0])
    # ai_extract returns {"error_message": ..., "response": {...actual fields...}}
    if isinstance(raw, dict) and "response" in raw:
        return raw["response"]
    return raw


# ---------------------------------------------------------------------------
# Feedback table
# ---------------------------------------------------------------------------

def _feedback_table_fqn() -> str:
    return f"`{_catalog()}`.`{_schema()}`.`pdf_extract_feedback`"


def _ensure_feedback_table(w: WorkspaceClient):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {_feedback_table_fqn()} (
        id STRING,
        filename STRING,
        volume_path STRING,
        field_name STRING,
        extracted_value STRING,
        is_correct BOOLEAN,
        corrected_value STRING,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    """
    resp = w.statement_execution.execute_statement(
        warehouse_id=_warehouse_id(),
        statement=sql,
        wait_timeout="30s",
    )
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        raise HTTPException(500, f"Failed to create feedback table: {resp.status.error}")


def _insert_feedback(w: WorkspaceClient, rows: list[dict]):
    values = []
    for r in rows:
        rid = uuid.uuid4().hex[:16]
        fn = r["filename"].replace("'", "''")
        vp = r["volume_path"].replace("'", "''")
        field = r["field_name"].replace("'", "''")
        ext_val = str(r.get("extracted_value") or "").replace("'", "''")
        correct = "TRUE" if r["is_correct"] else "FALSE"
        corr_val = str(r.get("corrected_value") or "").replace("'", "''")
        values.append(
            f"('{rid}', '{fn}', '{vp}', '{field}', '{ext_val}', {correct}, '{corr_val}', CURRENT_TIMESTAMP())"
        )

    sql = f"""
    INSERT INTO {_feedback_table_fqn()}
    (id, filename, volume_path, field_name, extracted_value, is_correct, corrected_value, created_at)
    VALUES {', '.join(values)}
    """
    resp = w.statement_execution.execute_statement(
        warehouse_id=_warehouse_id(),
        statement=sql,
        wait_timeout="30s",
    )
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)

    if resp.status.state != StatementState.SUCCEEDED:
        raise HTTPException(500, f"Feedback insert failed: {resp.status.error}")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def index():
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Batch Records Extract</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet"/>
<style>
  :root {
    --bg: #0f1117;
    --surface: #1a1d27;
    --surface2: #232734;
    --border: #2e3345;
    --text: #e4e6ef;
    --text2: #8b8fa3;
    --accent: #6366f1;
    --accent-hover: #818cf8;
    --green: #22c55e;
    --green-dim: #16a34a;
    --red: #ef4444;
    --radius: 12px;
  }
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family:'Inter',system-ui,sans-serif; background:var(--bg); color:var(--text);
         min-height:100vh; }

  /* Layout */
  .app { display:flex; min-height:100vh; }
  .sidebar { width:340px; background:var(--surface); border-right:1px solid var(--border);
             padding:32px 24px; display:flex; flex-direction:column; flex-shrink:0; }
  .main { flex:1; padding:40px 48px; overflow-y:auto; }

  /* Sidebar */
  .logo { display:flex; align-items:center; gap:10px; margin-bottom:8px; }
  .logo-icon { width:36px; height:36px; background:var(--accent); border-radius:10px;
               display:flex; align-items:center; justify-content:center; font-size:18px; }
  .logo h1 { font-size:1.15rem; font-weight:700; letter-spacing:-.3px; }
  .logo-sub { font-size:.78rem; color:var(--text2); margin-bottom:32px; line-height:1.5; }

  /* Drop zone */
  .dropzone { border:2px dashed var(--border); border-radius:var(--radius); padding:32px 20px;
              text-align:center; cursor:pointer; transition:all .25s; position:relative; }
  .dropzone:hover, .dropzone.dragover { border-color:var(--accent); background:rgba(99,102,241,.06); }
  .dropzone-icon { font-size:2.4rem; margin-bottom:8px; opacity:.6; }
  .dropzone-text { font-size:.85rem; color:var(--text2); line-height:1.6; }
  .dropzone-text strong { color:var(--accent); }
  .dropzone input { position:absolute; inset:0; opacity:0; cursor:pointer; }
  .file-name { margin-top:12px; font-size:.8rem; color:var(--green); font-weight:500;
               display:none; word-break:break-all; }

  .extract-btn { width:100%; margin-top:20px; padding:14px; background:var(--accent);
                 color:#fff; border:none; border-radius:var(--radius); font-size:.9rem;
                 font-weight:600; cursor:pointer; transition:all .2s; letter-spacing:.2px; }
  .extract-btn:hover { background:var(--accent-hover); transform:translateY(-1px);
                       box-shadow:0 4px 20px rgba(99,102,241,.3); }
  .extract-btn:disabled { background:var(--surface2); color:var(--text2); cursor:wait;
                          transform:none; box-shadow:none; }

  .sidebar-footer { margin-top:auto; padding-top:24px; border-top:1px solid var(--border);
                    font-size:.72rem; color:var(--text2); line-height:1.6; }
  .sidebar-footer span { color:var(--accent); font-weight:600; }

  /* Spinner */
  .spinner { display:none; margin-top:20px; text-align:center; }
  .spinner-ring { width:32px; height:32px; border:3px solid var(--border);
                  border-top-color:var(--accent); border-radius:50%;
                  animation:spin .8s linear infinite; display:inline-block; }
  @keyframes spin { to { transform:rotate(360deg); } }
  .spinner-text { margin-top:8px; font-size:.8rem; color:var(--accent); font-weight:500; }
  .spinner-steps { margin-top:12px; text-align:left; font-size:.75rem; color:var(--text2); line-height:2; }
  .spinner-steps .done { color:var(--green); }
  .spinner-steps .active { color:var(--accent); }

  /* Main area */
  .placeholder { display:flex; flex-direction:column; align-items:center; justify-content:center;
                 height:100%; opacity:.4; text-align:center; }
  .placeholder-icon { font-size:4rem; margin-bottom:16px; }
  .placeholder-text { font-size:1rem; color:var(--text2); }

  /* Stats bar */
  .stats { display:flex; gap:16px; margin-bottom:24px; animation:fadeIn .4s ease; }
  .stat { background:var(--surface); border:1px solid var(--border); border-radius:var(--radius);
          padding:16px 20px; flex:1; }
  .stat-label { font-size:.7rem; text-transform:uppercase; letter-spacing:.8px;
                color:var(--text2); margin-bottom:4px; font-weight:600; }
  .stat-value { font-size:1.3rem; font-weight:700; }
  .stat-value.accent { color:var(--accent); }
  .stat-value.green { color:var(--green); }

  /* Results table */
  .results-header { display:flex; align-items:center; justify-content:space-between;
                    margin-bottom:16px; }
  .results-header h2 { font-size:1.1rem; font-weight:600; }
  .download-btn { display:inline-flex; align-items:center; gap:8px; padding:10px 20px;
                  background:var(--green); color:#fff; border:none; border-radius:8px;
                  font-size:.82rem; font-weight:600; cursor:pointer; transition:all .2s; }
  .download-btn:hover { background:var(--green-dim); transform:translateY(-1px);
                        box-shadow:0 4px 16px rgba(34,197,94,.25); }
  .download-btn svg { width:16px; height:16px; }

  .table-wrap { background:var(--surface); border:1px solid var(--border);
                border-radius:var(--radius); overflow:hidden; animation:fadeIn .4s ease; }
  table { width:100%; border-collapse:collapse; }
  th { background:var(--surface2); text-align:left; padding:12px 20px; font-size:.72rem;
       text-transform:uppercase; letter-spacing:1px; color:var(--text2); font-weight:600; }
  td { padding:11px 20px; font-size:.85rem; border-top:1px solid var(--border); }
  td.field { color:var(--text2); font-weight:500; width:40%; }
  td.value { color:var(--text); font-weight:500; }
  td.value.empty { color:#555; font-style:italic; }
  tr:hover td { background:rgba(99,102,241,.04); }
  .row-num { color:#444; font-size:.75rem; width:40px; text-align:center; }

  /* Feedback */
  td.fb { width:140px; white-space:nowrap; }
  .fb-toggle { display:inline-flex; gap:4px; }
  .fb-btn { padding:4px 10px; border-radius:6px; border:1px solid var(--border); background:transparent;
            font-size:.72rem; font-weight:600; cursor:pointer; transition:all .15s; color:var(--text2); }
  .fb-btn:hover { border-color:var(--text2); }
  .fb-btn.correct.active { background:var(--green); border-color:var(--green); color:#fff; }
  .fb-btn.wrong.active { background:var(--red); border-color:var(--red); color:#fff; }
  td.correction { width:180px; }
  .correction-input { width:100%; padding:6px 10px; border-radius:6px; border:1px solid var(--border);
                      background:var(--surface2); color:var(--text); font-size:.8rem;
                      font-family:inherit; outline:none; transition:border .2s; }
  .correction-input:focus { border-color:var(--accent); }
  .correction-input:disabled { opacity:.3; cursor:not-allowed; }
  .submit-fb { margin-top:16px; display:none; padding:12px 28px; background:var(--accent);
               color:#fff; border:none; border-radius:8px; font-size:.85rem; font-weight:600;
               cursor:pointer; transition:all .2s; }
  .submit-fb:hover { background:var(--accent-hover); }
  .submit-fb:disabled { background:var(--surface2); color:var(--text2); cursor:wait; }
  .fb-success { margin-top:12px; font-size:.82rem; color:var(--green); display:none; font-weight:500; }

  /* Error */
  .error { background:rgba(239,68,68,.1); border:1px solid rgba(239,68,68,.25);
           color:var(--red); padding:16px 20px; border-radius:var(--radius); font-size:.85rem; }

  .result { display:none; }
  @keyframes fadeIn { from { opacity:0; transform:translateY(8px); } to { opacity:1; transform:none; } }

  /* Responsive */
  @media (max-width:768px) {
    .app { flex-direction:column; }
    .sidebar { width:100%; border-right:none; border-bottom:1px solid var(--border); }
    .main { padding:24px 16px; }
    .stats { flex-direction:column; }
  }
</style>
</head>
<body>
<div class="app">
  <!-- Sidebar -->
  <div class="sidebar">
    <div class="logo">
      <div class="logo-icon">AI</div>
      <h1>Batch Records Extract</h1>
    </div>
    <p class="logo-sub">Upload a document to extract structured data using Databricks AI Functions.</p>

    <form id="form">
      <div class="dropzone" id="dropzone">
        <div class="dropzone-icon">&#128196;</div>
        <div class="dropzone-text">
          Drag & drop a PDF here<br/>or <strong>click to browse</strong>
        </div>
        <input type="file" id="file" accept=".pdf"/>
      </div>
      <div class="file-name" id="fileName"></div>
      <button type="submit" class="extract-btn" id="btn" disabled>Extract Fields</button>
    </form>

    <div class="spinner" id="spinner">
      <div class="spinner-ring"></div>
      <div class="spinner-text" id="spinnerText">Preprocessing scanned PDF...</div>
      <div class="spinner-steps" id="spinnerSteps"></div>
    </div>

    <div class="sidebar-footer">
      Powered by <span>Databricks AI Functions</span><br/>
      ai_parse_document + ai_extract
    </div>
  </div>

  <!-- Main -->
  <div class="main">
    <div class="placeholder" id="placeholder">
      <div class="placeholder-icon">&#128203;</div>
      <div class="placeholder-text">Upload a PDF to see extracted results here</div>
    </div>
    <div class="result" id="result"></div>
  </div>
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"></script>
<script>
const form = document.getElementById('form');
const btn = document.getElementById('btn');
const fileInput = document.getElementById('file');
const fileNameEl = document.getElementById('fileName');
const spinner = document.getElementById('spinner');
const result = document.getElementById('result');
const placeholder = document.getElementById('placeholder');
const dropzone = document.getElementById('dropzone');
let lastData = null;

// Drag & drop
dropzone.addEventListener('dragover', (e) => { e.preventDefault(); dropzone.classList.add('dragover'); });
dropzone.addEventListener('dragleave', () => dropzone.classList.remove('dragover'));
dropzone.addEventListener('drop', (e) => {
  e.preventDefault();
  dropzone.classList.remove('dragover');
  if (e.dataTransfer.files.length) {
    fileInput.files = e.dataTransfer.files;
    showFile(e.dataTransfer.files[0]);
  }
});
fileInput.addEventListener('change', () => {
  if (fileInput.files[0]) showFile(fileInput.files[0]);
});

function showFile(f) {
  fileNameEl.textContent = f.name + ' (' + (f.size / 1024).toFixed(0) + ' KB)';
  fileNameEl.style.display = 'block';
  btn.disabled = false;
}

function renderResults(data) {
  const extracted = data.extracted || {};
  const fields = Object.entries(extracted);
  const filled = fields.filter(([,v]) => v !== null && v !== '').length;

  let html = `<div class="stats">
    <div class="stat"><div class="stat-label">Source File</div>
      <div class="stat-value">${data.filename}</div></div>
    <div class="stat"><div class="stat-label">Fields Extracted</div>
      <div class="stat-value accent">${fields.length}</div></div>
    <div class="stat"><div class="stat-label">Fields Populated</div>
      <div class="stat-value green">${filled} / ${fields.length}</div></div>
  </div>`;

  html += `<div class="results-header">
    <h2>Extraction Results</h2>
    <button class="download-btn" onclick="downloadExcel()">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4M7 10l5 5 5-5M12 15V3"/>
      </svg>
      Download Excel
    </button>
  </div>`;

  html += '<div class="table-wrap"><table><thead><tr><th>#</th><th>Field</th><th>Value</th><th>Feedback</th><th>Correction</th></tr></thead><tbody>';
  fields.forEach(([key, val], i) => {
    const label = key.replace(/_/g, ' ').replace(/\\b\\w/g, c => c.toUpperCase());
    const isEmpty = val === null || val === '';
    const display = isEmpty ? 'Not found' : val;
    const cls = isEmpty ? 'value empty' : 'value';
    html += `<tr data-key="${key}" data-val="${(val===null?'':val)}">
      <td class="row-num">${i+1}</td>
      <td class="field">${label}</td>
      <td class="${cls}">${display}</td>
      <td class="fb">
        <div class="fb-toggle">
          <button class="fb-btn correct" onclick="setFb(this,'correct')">Correct</button>
          <button class="fb-btn wrong" onclick="setFb(this,'wrong')">Wrong</button>
        </div>
      </td>
      <td class="correction">
        <input class="correction-input" placeholder="Correct value..." disabled/>
      </td>
    </tr>`;
  });
  html += '</tbody></table></div>';
  html += '<button class="submit-fb" id="submitFb" onclick="submitFeedback()">Submit Feedback</button>';
  html += '<div class="fb-success" id="fbSuccess"></div>';
  return html;
}

form.addEventListener('submit', async (e) => {
  e.preventDefault();
  const file = fileInput.files[0];
  if (!file) return;

  btn.disabled = true;
  spinner.style.display = 'block';
  result.style.display = 'none';
  placeholder.style.display = 'none';

  const spinnerText = document.getElementById('spinnerText');
  const spinnerSteps = document.getElementById('spinnerSteps');
  spinnerSteps.innerHTML = '';
  const steps = [
    { label: 'Preprocessing scanned PDF...', delay: 0 },
    { label: 'Uploading to volume...', delay: 5000 },
    { label: 'Parsing document (ai_parse_document)...', delay: 8000 },
    { label: 'Extracting fields (ai_extract)...', delay: 25000 },
    { label: 'Almost there...', delay: 60000 },
  ];
  let stepIdx = 0;
  spinnerText.textContent = steps[0].label;
  const stepTimer = setInterval(() => {
    if (stepIdx < steps.length) {
      spinnerSteps.innerHTML += `<div class="done">\u2713 ${steps[stepIdx].label.replace('...','')}</div>`;
    }
    stepIdx++;
    if (stepIdx < steps.length) {
      spinnerText.textContent = steps[stepIdx].label;
    }
  }, steps[Math.min(stepIdx + 1, steps.length - 1)]?.delay || 15000);

  // Better: use timeouts for each step
  clearInterval(stepTimer);
  let timers = [];
  steps.forEach((s, i) => {
    if (i === 0) { spinnerText.textContent = s.label; return; }
    timers.push(setTimeout(() => {
      spinnerSteps.innerHTML += `<div class="done">\u2713 ${steps[i-1].label.replace('...','')}</div>`;
      spinnerText.textContent = s.label;
    }, s.delay));
  });

  const fd = new FormData();
  fd.append('file', file);

  try {
    const res = await fetch('/api/extract', { method: 'POST', body: fd });
    const text = await res.text();
    let data;
    try { data = JSON.parse(text); } catch { throw new Error(text); }
    if (!res.ok) throw new Error(data.detail || JSON.stringify(data));
    lastData = data;
    result.innerHTML = renderResults(data);
    result.style.display = 'block';
  } catch (err) {
    result.innerHTML = `<div class="error">${err.message}</div>`;
    result.style.display = 'block';
  } finally {
    btn.disabled = false;
    spinner.style.display = 'none';
    timers.forEach(t => clearTimeout(t));
  }
});

function downloadExcel() {
  if (!lastData) return;
  const extracted = lastData.extracted || {};

  const rows = Object.entries(extracted).map(([key, val]) => ({
    Field: key.replace(/_/g, ' ').replace(/\\b\\w/g, c => c.toUpperCase()),
    Value: val === null || val === '' ? '' : val
  }));

  const ws = XLSX.utils.json_to_sheet(rows);
  ws['!cols'] = [{ wch: 45 }, { wch: 45 }];
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Extraction Results');

  const safeName = lastData.filename.replace(/\\.pdf$/i, '');
  XLSX.writeFile(wb, `${safeName}_extracted.xlsx`);
}

function setFb(btn, type) {
  const row = btn.closest('tr');
  const btns = row.querySelectorAll('.fb-btn');
  btns.forEach(b => b.classList.remove('active'));
  btn.classList.add('active');

  const input = row.querySelector('.correction-input');
  if (type === 'wrong') {
    input.disabled = false;
    input.focus();
  } else {
    input.disabled = true;
    input.value = '';
  }

  // Show submit button if any feedback given
  const anyFb = document.querySelectorAll('.fb-btn.active').length > 0;
  document.getElementById('submitFb').style.display = anyFb ? 'inline-block' : 'none';
}

async function submitFeedback() {
  if (!lastData) return;
  const rows = document.querySelectorAll('.table-wrap tbody tr');
  const items = [];

  rows.forEach(row => {
    const activeBtn = row.querySelector('.fb-btn.active');
    if (!activeBtn) return;

    const isCorrect = activeBtn.classList.contains('correct');
    const correctedValue = row.querySelector('.correction-input').value;
    items.push({
      filename: lastData.filename,
      volume_path: lastData.volume_path,
      field_name: row.dataset.key,
      extracted_value: row.dataset.val,
      is_correct: isCorrect,
      corrected_value: isCorrect ? '' : correctedValue,
    });
  });

  if (items.length === 0) return;

  const submitBtn = document.getElementById('submitFb');
  const fbSuccess = document.getElementById('fbSuccess');
  submitBtn.disabled = true;
  submitBtn.textContent = 'Submitting...';
  fbSuccess.style.display = 'none';

  try {
    const res = await fetch('/api/feedback', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ items }),
    });
    const text = await res.text();
    let data;
    try { data = JSON.parse(text); } catch { throw new Error(text); }
    if (!res.ok) throw new Error(data.detail || 'Feedback submission failed');
    fbSuccess.textContent = `Feedback saved (${data.count} fields) to Unity Catalog table.`;
    fbSuccess.style.display = 'block';
    submitBtn.style.display = 'none';
  } catch (err) {
    fbSuccess.textContent = err.message;
    fbSuccess.style.display = 'block';
    fbSuccess.style.color = 'var(--red)';
  } finally {
    submitBtn.disabled = false;
    submitBtn.textContent = 'Submit Feedback';
  }
}
</script>
</body>
</html>"""


@app.post("/api/extract")
async def extract(file: UploadFile = File(...)):
    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Only PDF files are accepted.")

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(400, "Empty file.")

    try:
        w = _client()
        volume_path = _upload_to_volume(w, file.filename, content)
        extracted = _run_extract(w, volume_path)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Extraction failed: {e}")

    return {
        "filename": file.filename,
        "volume_path": volume_path,
        "extracted": extracted,
    }


@app.post("/api/feedback")
async def feedback(request: Request):
    body = await request.json()
    items = body.get("items", [])
    if not items:
        raise HTTPException(400, "No feedback items provided.")

    try:
        w = _client()
        _ensure_feedback_table(w)
        _insert_feedback(w, items)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Feedback failed: {e}")

    return {"status": "ok", "count": len(items)}


@app.get("/api/health")
def health():
    return {"status": "ok"}
