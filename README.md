# PDF Extract Mini App

A Databricks App that lets you upload scanned PDF batch records, extract structured data using the `ai_extract` SQL AI function, and provide human-in-the-loop feedback on extraction results.

## Architecture

1. **Upload** — PDF is preprocessed (pages rendered as 300 DPI images and rebuilt into a clean PDF) and stored in a Unity Catalog Volume.
2. **Extract** — The `ai_extract` SQL AI function runs against the uploaded file via a SQL Warehouse.
3. **Review & Feedback** — Extracted fields are displayed in the UI. Users can mark values as correct or provide corrections, which are saved to a `pdf_extract_feedback` Delta table.

## Prerequisites

* A Databricks workspace (AWS)
* A **Unity Catalog** catalog and schema you have write access to
* A **UC Volume** created inside that schema for document uploads
* A running **SQL Warehouse** (Serverless recommended) with AI Functions enabled
* Python 3.10+

## Project Structure

```
pdf-extract-mini-app/
├── app.py              # FastAPI application
├── app.yaml            # Databricks App configuration
├── requirements.txt    # Python dependencies
└── README.md
```

## Setup

### 1. Create Unity Catalog Resources

If they don't already exist, create the catalog, schema, and volume:

```sql
CREATE CATALOG IF NOT EXISTS <your_catalog>;
CREATE SCHEMA IF NOT EXISTS <your_catalog>.<your_schema>;
CREATE VOLUME IF NOT EXISTS <your_catalog>.<your_schema>.<your_volume>
  COMMENT 'Volume for PDF document uploads';
```

### 2. Note Your SQL Warehouse ID

Navigate to **SQL Warehouses** in the Databricks UI, select your warehouse, and copy the **ID** from the connection details or URL. It looks like: `862f1d757f0424f7`.

### 3. Configure `app.yaml`

Open `app.yaml` and fill in the four required environment variables:

```yaml
command:
  - python
  - -m
  - uvicorn
  - app:app
  - --host
  - 0.0.0.0
  - --port
  - "8000"

env:
  - name: UC_CATALOG
    value: "<your_catalog>"         # Unity Catalog catalog name
  - name: UC_SCHEMA
    value: "<your_schema>"          # Schema within the catalog
  - name: UC_VOLUME_NAME
    value: "<your_volume>"          # Volume name for storing uploaded PDFs
  - name: SQL_WAREHOUSE_ID
    value: "<your_warehouse_id>"    # Databricks SQL Warehouse ID
```

#### Argument Reference

| Variable | Required | Description |
| --- | --- | --- |
| `UC_CATALOG` | Yes | The Unity Catalog catalog where your schema and volume live. Example: `nm_demo` |
| `UC_SCHEMA` | Yes | The schema inside the catalog. The app also creates a `pdf_extract_feedback` table here. Example: `ocr_test` |
| `UC_VOLUME_NAME` | Yes | The name of the UC Volume where uploaded PDFs are stored. Example: `document_upload` |
| `SQL_WAREHOUSE_ID` | Yes | The ID of a running SQL Warehouse used to execute `ai_extract` queries. Example: `862f1d757f0424f7` |
| `EXTRACT_SCHEMA` | No | Optional JSON string to override the default extraction schema. If not set, the built-in batch-record schema is used. |

### 4. Deploy as a Databricks App

From the Databricks workspace:

1. Go to **Compute > Apps** and click **Create App**.
2. Point the app source to this project folder.
3. The `app.yaml` command and env variables will be picked up automatically.
4. Grant the app's service principal access to the catalog, schema, volume, and SQL warehouse.

### 5. Verify Permissions

The app's service principal needs:

* **USE CATALOG** on `<your_catalog>`
* **USE SCHEMA** on `<your_catalog>.<your_schema>`
* **READ VOLUME** and **WRITE VOLUME** on the volume
* **Can use** permission on the SQL Warehouse
* **CREATE TABLE** on the schema (for the feedback table)

## Usage

1. Open the app URL in your browser.
2. Drag and drop (or click to browse) a PDF file.
3. Click **Extract** — the app uploads the PDF, runs AI extraction, and displays results.
4. Review each extracted field. Mark values as correct or provide corrections.
5. Submit feedback — corrections are persisted to `<catalog>.<schema>.pdf_extract_feedback`.

## Custom Extraction Schema

To extract different fields, set the `EXTRACT_SCHEMA` environment variable in `app.yaml` with a JSON object describing your desired fields:

```yaml
  - name: EXTRACT_SCHEMA
    value: '{"invoice_number": {"type": "string", "description": "Invoice ID"}, "total_amount": {"type": "number", "description": "Total in USD"}}'
```

Each key is a field name, and the value must include `type` (`string` or `number`) and `description`.

## Dependencies

Defined in `requirements.txt`:

* **fastapi** — Web framework
* **uvicorn** — ASGI server
* **databricks-sdk** — Databricks API client (auth handled automatically inside the app)
* **python-multipart** — File upload support
* **pymupdf** — PDF rendering and preprocessing
