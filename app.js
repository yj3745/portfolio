// Yash Jain - Data Engineer Portfolio Controller (Light Theme)

const architectureDetails = {
  // Project 1 Nodes
  p1_s3: {
    title: "AWS S3 (Ingestion Stage)",
    tech: "Amazon Web Services // Simple Storage Service",
    badge: "Cloud Object Storage",
    badgeColor: "bg-amber-50 text-amber-800 border-amber-200",
    sla: "< 100ms Event Publish",
    description: "Multi-tenant cloud object store holding raw IoT device telemetry files. Partitioned hierarchically by year/month/day/hour with external stage role encryption.",
    code: `-- External Stage mounted via AWS IAM Role
CREATE OR REPLACE STAGE raw_lake.stage_s3_telemetry
  URL = 's3://telemetry-streaming-ingest-eu-1/scanner_logs/'
  STORAGE_INTEGRATION = aws_s3_snowflake_role
  FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE);`
  },
  p1_eventbridge: {
    title: "AWS EventBridge & SQS",
    tech: "Amazon Web Services // Event-Driven Broker",
    badge: "Asynchronous Message Router",
    badgeColor: "bg-pink-50 text-pink-800 border-pink-200",
    sla: "Zero Latency (< 50ms)",
    description: "Captures S3 ObjectCreated events and routes notifications through Amazon EventBridge / SQS queue directly to Snowpipe with guaranteed at-least-once delivery.",
    code: `{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": { "name": ["telemetry-streaming-ingest-eu-1"] },
    "object": { "key": [{ "prefix": "scanner_logs/" }] }
  }
}`
  },
  p1_snowpipe: {
    title: "Snowflake Snowpipe",
    tech: "Snowflake // Serverless Continuous Ingestion",
    badge: "Auto-Ingest Engine",
    badgeColor: "bg-sky-50 text-sky-800 border-sky-200",
    sla: "< 60s Micro-batch SLA",
    description: "Serverless streaming ingestion service listening to SQS notifications. Automatically loads new files into raw tables with zero compute idle costs.",
    code: `-- Serverless Snowpipe with auto-ingest enabled
CREATE OR REPLACE PIPE raw_lake.pipe_scanner_telemetry
  AUTO_INGEST = TRUE
  AWS_SNS_TOPIC = 'arn:aws:sns:eu-central-1:123456789012:s3-events'
AS
COPY INTO raw_lake.bronze_scanner_telemetry (raw_payload, file_name, ingested_at)
FROM (
  SELECT $1, METADATA$FILENAME, CURRENT_TIMESTAMP()
  FROM @raw_lake.stage_s3_telemetry
);`
  },
  p1_bronze: {
    title: "Bronze Layer (Raw Schema-on-Read)",
    tech: "Snowflake // Append-Only VARIANT Storage",
    badge: "Bronze Raw Zone",
    badgeColor: "bg-amber-50 text-amber-800 border-amber-200",
    sla: "Near Real-Time (< 15 mins)",
    description: "Append-only staging table preserving 100% untransformed JSON payload structure in VARIANT columns alongside ingestion timestamps and file lineage.",
    code: `CREATE OR REPLACE TABLE raw_lake.bronze_scanner_telemetry (
  raw_payload       VARIANT NOT NULL,
  file_name         VARCHAR NOT NULL,
  file_row_number   NUMBER,
  ingested_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);`
  },
  p1_silver: {
    title: "Silver Layer (CDC & SCD Type 2)",
    tech: "Snowflake Streams & Scheduled Tasks",
    badge: "Curated & Conformed",
    badgeColor: "bg-indigo-50 text-indigo-800 border-indigo-200",
    sla: "Hourly / Stream Delta",
    description: "Snowflake Streams capture row deltas and scheduled Tasks perform atomic MERGE operations tracking historical hardware and device states via SCD Type 2.",
    code: `CREATE OR REPLACE STREAM raw_lake.str_bronze_telemetry 
  ON TABLE raw_lake.bronze_scanner_telemetry;

-- Atomic merge task tracking historical state changes
MERGE INTO curated_zone.dim_devices_scd2 AS tgt
USING raw_lake.str_bronze_telemetry AS src
ON tgt.device_id = src.raw_payload:device_id::VARCHAR AND tgt.is_current = TRUE
WHEN MATCHED AND tgt.status <> src.raw_payload:status::VARCHAR THEN
  UPDATE SET tgt.valid_to = CURRENT_TIMESTAMP(), tgt.is_current = FALSE
WHEN NOT MATCHED THEN
  INSERT (device_id, status, valid_from, valid_to, is_current)
  VALUES (src.raw_payload:device_id, src.raw_payload:status, CURRENT_TIMESTAMP(), '9999-12-31', TRUE);`
  },
  p1_gold: {
    title: "Gold Layer (Dynamic Tables & Star Schema)",
    tech: "Snowflake // Declarative Dynamic Tables",
    badge: "Production Marts",
    badgeColor: "bg-emerald-50 text-emerald-800 border-emerald-200",
    sla: "Sub-Second Query Latency",
    description: "Materialized star schema marts clustered by organization and metric date, automatically refreshed with declared target lag for executive reporting.",
    code: `CREATE OR REPLACE DYNAMIC TABLE gold_marts.dt_device_health_summary
  TARGET_LAG = '10 minutes'
  WAREHOUSE = REPORTING_WH
AS
SELECT 
  d.region_code,
  DATE_TRUNC('hour', f.created_at) AS metric_hour,
  COUNT(DISTINCT f.device_key)    AS active_devices,
  SUM(f.scan_volume)              AS total_scans
FROM gold_marts.fact_scanner_events f
JOIN curated_zone.dim_devices_scd2 d 
  ON f.device_key = d.surrogate_key AND d.is_current = TRUE
GROUP BY 1, 2;`
  },

  // Project 2 Nodes
  p2_staging: {
    title: "Staging View Abstraction",
    tech: "Snowflake // SQL View Layer",
    badge: "Clean Typing & Source Isolation",
    badgeColor: "bg-blue-50 text-blue-800 border-blue-200",
    sla: "Instant Views",
    description: "Lightweight view layer casting raw column names, enforcing standard snake_case conventions, and handling timezones before modeling.",
    code: `CREATE OR REPLACE VIEW staging.stg_orders AS
SELECT 
  order_id::VARCHAR       AS order_id,
  customer_id::VARCHAR    AS customer_id,
  order_amount::FLOAT     AS order_amount,
  created_at::TIMESTAMP   AS order_timestamp
FROM raw.orders;`
  },
  p2_dbt: {
    title: "dbt Core Modular DAG Models",
    tech: "dbt Core // Incremental SQL Pipelines",
    badge: "Modular Transformation",
    badgeColor: "bg-orange-50 text-orange-800 border-orange-200",
    sla: "Incremental Micro-batches",
    description: "Transforms modular SQL models with {{ config(materialized='incremental') }} using merge keys to avoid full table scans on multi-million row datasets.",
    code: `{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

SELECT * FROM {{ ref('stg_orders') }}
{% if is_incremental() %}
  WHERE order_timestamp > (SELECT MAX(order_timestamp) FROM {{ this }})
{% endif %}`
  },
  p2_testing: {
    title: "Automated Data Contracts & Testing Gate",
    tech: "dbt test // CI/CD Validation Gate",
    badge: "Automated Data Governance",
    badgeColor: "bg-purple-50 text-purple-800 border-purple-200",
    sla: "Pre-deployment Gate",
    description: "Guarantees data integrity by running automated schema checks: unique constraints, not_null column assertions, and foreign key relationships.",
    code: `models:
  - name: fct_orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: customer_id
        tests:
          - relationships:
              to: ref('dim_customers')
              field: customer_id`
  },
  p2_marts: {
    title: "Kimball Star Schema Data Mart",
    tech: "Snowflake // Dimensional Architecture",
    badge: "Optimized BI Serving",
    badgeColor: "bg-emerald-50 text-emerald-800 border-emerald-200",
    sla: "Sub-second Analytics",
    description: "Denormalized fact and dimension models optimized for high-concurrency analytical queries, reducing business report load time by 40%.",
    code: `CREATE OR REPLACE TABLE gold_analytics.fct_sales (
  sales_key           INT IDENTITY(1,1),
  order_id            VARCHAR(64) NOT NULL,
  customer_key        INT NOT NULL,
  date_key            INT NOT NULL,
  order_amount        NUMBER(12,2),
  discount_amount     NUMBER(10,2)
)
CLUSTER BY (date_key, customer_key);`
  },

  // Project 3 Nodes
  p3_airflow: {
    title: "Apache Airflow Orchestrator",
    tech: "Apache Airflow // Python DAG Workflow",
    badge: "Enterprise Scheduling",
    badgeColor: "bg-teal-50 text-teal-800 border-teal-200",
    sla: "Automated Retries & SLAs",
    description: "Centrally orchestrates dependencies across extraction, transformation, data validation gates, and alerting sensors with full lineage tracking.",
    code: `from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

with DAG('lakehouse_curation_daily', schedule_interval='@daily') as dag:
    run_staging = SnowflakeOperator(task_id='run_staging', sql='CALL proc_staging();')
    run_dbt = SnowflakeOperator(task_id='run_dbt', sql='CALL proc_dbt_run();')
    run_staging >> run_dbt`
  },
  p3_warehouses: {
    title: "Dedicated Virtual Warehouses",
    tech: "Snowflake // Multi-Cluster Compute Isolation",
    badge: "Workload Segregation",
    badgeColor: "bg-blue-50 text-blue-800 border-blue-200",
    sla: "Zero Contention",
    description: "Separates ingestion, transformation, and reporting workloads onto dedicated warehouses with aggressive 60s auto-suspend rules, cutting credit burn by 30%.",
    code: `CREATE OR REPLACE WAREHOUSE transform_wh WITH
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  STATEMENT_TIMEOUT_IN_SECONDS = 1800;`
  },
  p3_finops: {
    title: "Query Profiler & Clustering FinOps",
    tech: "Snowflake FinOps // Micro-Partition Tuning",
    badge: "Cost Optimization",
    badgeColor: "bg-amber-50 text-amber-800 border-amber-200",
    sla: "Eliminated Spilling",
    description: "Identifies query bottlenecks through Query Profiler, optimizes micro-partition pruning via clustering keys, and eliminates local/remote disk spilling.",
    code: `-- Altering table clustering to prune 90%+ micro-partitions
ALTER TABLE gold_analytics.fct_sales 
  CLUSTER BY (date_key, customer_key);

-- Inspecting clustering depth
SELECT SYSTEM$CLUSTERING_INFORMATION('gold_analytics.fct_sales');`
  },
  p3_clone: {
    title: "Zero-Copy Cloning & Time Travel",
    tech: "Snowflake // Instant Metadata Duplication",
    badge: "Zero-Storage Staging",
    badgeColor: "bg-indigo-50 text-indigo-800 border-indigo-200",
    sla: "Instant Sandbox (< 10s)",
    description: "Enables instant replication of production databases for dev/QA testing without duplicating physical storage costs, alongside 90-day point-in-time recovery.",
    code: `-- Instant Zero-Copy Clone for isolated QA testing
CREATE OR REPLACE DATABASE dev_qa_lake CLONE prod_lake;

-- Point-in-time recovery via Time Travel
SELECT * FROM prod_lake.curated.fct_sales 
  AT (OFFSET => -60*15); -- 15 minutes ago`
  }
};

function initArchitectureInteractivity() {
  document.querySelectorAll('.arch-node').forEach(node => {
    node.addEventListener('click', () => {
      const pId = node.dataset.project;
      const nId = node.dataset.node;
      const data = architectureDetails[nId];
      if (!data) return;

      const container = node.closest('.arch-diagram-container');
      if (container) {
        container.querySelectorAll('.arch-node').forEach(n => n.classList.remove('active-node'));
      }
      node.classList.add('active-node');

      const inspector = document.getElementById(`inspector-${pId}`);
      if (!inspector) return;

      const titleEl = inspector.querySelector('.inspector-title');
      const techEl = inspector.querySelector('.inspector-tech');
      const badgeEl = inspector.querySelector('.inspector-badge');
      const slaEl = inspector.querySelector('.inspector-sla');
      const descEl = inspector.querySelector('.inspector-desc');
      const codeEl = inspector.querySelector('.inspector-code');

      if (titleEl) titleEl.textContent = data.title;
      if (techEl) techEl.textContent = data.tech;
      if (badgeEl) {
        badgeEl.textContent = data.badge;
        badgeEl.className = `inspector-badge px-2.5 py-0.5 text-xs font-semibold rounded-full border ${data.badgeColor}`;
      }
      if (slaEl) slaEl.textContent = data.sla;
      if (descEl) descEl.textContent = data.description;
      if (codeEl) codeEl.textContent = data.code;
    });
  });
}

function initResumeModal() {
  const modal = document.getElementById('resume-modal');
  const openBtns = document.querySelectorAll('.open-resume-modal');
  const closeBtn = document.getElementById('close-resume-modal');

  if (!modal) return;

  openBtns.forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.preventDefault();
      modal.classList.remove('hidden');
      document.body.style.overflow = 'hidden';
    });
  });

  if (closeBtn) {
    closeBtn.addEventListener('click', () => {
      modal.classList.add('hidden');
      document.body.style.overflow = 'auto';
    });
  }

  modal.addEventListener('click', (e) => {
    if (e.target === modal) {
      modal.classList.add('hidden');
      document.body.style.overflow = 'auto';
    }
  });
}

function initMobileNav() {
  const menuBtn = document.getElementById('mobile-menu-btn');
  const mobileMenu = document.getElementById('mobile-menu');

  if (menuBtn && mobileMenu) {
    menuBtn.addEventListener('click', () => {
      mobileMenu.classList.toggle('hidden');
    });

    mobileMenu.querySelectorAll('a').forEach(link => {
      link.addEventListener('click', () => {
        mobileMenu.classList.add('hidden');
      });
    });
  }
}

document.addEventListener('DOMContentLoaded', () => {
  initArchitectureInteractivity();
  initResumeModal();
  initMobileNav();
});
