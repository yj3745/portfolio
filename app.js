// Yash Jain - Data Engineer Portfolio Controller (Light Theme)

const architectureDetails = {
  // =========================================================================
  // PROJECT 1: Snowflake-Native End-to-End Pipeline
  // S3 -> Storage Integration -> External Stage -> Snowpipe -> Bronze -> CDC (Stream/Task) -> Silver (Deduplication) -> DQ Gate -> Gold (DIM & FACT SCD2)
  // =========================================================================
  p1_s3_storage: {
    title: "AWS S3 & Storage Integration",
    tech: "AWS S3 // Snowflake STORAGE INTEGRATION",
    badge: "Cloud IAM Authentication",
    badgeColor: "bg-amber-50 text-amber-800 border-amber-200",
    sla: "< 100ms File Event",
    description: "Configures cross-account IAM Role authentication via Snowflake STORAGE INTEGRATION. Decouples AWS cloud storage credentials from Snowflake database users, enabling automated and secure access to the target S3 data lake bucket.",
    code: `-- 1. Production Storage Integration using IAM Role ARN
CREATE OR REPLACE STORAGE INTEGRATION s3_lake_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake_s3_lake_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://enterprise-data-lake-prod/telemetry/');

-- 2. Inspect external credentials for AWS IAM trust policy
DESC INTEGRATION s3_lake_integration;`
  },
  p1_stage: {
    title: "External Stage & File Format",
    tech: "Snowflake Named Stage // JSON / Parquet Parsing",
    badge: "Schema-on-Read Stage",
    badgeColor: "bg-sky-50 text-sky-800 border-sky-200",
    sla: "Zero Ingestion Lag",
    description: "Named external stage bound to the storage integration, targeting partitioned S3 directory paths with explicit JSON file format definitions and metadata extraction rules.",
    code: `-- Reusable File Format for semi-structured payloads
CREATE OR REPLACE FILE FORMAT raw_lake.json_file_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE
  ENABLE_OCTAL = FALSE
  ALLOW_DUPLICATE = FALSE;

-- Named External Stage mounted to S3 telemetry prefix
CREATE OR REPLACE STAGE raw_lake.ext_stage_s3_telemetry
  STORAGE_INTEGRATION = s3_lake_integration
  URL = 's3://enterprise-data-lake-prod/telemetry/'
  FILE_FORMAT = raw_lake.json_file_format;`
  },
  p1_snowpipe: {
    title: "Snowpipe (Auto-Ingest)",
    tech: "Snowflake Serverless // SQS Notification Channel",
    badge: "Micro-Batch Auto-Ingest",
    badgeColor: "bg-pink-50 text-pink-800 border-pink-200",
    sla: "< 60s Micro-batch SLA",
    description: "Serverless continuous ingestion service triggered by AWS S3 ObjectCreated events via Amazon SQS notifications. Ingests raw files continuously into Bronze tables with zero virtual warehouse idle compute charges.",
    code: `-- Serverless Snowpipe with SQS auto-ingest enabled
CREATE OR REPLACE PIPE raw_lake.pipe_telemetry_autoingest
  AUTO_INGEST = TRUE
AS
COPY INTO raw_lake.bronze_telemetry_raw (
  raw_payload,
  metadata_filename,
  metadata_row_number,
  ingested_at
)
FROM (
  SELECT 
    $1,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    CURRENT_TIMESTAMP()
  FROM @raw_lake.ext_stage_s3_telemetry
);`
  },
  p1_bronze: {
    title: "Bronze Layer (Raw VARIANT)",
    tech: "Snowflake // Append-Only VARIANT Storage",
    badge: "Raw Immutable Zone",
    badgeColor: "bg-amber-50 text-amber-800 border-amber-200",
    sla: "Near Real-Time",
    description: "Immutable, append-only staging table that stores raw semi-structured JSON directly into Snowflake VARIANT columns, capturing complete file lineage, row indices, and precise audit load timestamps.",
    code: `CREATE OR REPLACE TABLE raw_lake.bronze_telemetry_raw (
  raw_payload         VARIANT NOT NULL,
  metadata_filename   VARCHAR(512) NOT NULL,
  metadata_row_number NUMBER NOT NULL,
  ingested_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);`
  },
  p1_cdc: {
    title: "CDC (Streams & Serverless Tasks)",
    tech: "Snowflake Streams // Scheduled Serverless Tasks",
    badge: "Change Data Capture Engine",
    badgeColor: "bg-indigo-50 text-indigo-800 border-indigo-200",
    sla: "Cron / Offset Event",
    description: "Append-only Snowflake Streams monitor delta inserts on Bronze tables with zero storage overhead. Scheduled serverless Tasks trigger transformations conditionally only when SYSTEM$STREAM_HAS_DATA returns TRUE.",
    code: `-- Append-only stream capturing new records in Bronze
CREATE OR REPLACE STREAM raw_lake.stream_bronze_telemetry
  ON TABLE raw_lake.bronze_telemetry_raw
  APPEND_ONLY = TRUE;

-- Serverless task triggering every 5 minutes when delta exists
CREATE OR REPLACE TASK curated_zone.task_silver_cdc_process
  SCHEDULE = '5 MINUTE'
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
WHEN
  SYSTEM$STREAM_HAS_DATA('raw_lake.stream_bronze_telemetry')
AS
  CALL curated_zone.sp_process_silver_telemetry();`
  },
  p1_silver: {
    title: "Silver Layer (Deduplication & Cleanse)",
    tech: "Snowflake SQL // QUALIFY ROW_NUMBER() Deduplication",
    badge: "Curated & Enriched Zone",
    badgeColor: "bg-blue-50 text-blue-800 border-blue-200",
    sla: "Sub-5 Minute SLA",
    description: "Extracts nested JSON fields, applies strong typing, handles timezone conversions, and guarantees 100% deduplication using QUALIFY ROW_NUMBER() window partitioning on business keys before committing.",
    code: `-- Silver transformation with windowed deduplication
INSERT INTO curated_zone.silver_device_telemetry
SELECT 
  raw_payload:device_id::VARCHAR(64)       AS device_id,
  raw_payload:event_id::VARCHAR(64)        AS event_id,
  raw_payload:status::VARCHAR(32)          AS device_status,
  raw_payload:battery_pct::FLOAT           AS battery_percentage,
  raw_payload:temperature_c::FLOAT         AS temperature_c,
  raw_payload:event_time::TIMESTAMP_NTZ    AS event_timestamp,
  metadata_filename                        AS source_file,
  CURRENT_TIMESTAMP()                      AS silver_loaded_at
FROM raw_lake.stream_bronze_telemetry
WHERE raw_payload:device_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY raw_payload:device_id, raw_payload:event_id 
  ORDER BY raw_payload:event_time DESC
) = 1;`
  },
  p1_dq: {
    title: "Automated Data Quality Gate",
    tech: "Snowflake SQL // Constraints & Quarantine Routing",
    badge: "Pre-Publish Governance Gate",
    badgeColor: "bg-rose-50 text-rose-800 border-rose-200",
    sla: "100% Constraint Enforcement",
    description: "Automated programmatic validation asserting primary key uniqueness, non-null values, metric range bounds (e.g., battery 0-100%), and routing anomalous records into quarantine tables prior to Gold promotion.",
    code: `-- Programmatic Data Quality Gate Stored Procedure
CREATE OR REPLACE PROCEDURE curated_zone.sp_validate_silver_quality()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
  -- Quarantine corrupted or out-of-boundary sensor records
  INSERT INTO curated_zone.quarantine_telemetry
  SELECT * FROM curated_zone.silver_device_telemetry
  WHERE device_id IS NULL 
     OR battery_percentage < 0 
     OR battery_percentage > 100
     OR event_timestamp > CURRENT_TIMESTAMP()
     OR event_timestamp < '2020-01-01'::TIMESTAMP_NTZ;

  -- Delete quarantined records from silver before gold promotion
  DELETE FROM curated_zone.silver_device_telemetry
  WHERE device_id IS NULL OR battery_percentage < 0 OR battery_percentage > 100;
  
  RETURN 'SUCCESS: Quality gate verified and quarantine routed.';
END;
$$;`
  },
  p1_gold: {
    title: "Gold Layer (Kimball Star Schema & SCD2)",
    tech: "Snowflake // Conformed Dimensions, Facts & SCD2",
    badge: "Production BI Marts",
    badgeColor: "bg-emerald-50 text-emerald-800 border-emerald-200",
    sla: "Sub-Second Query Latency",
    description: "Enterprise dimensional models with Slowly Changing Dimensions Type 2 (SCD2) tracking historical device states via MD5 hash-diff detection, alongside clustered Daily Fact tables optimized for BI analytical tools.",
    code: `-- SCD Type 2 Dimension Merge with Hash-Diff Change Detection
MERGE INTO gold_marts.dim_device_scd2 AS tgt
USING (
  SELECT 
    device_id,
    device_status,
    MD5(CONCAT_WS('||', device_id, device_status)) AS record_hash,
    event_timestamp
  FROM curated_zone.silver_device_telemetry
) AS src
ON tgt.device_id = src.device_id AND tgt.is_current = TRUE
-- Expire changed existing record
WHEN MATCHED AND tgt.record_hash <> src.record_hash THEN
  UPDATE SET tgt.valid_to = src.event_timestamp, tgt.is_current = FALSE
-- Insert brand new record or updated state version
WHEN NOT MATCHED THEN
  INSERT (device_id, device_status, record_hash, valid_from, valid_to, is_current)
  VALUES (src.device_id, src.device_status, src.record_hash, src.event_timestamp, '9999-12-31', TRUE);

-- Clustered Daily Telemetry Fact Table
CREATE OR REPLACE TABLE gold_marts.fct_device_telemetry_daily (
  device_key          INT NOT NULL,
  date_key            INT NOT NULL,
  total_events        NUMBER(10,0),
  avg_temperature     NUMBER(6,2),
  min_battery_pct     NUMBER(5,2),
  created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) CLUSTER BY (date_key, device_key);`
  },

  // =========================================================================
  // PROJECT 2: Enterprise Analytics Engineering & Orchestration
  // AWS S3 Landing -> Apache Airflow TaskFlow -> dbt Staging & Contracts -> dbt Incremental Merge -> dbt Test Gates -> Snowflake Clustered Marts & FinOps CI/CD
  // =========================================================================
  p2_aws_s3: {
    title: "AWS S3 Landing Zone & Partitioning",
    tech: "Amazon Web Services // S3 Bucket & Event Triggers",
    badge: "Cloud Ingestion Staging",
    badgeColor: "bg-amber-50 text-amber-800 border-amber-200",
    sla: "Batch SLA (< 5 Mins)",
    description: "S3 object storage receiving partitioned Parquet and CSV extracts from ERP/CRM systems, secured with KMS encryption and IAM least-privilege policies.",
    code: `# AWS S3 Partitioned Ingestion Hierarchy
s3://enterprise-analytics-landing-prod/
  ├── erp_orders/year=2026/month=10/day=13/orders_part_01.parquet
  ├── crm_customers/year=2026/month=10/customers_delta.parquet
  └── clickstream/year=2026/month=10/events_batch.parquet

# S3 Event Notification triggers Airflow DAG or SQS sensor
NotificationConfiguration:
  QueueConfigurations:
    - QueueArn: "arn:aws:sqs:us-east-1:123456789012:airflow-s3-landing"
      Events: ["s3:ObjectCreated:*"]
      Filter: { Prefix: "erp_orders/" }`
  },
  p2_airflow: {
    title: "Apache Airflow Orchestration (TaskFlow)",
    tech: "Apache Airflow // TaskFlow API & Snowflake Providers",
    badge: "Enterprise DAG Orchestrator",
    badgeColor: "bg-teal-50 text-teal-800 border-teal-200",
    sla: "Automated Retries & Webhooks",
    description: "Directed Acyclic Graph orchestrating S3 landing checks, Snowflake COPY stages, dbt run/test task groups, and Slack/PagerDuty failure alerting with exponential backoff.",
    code: `from airflow.decorators import dag, task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from datetime import datetime, timedelta

@dag(
    schedule_interval='0 2 * * *', # Daily 02:00 AM UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)}
)
def enterprise_analytics_pipeline():
    # Step 1: Stage raw landing files to Snowflake
    copy_to_snowflake = SnowflakeOperator(
        task_id='copy_s3_to_snowflake',
        snowflake_conn_id='snowflake_prod',
        sql='COPY INTO raw_lake.orders_landing FROM @raw_lake.ext_s3_stage;'
    )

    # Step 2: Cosmos / dbt Task Group executing models and tests
    dbt_execution = DbtTaskGroup(
        group_id='dbt_enterprise_transformations',
        project_config=ProjectConfig('/opt/airflow/dbt/enterprise_analytics'),
        profile_config=ProfileConfig(profile_name='snowflake_analytics')
    )

    copy_to_snowflake >> dbt_execution

pipeline = enterprise_analytics_pipeline()`
  },
  p2_dbt_staging: {
    title: "dbt Staging Models & Schema Contracts",
    tech: "dbt Core // Modular Staging & Data Contracts",
    badge: "Raw Abstraction Layer",
    badgeColor: "bg-blue-50 text-blue-800 border-blue-200",
    sla: "Schema Protection",
    description: "Standardizes naming conventions, parses timestamps, enforces strict column datatypes via dbt Model Contracts, and monitors source table freshness before transformations.",
    code: `-- models/staging/stg_erp_orders.sql
{{ config(
    materialized='view',
    schema='staging',
    contract={'enforced': true}
) }}

WITH raw_source AS (
    SELECT * FROM {{ source('raw_erp', 'orders_landing') }}
)
SELECT
    order_id::VARCHAR(64)              AS order_id,
    customer_id::VARCHAR(64)           AS customer_id,
    order_status::VARCHAR(32)          AS order_status,
    total_amount::NUMBER(12, 2)        AS order_amount,
    tax_amount::NUMBER(10, 2)          AS tax_amount,
    order_timestamp::TIMESTAMP_NTZ     AS order_timestamp,
    _etl_loaded_at::TIMESTAMP_NTZ      AS loaded_at
FROM raw_source`
  },
  p2_dbt_incremental: {
    title: "dbt Incremental Marts (Merge Strategy)",
    tech: "dbt Core // is_incremental() & Merge Unique Key",
    badge: "Cost-Optimized Modeling",
    badgeColor: "bg-orange-50 text-orange-800 border-orange-200",
    sla: "85%+ Compute Reduction",
    description: "Transforms multi-million row datasets incrementally by merging only newly modified or inserted records based on watermark timestamps, eliminating expensive full-table scans.",
    code: `-- models/marts/fct_orders.sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    cluster_by=['order_date', 'customer_key'],
    schema='analytics'
) }}

WITH staged_orders AS (
    SELECT * FROM {{ ref('stg_erp_orders') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),
customers AS (
    SELECT customer_key, customer_id FROM {{ ref('dim_customers') }}
)
SELECT
    o.order_id,
    c.customer_key,
    DATE(o.order_timestamp)            AS order_date,
    o.order_status,
    o.order_amount,
    o.tax_amount,
    o.loaded_at
FROM staged_orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id`
  },
  p2_dbt_testing: {
    title: "Automated dbt Testing & Governance",
    tech: "dbt test // Generic, Singular & Relationship Tests",
    badge: "Zero-Defect Quality Gate",
    badgeColor: "bg-purple-50 text-purple-800 border-purple-200",
    sla: "Pre-Production Assertion",
    description: "Enforces automated quality gates including uniqueness, foreign key relationships, accepted values, and custom SQL test assertions before BI access.",
    code: `# models/marts/schema.yml
version: 2
models:
  - name: fct_orders
    description: "Enterprise order transactions grain at individual order level"
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: customer_key
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_key
      - name: order_amount
        tests:
          - not_null
    tests:
      - dbt_utils.expression_is_true:
          expression: "order_amount >= 0"`
  },
  p2_snowflake_marts: {
    title: "Clustered Snowflake Marts & FinOps CI/CD",
    tech: "Snowflake // Micro-Partition Clustering & Zero-Copy Clone",
    badge: "Production BI & FinOps",
    badgeColor: "bg-emerald-50 text-emerald-800 border-emerald-200",
    sla: "Sub-Second BI Queries & 30% Savings",
    description: "Serves analytics via clustered Kimball Star Schemas, governs compute credit burn using dedicated Virtual Warehouses with 60s auto-suspend, and provisions zero-storage staging environments with Zero-Copy Cloning.",
    code: `-- Micro-partition clustering for 90%+ micro-partition pruning
ALTER TABLE analytics.fct_orders 
  CLUSTER BY (order_date, customer_key);

-- Multi-Cluster Virtual Warehouse dedicated for BI workloads
CREATE OR REPLACE WAREHOUSE reporting_wh WITH
  WAREHOUSE_SIZE = 'SMALL'
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 3
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- Zero-Copy Clone in GitHub Actions CI/CD to test dbt builds
CREATE OR REPLACE DATABASE ci_test_db CLONE analytics_prod_db;
-- Drop instantly after CI pipeline passes
DROP DATABASE ci_test_db;`
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
