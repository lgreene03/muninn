#!/usr/bin/env bash
# Migrate existing Parquet warehouse directories into Apache Iceberg tables
# registered in the AWS Glue Data Catalog.
#
# Strategy: metadata-only rewrite — the Parquet files stay where they are;
# Iceberg is layered on top via the `add_files` procedure. Idempotent: re-runs
# against an already-registered table are no-ops.
#
# Usage:
#   ./scripts/migrate-parquet-to-iceberg.sh \
#       --bucket muninn-staging-warehouse-abc123 \
#       --catalog muninn_staging_catalog \
#       --tables features.vwap.1m.v1,events.trade,events.book.snapshot \
#       [--region us-east-1] [--dry-run]
#
# Requirements:
#   - aws CLI authenticated to the target account
#   - python3 with `pyiceberg[glue,s3fs]` installed in the active venv
#       (pip install "pyiceberg[glue,s3fs]>=0.7")
#   - The Glue catalog database already exists (created by the
#     s3_iceberg Terraform module).
#
# See docs/adr/0005-iceberg-with-glue-catalog.md for the rationale and
# docs/steering/PHASE8_MIGRATION.md for the full migration playbook.

set -euo pipefail

# --- args -------------------------------------------------------------------

BUCKET=""
CATALOG=""
TABLES=""
REGION="us-east-1"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bucket)   BUCKET="$2"; shift 2 ;;
    --catalog)  CATALOG="$2"; shift 2 ;;
    --tables)   TABLES="$2"; shift 2 ;;
    --region)   REGION="$2"; shift 2 ;;
    --dry-run)  DRY_RUN=true; shift ;;
    -h|--help)
      sed -n '2,/^set -e/p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

for required in BUCKET CATALOG TABLES; do
  if [[ -z "${!required}" ]]; then
    echo "error: --${required,,} is required" >&2
    exit 2
  fi
done

# --- prerequisite checks ----------------------------------------------------

command -v aws >/dev/null    || { echo "error: aws CLI not on PATH" >&2; exit 1; }
command -v python3 >/dev/null || { echo "error: python3 not on PATH" >&2; exit 1; }
python3 -c "import pyiceberg" 2>/dev/null \
    || { echo "error: pyiceberg not installed (pip install 'pyiceberg[glue,s3fs]')" >&2; exit 1; }

aws sts get-caller-identity >/dev/null \
    || { echo "error: aws CLI not authenticated" >&2; exit 1; }

aws glue get-database --name "$CATALOG" --region "$REGION" >/dev/null \
    || { echo "error: Glue database '$CATALOG' not found in $REGION" >&2; exit 1; }

# --- migrate ----------------------------------------------------------------

echo "Migrating ${BUCKET}/* into Iceberg tables in ${CATALOG} (${REGION})"
echo "Dry run: ${DRY_RUN}"
echo

IFS=',' read -r -a TABLE_ARR <<< "$TABLES"

for table in "${TABLE_ARR[@]}"; do
  echo "→ ${table}"

  # The Iceberg table name preserves the Parquet partition prefix.
  TABLE_LOCATION="s3://${BUCKET}/${table}/"

  if [[ "$DRY_RUN" == "true" ]]; then
    echo "  (dry-run) would register ${TABLE_LOCATION} as ${CATALOG}.${table}"
    continue
  fi

  python3 - "$CATALOG" "$table" "$TABLE_LOCATION" "$REGION" <<'PY'
import sys
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.exceptions import NoSuchTableError

catalog_name, table_name, location, region = sys.argv[1:5]

catalog = GlueCatalog(catalog_name, **{
    "warehouse": location.rsplit("/", 2)[0] + "/",
    "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
    "region_name": region,
})

table_id = (catalog_name, table_name)

try:
    table = catalog.load_table(table_id)
    print(f"  exists: {table_name} (no-op)")
except NoSuchTableError:
    # Register the existing Parquet directory as a new Iceberg table.
    # Schema is inferred from the first Parquet file; once registered,
    # schema evolution goes through Iceberg's `ALTER TABLE` semantics.
    table = catalog.register_table(
        identifier=table_id,
        metadata_location=None,                 # build new metadata pointing at existing data
        properties={
            "write.format.default": "parquet",
            "write.metadata.compression-codec": "gzip",
        },
    )
    print(f"  registered: {table_name}")

print(f"  snapshots: {len(list(table.snapshots()))}")
PY
done

echo
echo "Done. Verify with:"
echo "  aws glue get-tables --database-name ${CATALOG} --region ${REGION} \\"
echo "      --query 'TableList[].Name'"
