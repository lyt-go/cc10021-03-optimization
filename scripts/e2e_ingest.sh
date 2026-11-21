#!/usr/bin/env bash
set -euo pipefail
BASE=http://127.0.0.1:8000

curl -s "$BASE/api/health"; echo

SCHEMA='{
  "schema_result": {
    "数据源类型": "mixed_batch",
    "采集批号": "HTTP-BATCH-1",
    "采集时间": "2026-11-21T00:00:00+00:00",
    "表清单": [
      {"表名称":"orders","所属库名":"mysql","表类型":"TABLE","预估行数":1000,"字段列表":[
        {"字段名称":"id","数据类型":"BIGINT","可为空":false,"是否主键":true},
        {"字段名称":"amount","数据类型":"DECIMAL(18,2)","可为空":true,"是否主键":false}]},
      {"表名称":"orders","所属库名":"postgresql","表类型":"TABLE","预估行数":2000,"字段列表":[
        {"字段名称":"order_no","数据类型":"VARCHAR(255)","可为空":false,"是否主键":false}]}
    ]
  }
}'

echo "== 1. 批次入库 =="
RECEIPT=$(curl -s -X POST "$BASE/api/ingest/batch" -H 'Content-Type: application/json' -d "$SCHEMA")
echo "$RECEIPT" | python3 -m json.tool
RCPT=$(echo "$RECEIPT" | python3 -c 'import sys,json;print(json.load(sys.stdin)["回执编号"])')

echo "== 2. 重放批号 =="
curl -s -X POST "$BASE/api/ingest/batch" -H 'Content-Type: application/json' -d "$SCHEMA" \
  | python3 -c 'import sys,json;d=json.load(sys.stdin);print("重复提交:",d.get("重复提交"),"回执:",d["回执编号"])'

echo "== 3. 回查回执 =="
curl -s "$BASE/api/ingest/receipts/$RCPT" | python3 -c 'import sys,json;d=json.load(sys.stdin);print([(i["所属库名"],i["表名称"],i["状态"],i["画像轮次"]) for i in d["分项明细"]])'

echo "== 4. 补齐前 pg.orders 发起画像（应被拒） =="
curl -s -X POST "$BASE/api/ingest/profiles/start" -H 'Content-Type: application/json' \
  -d '{"db_name":"postgresql","table_name":"orders"}' | python3 -m json.tool

echo "== 5. mysql.orders 第一轮画像并确认 =="
curl -s -X POST "$BASE/api/ingest/receipts/profile" -H 'Content-Type: application/json' \
  -d "{\"receipt_no\":\"$RCPT\",\"table_name\":\"orders\",\"db_name\":\"mysql\"}" | python3 -m json.tool
curl -s -X POST "$BASE/api/ingest/profiles/confirm" -H 'Content-Type: application/json' \
  -d '{"db_name":"mysql","table_name":"orders"}' | python3 -c 'import sys,json;d=json.load(sys.stdin);print("确认:",d["已确认"],"任务数:",d["确认任务数"],"轮次:",d["画像轮次"])'

echo "== 6. 补录 pg.orders 主键 =="
SUPP=$(cat <<EOF
{"receipt_no":"$RCPT","table_name":"orders","db_name":"postgresql","row_count":2000,"columns":[
  {"字段名称":"order_id","数据类型":"BIGINT","可为空":false,"是否主键":true},
  {"字段名称":"order_no","数据类型":"VARCHAR(255)","可为空":false,"是否主键":false},
  {"字段名称":"amount","数据类型":"NUMERIC","可为空":true,"是否主键":false}]}
EOF
)
curl -s -X POST "$BASE/api/ingest/supplement" -H 'Content-Type: application/json' -d "$SUPP" \
  | python3 -c 'import sys,json;d=json.load(sys.stdin);print("补录:",d["补录结果"],"画像轮次:",d["画像轮次"],"待处理:",d["画像待处理数"])'

echo "== 7. pg.orders 第二轮画像 =="
curl -s -X POST "$BASE/api/ingest/receipts/profile" -H 'Content-Type: application/json' \
  -d "{\"receipt_no\":\"$RCPT\",\"table_name\":\"orders\",\"db_name\":\"postgresql\"}" | python3 -m json.tool

echo "== 8. 回执终态与目录/画像清单 =="
curl -s "$BASE/api/ingest/receipts/$RCPT" | python3 -c 'import sys,json;d=json.load(sys.stdin);print("回执状态:",d["status"],"轮次表:",[(i["所属库名"],i["画像轮次"]) for i in d["分项明细"]])'
curl -s "$BASE/api/table_catalog" | python3 -c 'import sys,json;d=json.load(sys.stdin);print("目录:",[(x["所属库名"],x["表名称"],x["表编号"]) for x in d["items"]])'
curl -s "$BASE/api/column_profile?status=待处理" | python3 -c 'import sys,json;d=json.load(sys.stdin);print("画像待处理:",[(x["所属库名"],x["关联表名"],x["字段名称"],x["画像轮次"]) for x in d["items"]])'
curl -s "$BASE/api/ingest_receipt" | python3 -c 'import sys,json;d=json.load(sys.stdin);print("回执列表 total:",d["total"])'
