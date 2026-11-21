"""采集批次入库优化的端到端测试。

覆盖场景：
1. 同一批含两个库的 orders 同名表，仅一张通过主键校验；
2. 批次内只建一次目录索引、同库同表不重复登记（重放返回原回执）；
3. 同库同表并发导入只确认一次；
4. 接入回执逐条可追到已接收/待补正，历史目录数据缺字段登记仍兼容；
5. 缺主键分项可补录，补录后以新一轮进入画像待处理清单；
6. 补齐主键前后分别发起画像，回执能解释两张同名表分别在哪一轮进入计算。
"""
from __future__ import annotations

import os
import sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.column_profile import ColumnProfileService
from app.services.ingest_receipt import IngestReceiptService
from app.services.metadata_crawler import MetadataCrawlerService
from app.services.table_catalog import TableCatalogService
from app.store import store


def _reset_chain_tables() -> None:
    """清掉采集链路相关表，保证用例间互不污染（seed 数据保留其它模块）。"""
    for module in ("ingest_receipt", "table_catalog", "column_profile", "tag_classify"):
        store.rows(module).clear()


def _schema_with_two_orders() -> dict:
    """构造一批采集结果：mysql.orders 有主键，postgresql.orders 缺主键。"""
    return {
        "数据源类型": "mixed_batch",  # 批次级来源；库名由各表自带
        "采集时间": "2026-11-21T00:00:00+00:00",
        "采集批号": "BATCH-DUAL-ORDERS-1",
        "表数量": 2,
        "表清单": [
            {
                "表名称": "orders",
                "所属库名": "mysql",
                "表类型": "TABLE",
                "预估行数": 1000,
                "字段列表": [
                    {"字段名称": "id", "数据类型": "BIGINT", "可为空": False, "是否主键": True},
                    {"字段名称": "amount", "数据类型": "DECIMAL(18,2)", "可为空": True, "是否主键": False},
                ],
            },
            {
                "表名称": "orders",
                "所属库名": "postgresql",
                "表类型": "TABLE",
                "预估行数": 2000,
                "字段列表": [
                    {"字段名称": "order_no", "数据类型": "VARCHAR(255)", "可为空": False, "是否主键": False},
                    {"字段名称": "amount", "数据类型": "NUMERIC", "可为空": True, "是否主键": False},
                ],
            },
        ],
    }


class TestBatchIngestReceipt:
    def setup_method(self):
        _reset_chain_tables()
        self.crawler = MetadataCrawlerService()
        self.catalog = TableCatalogService()
        self.profile = ColumnProfileService()
        self.receipts = IngestReceiptService()

    def test_dual_orders_one_accepted_one_pending(self):
        receipt = self.crawler.ingest_metadata_batch(_schema_with_two_orders())
        assert receipt["总表数"] == 2
        assert receipt["已接收数"] == 1
        assert receipt["待补正数"] == 1
        assert receipt["status"] == "部分待补正"

        by_key = {(it["所属库名"], it["表名称"]): it for it in receipt["分项明细"]}
        assert by_key[("mysql", "orders")]["状态"] == "已接收"
        assert by_key[("mysql", "orders")]["表编号"]
        assert by_key[("postgresql", "orders")]["状态"] == "待补正"
        assert "主键" in by_key[("postgresql", "orders")]["原因"]

        # 目录里只登记了通过校验的 mysql.orders
        catalog = store.rows("table_catalog")
        assert [(r["所属库名"], r["表名称"]) for r in catalog] == [("mysql", "orders")]

    def test_catalog_entry_built_from_receipt_columns(self):
        """表目录用接入回执建立字段登记，确认结果回写画像待处理清单。"""
        receipt = self.crawler.ingest_metadata_batch(_schema_with_two_orders())
        entry = store.rows("table_catalog")[0]
        assert entry["接入回执"] == [receipt["回执编号"]]
        assert entry["主键字段"] == ["id"]
        registry = entry["字段登记"]
        assert {c["字段名称"] for c in registry} == {"id", "amount"}
        assert registry[0]["是否主键"] is True

        pending = self.profile.pending_profile_queue("mysql", "orders")
        assert len(pending) == 2
        assert all(row["status"] == "待处理" for row in pending)
        assert all(row["画像轮次"] == 1 for row in pending)
        assert {row["字段名称"] for row in pending} == {"id", "amount"}

    def test_replay_batch_returns_original_receipt_without_duplicate_writes(self):
        first = self.crawler.ingest_metadata_batch(_schema_with_two_orders())
        catalog_count_after_first = len(store.rows("table_catalog"))
        profile_count_after_first = len(store.rows("column_profile"))

        second = self.crawler.ingest_metadata_batch(_schema_with_two_orders())
        assert second["重复提交"] is True
        assert second["回执编号"] == first["回执编号"]
        # 没有任何重复目录行或重复画像任务
        assert len(store.rows("table_catalog")) == catalog_count_after_first
        assert len(store.rows("column_profile")) == profile_count_after_first

    def test_same_db_same_table_concurrent_import_confirmed_once(self):
        schema = _schema_with_two_orders()

        def import_once(_):
            return self.crawler.ingest_metadata_batch(schema)

        # 同批号并发：全部拿到同一回执，目录只落一条
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(import_once, range(8)))
        receipt_nos = {r["回执编号"] for r in results}
        assert len(receipt_nos) == 1
        mysql_orders = [
            r for r in store.rows("table_catalog")
            if r["所属库名"] == "mysql" and r["表名称"] == "orders"
        ]
        assert len(mysql_orders) == 1

        # 不同批号但同库同表重复登记：只确认，不产生第二条目录行
        schema_again = _schema_with_two_orders()
        schema_again["采集批号"] = "BATCH-DUAL-ORDERS-2"
        again = self.crawler.ingest_metadata_batch(schema_again)
        assert again["重复确认数"] == 1
        assert again["已接收数"] == 0
        item = next(it for it in again["分项明细"] if it["所属库名"] == "mysql")
        assert "只确认" in item["原因"]
        assert len(store.rows("table_catalog")) == 1
        # 原目录行回记两次接入回执
        assert store.rows("table_catalog")[0]["接入回执"] == [
            results[0]["回执编号"], again["回执编号"]]
        # 画像待处理任务仍然只有首批的两条，没有重复写入
        assert len(self.profile.pending_profile_queue("mysql", "orders")) == 2

    def test_supplement_primary_key_then_second_round_profile(self):
        receipt = self.crawler.ingest_metadata_batch(_schema_with_two_orders())
        receipt_no = receipt["回执编号"]

        # 补录前：postgresql.orders 没有待处理画像任务，发起画像被明确拒绝
        before = self.profile.start_table_profile("postgresql", "orders")
        assert before["可发起画像"] is False
        assert self.profile.pending_profile_queue("postgresql", "orders") == []

        # mysql.orders 已登记，可在补齐第二张表之前先发起第一轮画像
        first_start = self.receipts.start_profile(receipt_no, "orders", db_name="mysql")
        assert first_start["可发起画像"] is True
        assert first_start["画像轮次"] == 1
        assert first_start["发起任务数"] == 2
        confirmed = self.profile.confirm_table_profile("mysql", "orders")
        assert confirmed["已确认"] is True
        assert all(r["status"] == "已完成"
                   for r in self.profile.find_table_tasks("mysql", "orders"))

        # 补齐 postgresql.orders 的主键信息后补录
        supplemented_cols = [
            {"字段名称": "order_id", "数据类型": "BIGINT", "可为空": False, "是否主键": True},
            {"字段名称": "order_no", "数据类型": "VARCHAR(255)", "可为空": False, "是否主键": False},
            {"字段名称": "amount", "数据类型": "NUMERIC", "可为空": True, "是否主键": False},
        ]
        supplement = self.crawler.supplement_primary_key(
            receipt_no, "orders", supplemented_cols, db_name="postgresql", row_count=2000)
        assert supplement["补录结果"] == "已接收"
        assert supplement["画像轮次"] == 2

        # 回执翻转：无待补正，分项可逐条追到两张同名表的轮次
        refreshed = self.crawler.get_ingest_receipt(receipt_no)
        assert refreshed["待补正数"] == 0
        assert refreshed["status"] == "已接收"
        rounds = {(it["所属库名"], it["表名称"]): it["画像轮次"]
                  for it in refreshed["分项明细"]}
        assert rounds[("mysql", "orders")] == 1
        assert rounds[("postgresql", "orders")] == 2

        # 补录后第二张表以第二轮进入画像待处理清单
        pg_pending = self.profile.pending_profile_queue("postgresql", "orders")
        assert len(pg_pending) == 3
        assert all(row["画像轮次"] == 2 for row in pg_pending)

        second_start = self.receipts.start_profile(receipt_no, "orders", db_name="postgresql")
        assert second_start["可发起画像"] is True
        assert second_start["画像轮次"] == 2

        # 两张同名表互不串单
        assert len(self.profile.find_table_tasks("mysql", "orders")) == 2
        assert len(self.profile.find_table_tasks("postgresql", "orders")) == 3

    def test_supplement_without_pk_still_rejected(self):
        receipt = self.crawler.ingest_metadata_batch(_schema_with_two_orders())
        bad_cols = [
            {"字段名称": "order_no", "数据类型": "VARCHAR(255)", "可为空": False, "是否主键": False},
        ]
        try:
            self.crawler.supplement_primary_key(
                receipt["回执编号"], "orders", bad_cols, db_name="postgresql")
        except ValueError as exc:
            assert "主键" in str(exc)
        else:
            raise AssertionError("缺主键补录应当被拒绝")

    def test_legacy_catalog_rows_without_column_registry(self):
        """接入后兼容历史目录数据：没有字段登记/主键字段的旧行按空登记读取。"""
        store.rows("table_catalog").append({
            "id": 99, "表编号": "TABL-0099", "表名称": "legacy_t", "所属库名": "mysql",
            "表类型": "TABLE", "字段数量": 0, "数据行数": "0", "存储大小": "0 KB",
            "最后更新": "2026-01-01", "status": "已注册", "pending": False, "abnormal": False,
        })
        legacy = store.find("table_catalog", 99)
        assert self.catalog.column_registry(legacy) == []
        assert self.catalog.has_primary_key(legacy) is False

    def test_legacy_register_api_still_idempotent(self):
        """历史 register_table_with_columns 入口保留，且同库同表重复调用只登记一次。"""
        cols = [
            {"字段名称": "id", "数据类型": "BIGINT", "可为空": False, "是否主键": True},
            {"字段名称": "name", "数据类型": "VARCHAR(255)", "可为空": True, "是否主键": False},
        ]
        first = self.catalog.register_table_with_columns("ods_order_info", "mysql", cols, 100)
        second = self.catalog.register_table_with_columns("ods_order_info", "mysql", cols, 100)
        assert first["是否新登记"] is True
        assert second["是否新登记"] is False
        assert first["表编号"] == second["表编号"]
        assert len(store.rows("table_catalog")) == 1
        # 画像任务不重复
        assert len(store.rows("column_profile")) == 2

    def test_batch_index_avoids_repeated_catalog_scan(self):
        """大批次：同库同表重复出现只登记一次，且不随表数量产生重复取号扫描。

        通过重复表验证去重路径；取号走 store.next_id，目录索引整批只建一次。
        """
        tables = []
        for i in range(20):
            tables.append({
                "表名称": f"t_{i}",
                "所属库名": "mysql",
                "表类型": "TABLE",
                "预估行数": 10,
                "字段列表": [
                    {"字段名称": "id", "数据类型": "BIGINT", "可为空": False, "是否主键": True},
                ],
            })
        # 重放源表：t_0 在批次内出现两次
        tables.append(dict(tables[0]))
        schema = {
            "数据源类型": "mixed_batch",
            "采集批号": "BATCH-BIG-1",
            "采集时间": "2026-11-21T00:00:00+00:00",
            "表清单": tables,
        }
        receipt = self.crawler.ingest_metadata_batch(schema)
        assert receipt["总表数"] == 21
        assert receipt["已接收数"] == 20
        assert receipt["重复确认数"] == 1
        assert len(store.rows("table_catalog")) == 20

    def test_receipt_readable_via_service_and_traceable_per_item(self):
        receipt = self.crawler.ingest_metadata_batch(_schema_with_two_orders())
        fetched = self.receipts.get_by_receipt_no(receipt["回执编号"])
        assert fetched is not None
        statuses = {it["状态"] for it in fetched["分项明细"]}
        assert statuses == {"已接收", "待补正"}
        # 列表接口同样能查到回执
        items, total = self.receipts.list_entries(page=1, size=10)
        assert total >= 1
        assert any(r["回执编号"] == receipt["回执编号"] for r in items)
