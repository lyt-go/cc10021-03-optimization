"""元数据采集器服务：采集数据源schema并解析入库，驱动表目录注册和字段画像计算。

domain_chain: 采集器配置 (metadata_crawler)
  └─ 采集 → 接入回执 (ingest_receipt) → 表目录注册 (table_catalog)
  └─ 解析 → 画像待处理清单 (column_profile) → 字段画像计算

批次入库口径（本模块的优化重点）：
- 每批只建立一次（库名, 表名）目录索引，登记全程 O(1) 查表，不再每登记一张表就重扫目录；
  目录 ID 由 store 统一取号，避免逐条 max(id) 全表扫描。
- 同库同表幂等：重放批次/并发导入只确认一次，重复登记返回原登记记录与原接入回执。
- 每张表逐条落到接入回执分项（已接收 / 待补正），缺主键不丢表，支持补录主键后二次入库。
- 确认登记的表用回执字段建立目录字段登记，并把画像任务幂等回写到画像待处理清单；
  回执分项记录画像轮次，可解释同名表分别在哪一轮进入计算。
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.column_profile import ColumnProfileService
from app.services.table_catalog import TableCatalogService
from app.store import store


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class MetadataCrawlerService(BaseModuleService):
    MODULE = "metadata_crawler"
    ENTITY = "采集器配置"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["采集器编号", "数据源类型", "连接地址", "采集范围", "采集深度", "更新频率", "增量字段", "采集状态"]
    REQUIRED_FIELDS = ["采集器编号", "数据源类型", "连接地址"]
    STATUS_ORDER = ["草稿", "待校验", "采集中", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "启动采集": "采集中", "停用采集": "已停用"}
    NEGATIVE_ACTIONS = ["停用采集"]

    RECEIPT_MODULE = "ingest_receipt"

    def __init__(self) -> None:
        self.catalog_service = TableCatalogService()
        self.profile_service = ColumnProfileService()

    # ------------------------------------------------------------------ 采集

    def crawl_metadata_schema(self, datasource_type: str, connection_url: str, scope: str = "*") -> dict[str, Any]:
        """采集数据源元数据结构，返回解析后的schema字典。

        从连接地址采集表信息、字段定义和类型映射，将采集到的元数据结构解析为标准化的schema格式。
        采集结果包含表列表、每个表的列定义和约束信息，用于驱动下游表目录注册。
        """
        schema_hash = hashlib.sha256(f"{datasource_type}:{connection_url}:{scope}".encode()).hexdigest()[:12]
        tables = self._simulate_crawl_tables(datasource_type, scope)
        crawled_at = _now()
        for tbl in tables:
            tbl["采集批号"] = schema_hash
            tbl["采集时间"] = crawled_at
            # 采集表字段元数据
            tbl["字段列表"] = self._simulate_column_discovery(datasource_type, tbl["表名称"])
        return {
            "数据源类型": datasource_type,
            "采集时间": crawled_at,
            "采集批号": schema_hash,
            "表数量": len(tables),
            "表清单": tables,
        }

    def _simulate_crawl_tables(self, ds_type: str, scope: str) -> list[dict[str, Any]]:
        """模拟从数据源采集表列表——解析表名、表类型和分区信息。"""
        base_tables = {
            "mysql": [
                ("ods_order_info", "BIGINT", 8560000),
                ("ods_user_profile", "VARCHAR", 12500000),
                ("dwd_trade_detail", "DECIMAL", 42000000),
                ("dws_daily_summary", "TIMESTAMP", 980000),
                ("dim_product_sku", "VARCHAR", 230000),
            ],
            "hive": [
                ("ads_sale_report", "DOUBLE", 6500000),
                ("dwd_log_event", "STRING", 180000000),
                ("ods_click_stream", "STRING", 320000000),
                ("dim_date_dim", "DATE", 3650),
                ("dws_user_tag", "MAP", 45000000),
            ],
            "postgresql": [
                ("inventory_snapshot", "INTEGER", 3200000),
                ("payment_record", "NUMERIC", 18900000),
                ("refund_audit", "TEXT", 450000),
            ],
        }
        tables = base_tables.get(ds_type, base_tables["mysql"])
        return [
            {
                "表名称": name,
                "表类型": col_type,
                "预估行数": rows,
                "分区字段": "dt" if ds_type == "hive" else None,
            }
            for name, col_type, rows in tables
        ]

    def _simulate_column_discovery(self, ds_type: str, table_name: str) -> list[dict[str, Any]]:
        """解析表字段元数据：字段名、数据类型、是否可为空、是否主键。"""
        base_cols = {
            "id": ("BIGINT", False, True),
            "name": ("VARCHAR(255)", True, False),
            "status": ("TINYINT", False, False),
            "amount": ("DECIMAL(18,2)", True, False),
            "created_at": ("TIMESTAMP", False, False),
            "updated_at": ("TIMESTAMP", True, False),
            "batch_id": ("VARCHAR(64)", True, False),
            "partition_dt": ("DATE", False, False),
        }
        col_names = list(base_cols.keys())[: min(len(base_cols), 5 + hash(table_name) % 4)]
        return [
            {
                "字段名称": col,
                "数据类型": base_cols[col][0],
                "可为空": base_cols[col][1],
                "是否主键": base_cols[col][2],
            }
            for col in col_names
        ]

    # ------------------------------------------------------------ 批次接入

    def _find_receipt_by_batch(self, batch_no: str) -> dict[str, Any] | None:
        for receipt in store.rows(self.RECEIPT_MODULE):
            if receipt.get("接入批号") == batch_no:
                return receipt
        return None

    def ingest_metadata_batch(self, schema_result: dict[str, Any]) -> dict[str, Any]:
        """接入元数据批次：幂等登记到表目录并产出可回查的接入回执。

        优化点（相对逐表扫描的旧实现）：
        1. 全程只建一次（库名, 表名）目录索引，批次内查重/取号均为 O(1)，不重扫表目录；
        2. 同库同表只确认一次——重放的源表命中幂等键，返回原表编号与原回执，不重复登记；
        3. 主键校验失败的表不丢弃，落到回执「待补正」分项，后续可补录主键再入库；
        4. 确认登记的表用回执字段建立目录字段登记，并把画像任务回写到画像待处理清单。

        重复提交同一采集批号时原样返回首次回执（含「重复提交」标记），不产生任何重复写入。
        """
        batch_no = schema_result.get("采集批号") or f"batch-{_now()}"
        with store.ingest_lock:
            existing = self._find_receipt_by_batch(batch_no)
            if existing is not None:
                returned = dict(existing)
                returned["重复提交"] = True
                returned["提示"] = "该采集批号已接入，返回首次接入回执，未重复登记目录或画像任务"
                return returned

            tables = schema_result.get("表清单", [])
            batch_source = schema_result.get("数据源类型", "")
            receipt_id = store.next_id(self.RECEIPT_MODULE)
            receipt_no = f"RCPT-{receipt_id:04d}"
            # 整批只建一次目录索引：新登记的表同步回索引，批次内后续表即时命中去重
            catalog_index = store.index_by(TableCatalogService.MODULE,
                                           TableCatalogService.BUSINESS_KEY)

            items: list[dict[str, Any]] = []
            accepted = pending = duplicate = 0
            profile_round = 1

            for tbl in tables:
                table_name = tbl["表名称"]
                # 同一批可含多个库：优先取表级所属库名，缺省回退批次数据源类型
                db_name = tbl.get("所属库名") or batch_source
                cols = tbl.get("字段列表", [])
                item: dict[str, Any] = {
                    "表名称": table_name,
                    "所属库名": db_name,
                    "字段数量": len(cols),
                    "接入回执": receipt_no,
                    "画像轮次": None,
                }

                # 校验字段完整性：检查主键
                if not any(c.get("是否主键") for c in cols):
                    item.update({
                        "状态": "待补正",
                        "表编号": None,
                        "原因": "缺少主键约束",
                    })
                    items.append(item)
                    pending += 1
                    continue

                # 幂等登记：同库同表并发/重复导入只确认一次
                entry, created = self.catalog_service.upsert_catalog_table(
                    table_name=table_name,
                    db_name=db_name,
                    columns=cols,
                    row_count=int(tbl.get("预估行数", 0) or 0),
                    table_type=tbl.get("表类型", "TABLE"),
                    receipt_no=receipt_no,
                    catalog_index=catalog_index,
                )
                item["表编号"] = entry["表编号"]
                if created:
                    # 确认结果回写画像待处理清单（幂等，同库同表同字段不重复建任务）
                    dispatched = self.catalog_service.dispatch_profile_pending(
                        entry, cols, receipt_no=receipt_no, profile_round=profile_round)
                    self.catalog_service._dispatch_tag_classification(table_name, db_name, cols)
                    item.update({"状态": "已接收", "原因": None,
                                 "画像轮次": profile_round, "画像待处理数": dispatched})
                    accepted += 1
                else:
                    item.update({"状态": "已接收", "原因": "同库同表已登记，本次只确认不重复写入",
                                 "画像轮次": profile_round})
                    duplicate += 1
                items.append(item)

            receipt_status = "全部待补正" if accepted == 0 and pending else (
                "部分待补正" if pending else "已接收")
            involved_dbs = sorted({it["所属库名"] for it in items})
            receipt = {
                "id": receipt_id,
                "回执编号": receipt_no,
                "接入批号": batch_no,
                "数据源类型": batch_source,
                "涉及库名": involved_dbs,
                "接入时间": _now(),
                "采集时间": schema_result.get("采集时间"),
                "总表数": len(tables),
                "已接收数": accepted,
                "待补正数": pending,
                "重复确认数": duplicate,
                # 兼容历史回执字段名：成功注册/校验失败/失败明细
                "成功注册": accepted,
                "校验失败": pending,
                "失败明细": [
                    {"表名称": it["表名称"], "原因": it["原因"]}
                    for it in items if it.get("状态") == "待补正"
                ],
                "当前画像轮次": profile_round,
                "status": receipt_status,
                "pending": pending > 0,
                "abnormal": False,
                "分项明细": items,
            }
            store.rows(self.RECEIPT_MODULE).append(receipt)
            return dict(receipt)

    def get_ingest_receipt(self, receipt_no: str) -> dict[str, Any] | None:
        """按回执编号打开接入回执，供目录维护人员在补录前判断哪些表已可发起画像。"""
        for receipt in store.rows(self.RECEIPT_MODULE):
            if receipt.get("回执编号") == receipt_no:
                return dict(receipt)
        return None

    def supplement_primary_key(
        self,
        receipt_no: str,
        table_name: str,
        columns: list[dict[str, Any]],
        db_name: str | None = None,
        row_count: int = 0,
    ) -> dict[str, Any]:
        """补录缺主键分项的字段信息，通过主键校验后二次入库。

        仅允许补录回执中处于「待补正」的分项；补齐主键后：
        1. 幂等登记目录表并用新字段重建字段登记；
        2. 画像任务以新轮次（当前轮次+1）回写画像待处理清单，
           回执分项记录该轮次，从而解释同名表分别在哪一轮进入计算；
        3. 回执分项由「待补正」翻转为「已接收」，回执汇总计数同步回写。
        """
        with store.ingest_lock:
            receipt = next(
                (r for r in store.rows(self.RECEIPT_MODULE) if r.get("回执编号") == receipt_no),
                None,
            )
            if receipt is None:
                raise ValueError(f"接入回执 {receipt_no} 不存在")
            target_db = db_name or receipt.get("数据源类型", "")
            item = next(
                (it for it in receipt["分项明细"]
                 if it.get("表名称") == table_name
                 and it.get("所属库名") == target_db
                 and it.get("状态") == "待补正"),
                None,
            )
            if item is None:
                raise ValueError(f"回执 {receipt_no} 中没有表 {target_db}.{table_name} 的待补正分项")
            if not any(c.get("是否主键") for c in columns):
                raise ValueError("补录字段仍缺少主键约束，无法通过主键校验")

            next_round = int(receipt.get("当前画像轮次", 1)) + 1
            catalog_index = store.index_by(TableCatalogService.MODULE,
                                           TableCatalogService.BUSINESS_KEY)
            entry, created = self.catalog_service.upsert_catalog_table(
                table_name=table_name,
                db_name=target_db,
                columns=columns,
                row_count=row_count,
                receipt_no=receipt_no,
                catalog_index=catalog_index,
            )
            # 补录会刷新字段登记（含主键标记），保证历史缺登记的目录行也能补齐
            entry["字段登记"] = self.catalog_service._build_column_registry(columns)
            entry["主键字段"] = [c.get("字段名称") for c in columns if c.get("是否主键")]
            entry["字段数量"] = len(columns)
            dispatched = self.catalog_service.dispatch_profile_pending(
                entry, columns, receipt_no=receipt_no, profile_round=next_round)
            if created:
                self.catalog_service._dispatch_tag_classification(table_name, target_db, columns)

            item.update({
                "状态": "已接收",
                "原因": None,
                "表编号": entry["表编号"],
                "字段数量": len(columns),
                "画像轮次": next_round,
                "画像待处理数": dispatched,
                "补录时间": _now(),
            })
            receipt["待补正数"] = sum(1 for it in receipt["分项明细"] if it.get("状态") == "待补正")
            receipt["已接收数"] = sum(1 for it in receipt["分项明细"] if it.get("状态") == "已接收")
            receipt["当前画像轮次"] = next_round
            receipt["status"] = "部分待补正" if receipt["待补正数"] else "已接收"
            receipt["pending"] = receipt["待补正数"] > 0
            return {
                "回执编号": receipt_no,
                "表名称": table_name,
                "所属库名": target_db,
                "表编号": entry["表编号"],
                "补录结果": "已接收",
                "画像轮次": next_round,
                "画像待处理数": dispatched,
                "回执": dict(receipt),
            }

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["operation"] = "采集"
        snapshot["desc"] = "采集数据源元数据结构并解析入库"
        snapshot["artifact"] = "表定义集合"
        snapshot["artifact_desc"] = "采集产出的表定义集合"
        return snapshot
