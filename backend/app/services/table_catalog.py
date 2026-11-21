"""表目录服务：注册和管理数据表定义，驱动字段画像、标签分类和血缘关系。

domain_chain: 表目录注册 (table_catalog)
  └─ 入库 → 字段画像计算 (column_profile)
  └─ 关联 → 分类标签 (tag_classify)
  └─ 关联 → 血缘图谱 (lineage_graph)

登记口径（与采集接入回执保持一致）：
- 以（所属库名, 表名称）为业务键做幂等登记：同库同表并发/重复导入只确认一次，
  重复登记直接回原登记记录，不再产生第二条目录行。
- 目录行上挂「字段登记」（由接入回执的字段列表建立），历史目录数据没有该字段时按空登记兼容。
- 画像任务按（所属库名, 表名称, 字段名称）幂等回写到画像待处理清单，确认结果可逐条追踪。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class TableCatalogService(BaseModuleService):
    MODULE = "table_catalog"
    ENTITY = "表目录"
    OPERATION_TERM = "入库"
    LIST_FIELDS = ["表编号", "表名称", "所属库名", "表类型", "字段数量", "数据行数", "存储大小", "最后更新"]
    REQUIRED_FIELDS = ["表编号", "表名称", "所属库名"]
    STATUS_ORDER = ["草稿", "待校验", "已注册", "已下线"]
    ACTION_RULES = {"提交校验": "待校验", "确认注册": "已注册", "标记下线": "已下线"}
    NEGATIVE_ACTIONS = []

    # 目录业务键：同库同表只允许一条登记
    BUSINESS_KEY = ("所属库名", "表名称")

    def register_table_with_columns(self, table_name: str, db_name: str, columns: list[dict],
                                     row_count: int = 0) -> dict[str, Any]:
        """将采集到的表定义入库到目录并触发后续流水线（兼容历史调用口径）。

        内部走幂等登记：同库同表重复/并发导入只确认一次，已存在时直接返回原登记。
        入库后自动：
        1. 校验字段完整性（非空检查、主键检查、类型匹配）
        2. 触发字段画像计算管道（回写画像待处理清单）
        3. 触发分类标签关联
        """
        with store.ingest_lock:
            entry, created = self.upsert_catalog_table(
                table_name=table_name,
                db_name=db_name,
                columns=columns,
                row_count=row_count,
                table_type="TABLE",
                receipt_no=None,
            )
            validation_issues = self._validate_columns(columns)
            # 仅首次登记时派发下游，重复确认不重复写入画像/标签
            if created:
                profile_entries = self.dispatch_profile_pending(entry, columns)
                tag_entries = self._dispatch_tag_classification(table_name, db_name, columns)
            else:
                profile_entries = 0
                tag_entries = 0

        return {
            "注册表": table_name,
            "所属库名": db_name,
            "表编号": entry["表编号"],
            "是否新登记": created,
            "字段数量": len(columns),
            "校验问题": len(validation_issues),
            "校验详情": validation_issues,
            "画像任务数": profile_entries,
            "标签分类数": tag_entries,
        }

    def upsert_catalog_table(
        self,
        *,
        table_name: str,
        db_name: str,
        columns: list[dict],
        row_count: int = 0,
        table_type: str = "TABLE",
        receipt_no: str | None = None,
        catalog_index: dict[tuple, dict[str, Any]] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """幂等登记目录表：同库同表只确认一次，返回（目录行, 是否新登记）。

        catalog_index 由采集批次一次性建好后传入，避免每登记一张表都重新扫描目录；
        新登记行会同步写回索引，保证批次内后续表也能命中去重。
        """
        if catalog_index is None:
            catalog_index = store.index_by(self.MODULE, self.BUSINESS_KEY)
        business_key = (db_name, table_name)
        existing = catalog_index.get(business_key)
        if existing is not None:
            # 重复登记：登记来源回执补记到原目录行，不新建第二条
            if receipt_no and receipt_no not in existing.setdefault("接入回执", []):
                existing["接入回执"].append(receipt_no)
            return existing, False

        rows = store.rows(self.MODULE)
        new_id = store.next_id(self.MODULE)
        entry = {
            "id": new_id,
            "表编号": f"TABL-{new_id:04d}",
            "表名称": table_name,
            "所属库名": db_name,
            "表类型": table_type or "TABLE",
            "字段数量": len(columns),
            "数据行数": str(row_count),
            "存储大小": f"{row_count * 256 // 1024} KB",
            "最后更新": _now(),
            # 接入回执建立的字段登记：字段级元数据随目录行留存，供画像与补录回查
            "字段登记": self._build_column_registry(columns),
            "主键字段": [c.get("字段名称") for c in columns if c.get("是否主键")],
            "接入回执": [receipt_no] if receipt_no else [],
            "status": "已注册",
            "pending": False,
            "abnormal": False,
        }
        rows.append(entry)
        catalog_index[business_key] = entry
        return entry, True

    def _build_column_registry(self, columns: list[dict]) -> list[dict[str, Any]]:
        """把接入回执里的字段列表固化为目录字段登记。"""
        registry = []
        for col in columns:
            registry.append({
                "字段名称": col.get("字段名称", ""),
                "数据类型": col.get("数据类型", ""),
                "可为空": col.get("可为空"),
                "是否主键": bool(col.get("是否主键")),
            })
        return registry

    def column_registry(self, entry: dict[str, Any]) -> list[dict[str, Any]]:
        """读取目录字段登记；历史目录数据没有该字段时按空登记兼容。"""
        return list(entry.get("字段登记") or [])

    def has_primary_key(self, entry: dict[str, Any]) -> bool:
        pk_fields = entry.get("主键字段")
        if pk_fields:
            return True
        return any(col.get("是否主键") for col in self.column_registry(entry))

    def dispatch_profile_pending(
        self,
        catalog_entry: dict[str, Any],
        columns: list[dict] | None = None,
        *,
        receipt_no: str | None = None,
        profile_round: int = 1,
    ) -> int:
        """把确认登记的表字段幂等回写到画像待处理清单（column_profile，状态=待处理）。

        以（所属库名, 表名称, 字段名称）去重：同库同表重复导入不会重复建画像任务。
        返回本次新回写的画像待处理条数。
        """
        if columns is None:
            columns = self.column_registry(catalog_entry)
        db_name = catalog_entry.get("所属库名", "")
        table_name = catalog_entry.get("表名称", "")
        profile_rows = store.rows("column_profile")
        existing = {
            (row.get("所属库名", ""), row.get("关联表名", ""), row.get("字段名称", ""))
            for row in profile_rows
        }
        dispatched = 0
        next_id = store.next_id("column_profile")
        for col in columns:
            col_name = col.get("字段名称", "")
            dedup_key = (db_name, table_name, col_name)
            if dedup_key in existing:
                continue
            profile_rows.append({
                "id": next_id,
                "画像编号": f"COLU-{next_id:04d}",
                "所属库名": db_name,
                "关联表名": table_name,
                "关联表编号": catalog_entry.get("表编号", ""),
                "字段名称": col_name,
                "数据类型": col.get("数据类型", ""),
                "空值比例": "待计算",
                "唯一值数": "待计算",
                "最小值": "待计算",
                "最大值": "待计算",
                "画像版本": "v1.0",
                "接入回执": receipt_no or "",
                "画像轮次": profile_round,
                "status": "待处理",
                "pending": True,
                "abnormal": False,
            })
            existing.add(dedup_key)
            next_id += 1
            dispatched += 1
        return dispatched

    def _validate_columns(self, columns: list[dict]) -> list[dict]:
        """校验字段完整性：检查非空字段、主键唯一性和数据类型有效性。"""
        issues = []
        valid_types = {"BIGINT", "VARCHAR(255)", "TINYINT", "DECIMAL(18,2)", "TIMESTAMP",
                       "DATE", "INTEGER", "NUMERIC", "TEXT", "STRING", "MAP", "DOUBLE"}
        for col in columns:
            col_name = col.get("字段名称", "")
            col_type = col.get("数据类型", "")
            if not col_name:
                issues.append({"字段": "(无名)", "问题": "字段名称为空"})
                continue
            if col_type not in valid_types:
                issues.append({"字段": col_name, "问题": f"数据类型 {col_type} 不在已知类型集中"})
            nullable = col.get("可为空")
            if nullable not in (True, False):
                issues.append({"字段": col_name, "问题": "可为空属性缺失或无效"})
        return issues

    def _dispatch_column_profile(self, table_id: int, table_name: str, columns: list[dict]) -> int:
        """兼容历史调用：按表名回写画像待处理清单。"""
        entry = store.find(self.MODULE, table_id)
        if entry is None:
            return 0
        return self.dispatch_profile_pending(entry, columns)

    def _dispatch_tag_classification(self, table_name: str, db_name: str, columns: list[dict]) -> int:
        """根据表结构和字段类型关联分类标签。"""
        tag_rows = store.rows("tag_classify")
        classified = 0
        dimension_map = {
            "mysql": "ODS层",
            "hive": "ADS/DWD层",
            "postgresql": "业务库",
        }
        dimension = dimension_map.get(db_name, "未知层")

        # 为表级添加分类标签
        for tag_suffix, tag_val in [("分层", dimension), ("库", db_name), ("表类型", "事实表" if len(columns) > 4 else "维度表")]:
            new_tid = store.next_id("tag_classify")
            tag_rows.append({
                "id": new_tid,
                "标签编号": f"TAG_-{new_tid:04d}",
                "标签名称": f"{table_name}_{tag_suffix}",
                "分类维度": tag_suffix,
                "关联表名": table_name,
                "标签值": tag_val,
                "分类依据": f"{db_name}来源表自动分类",
                "生效时间": _now(),
                "标签版本": "v1",
                "status": "已生效",
                "pending": False,
                "abnormal": False,
            })
            classified += 1
        return classified

    def sync_lineage_entries(self, table_name: str, upstream_tables: list[str]) -> int:
        """将上游表与当前表关联写入血缘图谱，建立上下游数据血缘关系。"""
        lineage_rows = store.rows("lineage_graph")
        synced = 0
        for upstream in upstream_tables:
            new_lid = store.next_id("lineage_graph")
            lineage_rows.append({
                "id": new_lid,
                "关系编号": f"LINE-{new_lid:04d}",
                "上游表名": upstream,
                "下游表名": table_name,
                "来源字段": "id",
                "目标字段": "source_id",
                "转换逻辑": "ETL聚合转换",
                "采集批次": datetime.now(timezone.utc).strftime("%Y%m%d%H"),
                "关系版本": "v1",
                "status": "已生效",
                "pending": False,
                "abnormal": False,
            })
            synced += 1
        return synced

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["operation"] = "入库"
        snapshot["desc"] = "将采集到的表定义入库到目录存储"
        snapshot["artifact"] = "表定义目录"
        snapshot["artifact_desc"] = "表定义目录登记册"
        return snapshot
