"""接入回执服务：采集批次入库后可回查的回执台账。

回执由 MetadataCrawlerService.ingest_metadata_batch 写入，本服务提供只读查询
与画像发起入口，状态流转集中在采集/目录服务内，路由层不做业务判断。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.column_profile import ColumnProfileService
from app.store import store


class IngestReceiptService(BaseModuleService):
    MODULE = "ingest_receipt"
    ENTITY = "接入回执"
    OPERATION_TERM = "接入"
    LIST_FIELDS = ["回执编号", "接入批号", "数据源类型", "接入时间", "总表数",
                   "已接收数", "待补正数", "重复确认数", "当前画像轮次"]
    REQUIRED_FIELDS = ["回执编号", "接入批号"]
    STATUS_ORDER = ["已接收", "部分待补正", "全部待补正"]
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        self.profile_service = ColumnProfileService()

    def get_by_receipt_no(self, receipt_no: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("回执编号") == receipt_no:
                return row
        return None

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """回执由采集批次入库自动生成，不接受手工登记。"""
        return None, ["接入回执由采集批次入库自动生成，请通过 /api/ingest/batch 接入"]

    def start_profile(self, receipt_no: str, table_name: str,
                      db_name: str | None = None) -> dict[str, Any]:
        """目录维护人员打开回执后，对已登记的表发起画像。

        待补正（缺主键未登记）的表没有画像待处理任务，会被明确拒绝，
        已登记的表则推进其待处理清单进入计算，并回写分项的画像轮次。
        """
        receipt = self.get_by_receipt_no(receipt_no)
        if receipt is None:
            raise ValueError(f"接入回执 {receipt_no} 不存在")
        target_db = db_name or receipt.get("数据源类型", "")
        result = self.profile_service.start_table_profile(target_db, table_name)
        if result.get("可发起画像"):
            for item in receipt.get("分项明细", []):
                if item.get("表名称") == table_name and item.get("所属库名") == target_db:
                    item["画像发起轮次"] = result.get("画像轮次")
                    item["画像发起时间"] = result.get("发起时间")
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["operation"] = "接入"
        snapshot["desc"] = "采集批次入库的可回查接入回执台账"
        snapshot["artifact"] = "接入回执"
        snapshot["artifact_desc"] = "逐表记录已接收/待补正的接入回执"
        return snapshot
