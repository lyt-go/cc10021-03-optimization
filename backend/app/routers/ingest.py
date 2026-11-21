"""采集接入专用接口：采集 → 批次入库 → 接入回执 → 补录 → 画像待处理清单。

与通用资源路由分开维护，承载采集链路的专有动作；业务判断全部在领域服务内。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.column_profile import ColumnProfileService
from app.services.ingest_receipt import IngestReceiptService
from app.services.metadata_crawler import MetadataCrawlerService

router = APIRouter(prefix="/api/ingest", tags=["采集接入"])

crawler_service = MetadataCrawlerService()
receipt_service = IngestReceiptService()
profile_service = ColumnProfileService()


class CrawlPayload(BaseModel):
    datasource_type: str
    connection_url: str
    scope: str = "*"


class IngestPayload(BaseModel):
    schema_result: dict[str, Any]


class SupplementPayload(BaseModel):
    receipt_no: str
    table_name: str
    columns: list[dict[str, Any]]
    db_name: str | None = None
    row_count: int = 0


class ProfilePayload(BaseModel):
    db_name: str
    table_name: str


class ReceiptProfilePayload(BaseModel):
    receipt_no: str
    table_name: str
    db_name: str | None = None


@router.post("/crawl")
def crawl(payload: CrawlPayload) -> dict[str, Any]:
    return crawler_service.crawl_metadata_schema(
        payload.datasource_type, payload.connection_url, payload.scope)


@router.post("/batch")
def ingest_batch(payload: IngestPayload) -> dict[str, Any]:
    """批次入库：幂等登记并返回可回查接入回执；重复批号直接返回原回执。"""
    return crawler_service.ingest_metadata_batch(payload.schema_result)


@router.get("/receipts/{receipt_no}")
def get_receipt(receipt_no: str) -> dict[str, Any]:
    receipt = crawler_service.get_ingest_receipt(receipt_no)
    if receipt is None:
        raise HTTPException(status_code=404, detail=f"接入回执 {receipt_no} 不存在")
    return receipt


@router.post("/supplement")
def supplement(payload: SupplementPayload) -> dict[str, Any]:
    """补录缺主键分项，通过主键校验后以新画像轮次二次入库。"""
    try:
        return crawler_service.supplement_primary_key(
            receipt_no=payload.receipt_no,
            table_name=payload.table_name,
            columns=payload.columns,
            db_name=payload.db_name,
            row_count=payload.row_count,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/profiles/start")
def start_profile(payload: ProfilePayload) -> dict[str, Any]:
    """对已登记表发起画像：待处理清单 → 计算中。"""
    return profile_service.start_table_profile(payload.db_name, payload.table_name)


@router.post("/profiles/confirm")
def confirm_profile(payload: ProfilePayload) -> dict[str, Any]:
    """确认画像结果：计算中 → 已完成，结果回写画像清单。"""
    result = profile_service.confirm_table_profile(payload.db_name, payload.table_name)
    if not result.get("已确认"):
        raise HTTPException(status_code=400, detail=result.get("原因", "画像确认失败"))
    return result


@router.post("/receipts/profile")
def start_profile_from_receipt(payload: ReceiptProfilePayload) -> dict[str, Any]:
    """目录维护人员从回执页对已登记表发起画像（含待补正表的明确拦截）。"""
    try:
        return receipt_service.start_profile(
            payload.receipt_no, payload.table_name, payload.db_name)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
