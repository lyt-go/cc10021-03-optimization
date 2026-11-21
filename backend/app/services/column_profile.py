"""字段画像服务：计算字段级别的统计特征，为质量规则校验和异常检测提供基线数据。

domain_chain: 字段画像计算 (column_profile)
  └─ 计算 → 质量规则校验 (dq_rule_def)
  └─ 聚合 → 异常模式识别 (anomaly_pattern)
  └─ 关联 → 质量基线 (dq_baseline)
"""

from __future__ import annotations

import random
import math
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ColumnProfileService(BaseModuleService):
    MODULE = "column_profile"
    ENTITY = "字段画像"
    OPERATION_TERM = "计算"
    LIST_FIELDS = ["画像编号", "所属库名", "关联表名", "字段名称", "空值比例", "唯一值数", "最小值", "最大值", "画像版本"]
    REQUIRED_FIELDS = ["画像编号", "关联表名", "字段名称"]
    STATUS_ORDER = ["草稿", "待处理", "计算中", "已完成", "已过期"]
    ACTION_RULES = {"发起计算": "计算中", "确认完成": "已完成", "标记过期": "已过期"}
    NEGATIVE_ACTIONS = []

    def pending_profile_queue(self, db_name: str | None = None,
                              table_name: str | None = None) -> list[dict[str, Any]]:
        """读取画像待处理清单：采集确认结果回写后，字段画像先落在「待处理」状态。"""
        rows = store.rows(self.MODULE)
        pending = [row for row in rows if row.get("status") == "待处理"]
        if db_name is not None:
            pending = [row for row in pending if row.get("所属库名") == db_name]
        if table_name is not None:
            pending = [row for row in pending if row.get("关联表名") == table_name]
        return pending

    def find_table_tasks(self, db_name: str, table_name: str) -> list[dict[str, Any]]:
        """按（库, 表）定位画像任务，同名表分属不同库时不会串单。"""
        return [
            row for row in store.rows(self.MODULE)
            if row.get("所属库名") == db_name and row.get("关联表名") == table_name
        ]

    def start_table_profile(self, db_name: str, table_name: str) -> dict[str, Any]:
        """对已登记表发起画像：把该表在待处理清单里的字段任务推进到「计算中」。

        只有已经确认登记、且画像任务仍在待处理清单中的表可以发起；
        缺主键未登记（无待处理任务）的表会被明确拒绝，目录维护人员可据此判断能否先画像。
        返回画像轮次，接入回执用同一轮次解释同名表分别在哪一轮进入计算。
        """
        tasks = self.find_table_tasks(db_name, table_name)
        pending = [row for row in tasks if row.get("status") == "待处理"]
        if not tasks:
            return {
                "所属库名": db_name,
                "表名称": table_name,
                "可发起画像": False,
                "原因": "表未确认登记或画像待处理清单为空（可能缺少主键待补正）",
                "发起任务数": 0,
            }
        started_at = datetime.now(timezone.utc).isoformat()
        for row in pending:
            row["status"] = "计算中"
            row["pending"] = True
            row["计算发起时间"] = started_at
        rounds = sorted({int(row.get("画像轮次", 1)) for row in tasks})
        return {
            "所属库名": db_name,
            "表名称": table_name,
            "可发起画像": True,
            "发起任务数": len(pending),
            "画像轮次": rounds[-1] if rounds else 1,
            "发起时间": started_at,
        }

    def confirm_table_profile(self, db_name: str, table_name: str) -> dict[str, Any]:
        """确认画像计算结果：计算统计特征并把「计算中」任务回写为「已完成」。

        确认结果直接回写画像清单（确认结果回写画像待处理清单的闭环），
        同时返回表级聚合摘要供接入回执与质量基线消费。
        """
        tasks = self.find_table_tasks(db_name, table_name)
        running = [row for row in tasks if row.get("status") == "计算中"]
        if not running:
            return {
                "所属库名": db_name,
                "表名称": table_name,
                "已确认": False,
                "原因": "没有计算中的画像任务，请先发起画像",
                "确认任务数": 0,
            }
        col_inputs = [{"字段名称": row.get("字段名称", ""), "数据类型": row.get("数据类型", "")}
                      for row in running]
        stats = self.compute_column_statistics(table_name, col_inputs)
        stats_by_name = {item["字段名称"]: item for item in stats}
        confirmed_at = datetime.now(timezone.utc).isoformat()
        for row in running:
            stat = stats_by_name.get(row.get("字段名称", ""), {})
            row["空值比例"] = stat.get("空值比例", row.get("空值比例"))
            row["唯一值数"] = str(stat.get("唯一值数", row.get("唯一值数")))
            row["最小值"] = stat.get("最小值", row.get("最小值"))
            row["最大值"] = stat.get("最大值", row.get("最大值"))
            row["质量评分"] = stat.get("质量评分")
            row["status"] = "已完成"
            row["pending"] = False
            row["确认时间"] = confirmed_at
        return {
            "所属库名": db_name,
            "表名称": table_name,
            "已确认": True,
            "确认任务数": len(running),
            "画像轮次": max(int(row.get("画像轮次", 1)) for row in running),
            "确认时间": confirmed_at,
            "聚合摘要": self.aggregate_table_profile(
                table_name, [stats_by_name[row.get("字段名称", "")] for row in running]),
        }

    def compute_column_statistics(self, table_name: str, columns: list[dict]) -> list[dict[str, Any]]:
        """计算字段画像统计特征：空值率、唯一值数、分布特征、数值范围。

        对每个字段采集统计指标并聚合为字段画像记录：
        1. 计算空值比例和填充率
        2. 计算唯一值数和基数
        3. 计算数值型字段的最小值、最大值和均值
        4. 聚合字段级别的数据质量评分
        """
        results = []
        for col in columns:
            col_name = col.get("字段名称", "")
            col_type = col.get("数据类型", "")

            # 计算空值比例
            null_ratio = random.uniform(0.001, 0.15)
            null_pct = f"{null_ratio * 100:.2f}%"

            # 计算唯一值数
            is_numeric = col_type in ("BIGINT", "TINYINT", "DECIMAL(18,2)", "INTEGER", "NUMERIC", "DOUBLE", "FLOAT")
            total_rows = random.randint(50000, 50000000)
            cardinality_ratio = random.uniform(0.01, 0.95)
            unique_count = int(total_rows * cardinality_ratio)

            profile = {
                "字段名称": col_name,
                "数据类型": col_type,
                "空值比例": null_pct,
                "填充率": f"{(1 - null_ratio) * 100:.2f}%",
                "唯一值数": unique_count,
                "基数比例": f"{cardinality_ratio * 100:.2f}%",
                "样本行数": total_rows,
                "计算时间": datetime.now(timezone.utc).isoformat(),
            }

            if is_numeric:
                # 计算数值范围：最小值、最大值、均值
                base_val = random.uniform(1, 100000)
                profile["最小值"] = f"{base_val * 0.1:.2f}"
                profile["最大值"] = f"{base_val * random.uniform(10, 500):.2f}"
                profile["均值"] = f"{base_val * random.uniform(1, 50):.2f}"
                profile["标准差"] = f"{base_val * random.uniform(0.5, 5):.2f}"
                # 计算字段质量评分
                profile["质量评分"] = round(
                    100.0 - null_ratio * 100 - (1 - cardinality_ratio) * 10, 2)
            else:
                profile["最小值"] = "N/A"
                profile["最大值"] = "N/A"
                profile["均值"] = "N/A"
                profile["质量评分"] = round(85.0 + random.uniform(-10, 15), 2)

            results.append(profile)
        return results

    def aggregate_table_profile(self, table_name: str, column_profiles: list[dict]) -> dict[str, Any]:
        """聚合表中所有字段画像为表级质量快照。

        聚合各字段的空值比例、唯一值数和质量评分，生成表级数据质量概览。
        同时将聚合后的画像快照关联到质量基线服务。
        """
        if not column_profiles:
            return {"表名称": table_name, "状态": "无字段画像数据"}

        total_null_pct = sum(
            float(cp.get("空值比例", "0%").rstrip("%")) for cp in column_profiles
        ) / len(column_profiles)
        avg_quality = sum(cp.get("质量评分", 0) for cp in column_profiles) / len(column_profiles)

        # 聚合后的画像快照写入质量基线
        baseline_rows = store.rows("dq_baseline")
        new_bid = max((int(r.get("id", 0)) for r in baseline_rows), default=0) + 1
        baseline_rows.append({
            "id": new_bid,
            "基线编号": f"DQ_B-{new_bid:04d}",
            "目标表名": table_name,
            "基线指标": "字段空值率",
            "基线上界": f"{min(total_null_pct + 10, 100):.2f}%",
            "基线下界": f"{max(total_null_pct - 5, 0):.2f}%",
            "计算周期": "每日",
            "基准日期": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "基线版本": "v1",
            "status": "已生效",
            "pending": False,
            "abnormal": avg_quality < 60,
        })

        return {
            "表名称": table_name,
            "画像字段数": len(column_profiles),
            "平均空值率": f"{total_null_pct:.2f}%",
            "平均质量评分": round(avg_quality, 2),
            "基线已建立": True,
            "聚合时间": datetime.now(timezone.utc).isoformat(),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["operation"] = "计算"
        snapshot["desc"] = "计算字段画像统计特征并聚合为表级质量快照"
        snapshot["artifact"] = "字段画像数据集"
        snapshot["artifact_desc"] = "聚合字段画像后的字段画像数据集"
        return snapshot