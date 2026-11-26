"""重跑任务领域服务：覆盖策略与血缘影响传递，调度重跑状态机和水位推进，产出重跑任务数据集。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ReprocessTaskService(BaseModuleService):
    MODULE = "reprocess_task"
    ENTITY = "重跑任务"
    LIST_FIELDS = ["重跑编号", "触发原因", "影响事实表", "重跑区间", "覆盖策略", "依赖批次", "重跑水位", "重跑耗时", "负责班次", "负责人", "来源交接单", "交接签收"]
    REQUIRED_FIELDS = ["重跑编号", "触发原因", "影响事实表"]
    STATUS_ORDER = ["待排期", "重跑中", "已完成", "重跑失败"]
    ACTION_RULES = {"排期重跑": "重跑中", "确认完成": "已完成", "标记失败": "重跑失败"}
    NEGATIVE_ACTIONS = ["标记失败"]

    def schedule_reprocess(
        self, task_id: str, reason: str, target_table: str,
        reprocess_range: str, strategy: str, depends_on: str,
    ) -> tuple[bool, str, dict[str, Any]]:
        """排期重跑任务：覆盖策略决定旧区间是整段替换还是并存留痕。"""
        if strategy not in ("整段替换", "并存留痕"):
            return False, f"覆盖策略 {strategy} 无效", {}

        entry = {
            "重跑编号": task_id, "触发原因": reason,
            "影响事实表": target_table, "重跑区间": reprocess_range,
            "覆盖策略": strategy, "依赖批次": depends_on,
            "重跑水位": "", "重跑耗时": "",
            "status": "待排期", "pending": True, "abnormal": False,
        }
        rows = store.rows(self.MODULE)
        entry["id"] = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        rows.append(entry)
        return True, f"重跑任务 {task_id} 已排期，策略：{strategy}", entry

    def propagate_lineage_impact(self, target_table: str) -> dict[str, list[str]]:
        """沿血缘边传递重跑影响范围。"""
        lineage_rows = store.rows("lineage_edge")
        direct: list[str] = []
        for r in lineage_rows:
            if r.get("上游对象") == target_table:
                direct.append(str(r.get("下游对象", "")))
        seen = set(direct)
        indirect: list[str] = []
        for d in direct:
            for r in lineage_rows:
                if r.get("上游对象") == d and r.get("下游对象") not in seen:
                    indirect.append(str(r.get("下游对象", "")))
                    seen.add(str(r.get("下游对象", "")))
        return {"direct_impact": direct, "indirect_impact": indirect}

    def complete_reprocess(self, task_id: str, watermark: str) -> tuple[bool, str]:
        """确认重跑完成：更新重跑水位。"""
        rows = store.rows(self.MODULE)
        for r in rows:
            if r.get("重跑编号") == task_id:
                r["status"] = "已完成"
                r["重跑水位"] = watermark
                r["pending"] = False
                return True, f"重跑任务 {task_id} 已完成，水位 {watermark}"
        return False, f"重跑任务 {task_id} 不存在"

    def run_action(
        self, entry_id: int, action: str, *,
        remark: str | None = None,
        actor: str | None = None,
        target: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """排期重跑前校验排期权限：交接来源的任务须凭已签收交接单才可排期。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action not in self.ACTION_RULES:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"

        handover_no = str(entry.get("来源交接单") or "")
        granted = None
        if action == "排期重跑" and handover_no:
            actor = (actor or "").strip()
            if not actor:
                return None, (
                    f"重跑任务 {entry.get('重跑编号')} 由交接单 {handover_no} 交入，"
                    f"排期须注明凭交接签收的接班人"
                )
            from app.services.shift_handover import ShiftHandoverService
            granted = ShiftHandoverService().schedule_grant(
                str(entry.get("重跑编号")), actor)
            if granted is None:
                return None, (
                    f"{actor} 未凭交接单 {handover_no} 的签收取得"
                    f"重跑任务 {entry.get('重跑编号')} 的排期权限"
                )
            entry["排期依据"] = f"{granted.get('交接单号')}@{actor}"

        entry["status"] = self.ACTION_RULES[action]
        entry["pending"] = entry["status"] != self.STATUS_ORDER[-1]
        entry["abnormal"] = action in self.NEGATIVE_ACTIONS
        if action == "排期重跑":
            operator = (actor or "").strip()
            if operator and not str(entry.get("负责人") or "").strip():
                entry["负责人"] = operator
            if granted is not None and not str(entry.get("负责班次") or "").strip():
                entry["负责班次"] = str(granted.get("接班班次") or "")
        return entry, f"重跑任务已{action}"