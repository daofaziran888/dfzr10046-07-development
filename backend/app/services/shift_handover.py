"""值班交接领域服务：跨班排期的交接签收、处分幂等与排期权限授予。

核心行为：
- 值班交接单以「已送交」开局，接班人签收或拒收后流转；交班人可撤回送交。
- 同一轮交接只允许产生一个有效处分：并发送来的撤回、签收等申请先到先得，
  后到的申请不再改写状态，只登记为无效申请，避免重复处分。
- 签收结论同步到关联的重跑待办；签收人凭这次交接取得该重跑任务的排期权限。
- 拒收 / 撤回后的交接单可改派给下一位当班人，进入新一轮送交。
- 已经启动的工作留在本来负责的班次，不随改派或签收转移。
- 每张交接单都记录经手人、交班时刻与签收时间，可按记录或时刻核实班次切换。
"""
from __future__ import annotations

import threading
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

STATUS_DELIVERED = "已送交"
STATUS_SIGNED = "已签收"
STATUS_REJECTED = "已拒收"
STATUS_WITHDRAWN = "已撤回"

SIGN_ACTIONS = ("签收", "拒收", "撤回")


class ShiftHandoverService(BaseModuleService):
    MODULE = "shift_handover"
    ENTITY = "值班交接单"
    LIST_FIELDS = [
        "交接单号", "交班班次", "接班班次", "交班负责人", "接班负责人",
        "交班时刻", "接班时刻", "签收时间", "经手人", "关联迟到记录",
        "关联重跑任务", "签收结论",
    ]
    REQUIRED_FIELDS = [
        "交接单号", "交班班次", "接班班次", "交班负责人", "接班负责人",
    ]
    STATUS_ORDER = [STATUS_DELIVERED, STATUS_SIGNED, STATUS_REJECTED, STATUS_WITHDRAWN]
    ACTION_RULES = {
        "签收": STATUS_SIGNED,
        "拒收": STATUS_REJECTED,
        "撤回": STATUS_WITHDRAWN,
        "改派": STATUS_DELIVERED,
    }
    NEGATIVE_ACTIONS = ["拒收", "撤回"]

    # 进程内串行化：接口在线程池里并发执行，同一轮的处分只能有一个落库。
    _lock = threading.RLock()

    # ---------------------------------------------------------------- 工具

    @staticmethod
    def _now() -> str:
        return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    @staticmethod
    def _linked_task(ticket: dict[str, Any]) -> dict[str, Any] | None:
        task_id = str(ticket.get("关联重跑任务") or "")
        if not task_id:
            return None
        for task in store.rows("reprocess_task"):
            if task.get("重跑编号") == task_id:
                return task
        return None

    def _append_handler(
        self, ticket: dict[str, Any], actor: str, action: str, at: str,
        extra: dict[str, Any] | None = None,
    ) -> None:
        record: dict[str, Any] = {"动作": action, "经手人": actor, "时刻": at}
        record.update(extra or {})
        ticket.setdefault("经手记录", []).append(record)
        handlers: list[str] = []
        for item in ticket["经手记录"]:
            name = str(item.get("经手人") or "")
            if name and name not in handlers:
                handlers.append(name)
        ticket["经手人"] = ",".join(handlers)

    def _record_void_application(
        self, ticket: dict[str, Any], action: str, actor: str, reason: str,
    ) -> None:
        """登记未生效的并发申请：只留痕，不改写处分和状态。"""
        ticket.setdefault("无效申请", []).append({
            "动作": action, "经手人": actor, "时刻": self._now(), "原因": reason,
        })

    def _sync_conclusion(self, ticket: dict[str, Any], conclusion: str) -> None:
        """把签收结论同步到关联的重跑待办。"""
        ticket["签收结论"] = conclusion
        task = self._linked_task(ticket)
        if task is not None:
            task["交接签收"] = conclusion

    def _apply_ownership(self, ticket: dict[str, Any], actor: str) -> None:
        """签收后交接工作归属：未启动的工作交给接班人，已启动的留在原班次。"""
        task = self._linked_task(ticket)
        if task is None:
            ticket["保留工作"] = ""
            return
        if task.get("status") != "待排期":
            # 已经启动（重跑中/已完成/失败）的工作不随交接改派。
            shift = (task.get("负责班次") or ticket.get("交班班次")
                     or task.get("来源交接单") or "原班次")
            owner = task.get("负责人") or ticket.get("交班负责人") or "原负责人"
            ticket["保留工作"] = (
                f"{task.get('重跑编号')} 已启动，留在 {shift}（负责人 {owner}）"
            )
            return
        ticket["保留工作"] = ""
        task["负责人"] = actor
        task["负责班次"] = str(ticket.get("接班班次") or task.get("负责班次") or "")
        task["来源交接单"] = str(ticket.get("交接单号") or "")
        task["交接签收"] = f"已签收@{actor}"

    def _settle(
        self, ticket: dict[str, Any], action: str, actor: str, at: str,
    ) -> None:
        """落定本轮有效处分：登记处分与时刻，供后续改派留痕。"""
        ticket["处分"] = action
        ticket["处分时刻"] = at
        ticket.setdefault("处分记录", []).append({
            "轮次": int(ticket.get("轮次", 1)),
            "处分": action,
            "经手人": actor,
            "时刻": at,
        })

    def _check_disposition(self, ticket: dict[str, Any], action: str, actor: str) -> tuple[bool, str]:
        disposition = str(ticket.get("处分") or "")
        if disposition:
            reason = (
                f"交接单 {ticket.get('交接单号')} 已产生有效处分「{disposition}」，"
                f"本次「{action}」不再生效"
            )
            self._record_void_application(ticket, action, actor, reason)
            return False, reason
        return True, ""

    # ---------------------------------------------------------------- 动作

    def _sign(self, ticket: dict[str, Any], actor: str, remark: str | None) -> tuple[dict[str, Any] | None, str]:
        if ticket.get("status") != STATUS_DELIVERED:
            return None, f"交接单当前状态「{ticket.get('status')}」，不能签收"
        if actor != ticket.get("接班负责人"):
            return None, f"签收人须为接班负责人 {ticket.get('接班负责人')}，当前经手人 {actor}"
        at = self._now()
        ticket["status"] = STATUS_SIGNED
        ticket["签收时间"] = at
        self._settle(ticket, "签收", actor, at)
        ticket["处理时间"] = at
        conclusion = (remark or "同意接班").strip()
        self._append_handler(ticket, actor, "签收", at)
        self._apply_ownership(ticket, actor)
        suffix = f"；{ticket['保留工作']}" if ticket.get("保留工作") else ""
        self._sync_conclusion(ticket, f"已签收@{actor} {conclusion}{suffix}")
        ticket["pending"] = False
        ticket["abnormal"] = False
        return ticket, f"交接单 {ticket.get('交接单号')} 已签收，{actor} 取得排期权限"

    def _reject(self, ticket: dict[str, Any], actor: str, remark: str | None) -> tuple[dict[str, Any] | None, str]:
        if ticket.get("status") != STATUS_DELIVERED:
            return None, f"交接单当前状态「{ticket.get('status')}」，不能拒收"
        if actor != ticket.get("接班负责人"):
            return None, f"拒收人须为接班负责人 {ticket.get('接班负责人')}，当前经手人 {actor}"
        at = self._now()
        ticket["status"] = STATUS_REJECTED
        self._settle(ticket, "拒收", actor, at)
        ticket["处理时间"] = at
        self._append_handler(ticket, actor, "拒收", at)
        self._sync_conclusion(ticket, f"已拒收@{actor} {(remark or '收件人退回').strip()}")
        ticket["pending"] = False
        ticket["abnormal"] = True
        return ticket, f"交接单 {ticket.get('交接单号')} 已被 {actor} 退回，可改派下一位当班人"

    def _withdraw(self, ticket: dict[str, Any], actor: str, remark: str | None) -> tuple[dict[str, Any] | None, str]:
        if ticket.get("status") != STATUS_DELIVERED:
            return None, f"交接单当前状态「{ticket.get('status')}」，不能撤回"
        if actor != ticket.get("交班负责人"):
            return None, f"撤回人须为交班负责人 {ticket.get('交班负责人')}，当前经手人 {actor}"
        at = self._now()
        ticket["status"] = STATUS_WITHDRAWN
        self._settle(ticket, "撤回", actor, at)
        ticket["处理时间"] = at
        self._append_handler(ticket, actor, "撤回", at)
        self._sync_conclusion(ticket, f"已撤回@{actor} {(remark or '交班人撤回送交').strip()}")
        ticket["pending"] = False
        ticket["abnormal"] = True
        return ticket, f"交接单 {ticket.get('交接单号')} 已由 {actor} 撤回，本次交接不再授予排期权限"

    def _reassign(
        self, ticket: dict[str, Any], actor: str, target: str | None, remark: str | None,
    ) -> tuple[dict[str, Any] | None, str]:
        if ticket.get("status") not in (STATUS_REJECTED, STATUS_WITHDRAWN):
            return None, "仅已拒收或已撤回的交接单可改派下一位当班人"
        if ticket.get("status") == STATUS_REJECTED and actor != ticket.get("接班负责人"):
            return None, (
                f"改派须由退回该单的接班负责人 {ticket.get('接班负责人')} 发起，"
                f"当前经手人 {actor}"
            )
        if ticket.get("status") == STATUS_WITHDRAWN and actor != ticket.get("交班负责人"):
            return None, (
                f"改派须由撤回该单的交班负责人 {ticket.get('交班负责人')} 发起，"
                f"当前经手人 {actor}"
            )
        new_successor = (target or "").strip()
        if not new_successor:
            return None, "改派需要指定下一位接班负责人"
        if new_successor == ticket.get("接班负责人"):
            return None, f"改派目标不得与原接班人 {new_successor} 相同"
        at = self._now()
        previous = ticket.get("接班负责人")
        ticket["原接班负责人"] = previous
        ticket["接班负责人"] = new_successor
        ticket["轮次"] = int(ticket.get("轮次", 1)) + 1
        # 新一轮送交：上一轮处分留在「处分记录」里，本轮重新等待签收或拒收。
        ticket["处分"] = ""
        ticket["处分时刻"] = ""
        ticket["status"] = STATUS_DELIVERED
        ticket["签收时间"] = ""
        ticket["处理时间"] = at
        ticket["改派时刻"] = at
        ticket["改派经手人"] = actor
        self._append_handler(
            ticket, actor, "改派", at, {"改派目标": new_successor, "原接班人": previous})
        self._sync_conclusion(
            ticket,
            f"由 {actor} 改派给 {new_successor}（第 {ticket['轮次']} 轮）"
            + (f" {remark.strip()}" if remark and remark.strip() else ""),
        )
        ticket["pending"] = True
        ticket["abnormal"] = False
        return ticket, (
            f"交接单 {ticket.get('交接单号')} 已改派给 {new_successor}，"
            f"待其签收后取得排期权限"
        )

    # ---------------------------------------------------------------- 对外接口

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        rows = store.rows(self.MODULE)
        ticket_no = str(values.get("交接单号"))
        for row in rows:
            if row.get("交接单号") == ticket_no:
                return None, [f"交接单号 {ticket_no} 已存在"]
        if str(values.get("交班负责人")) == str(values.get("接班负责人")):
            return None, ["交班负责人与接班负责人不得为同一人"]

        entry: dict[str, Any] = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in self.LIST_FIELDS})
        entry["status"] = STATUS_DELIVERED
        entry["pending"] = True
        entry["abnormal"] = False
        entry["处分"] = ""
        entry["轮次"] = 1
        entry["签收时间"] = ""
        entry["经手人"] = str(values.get("交班负责人") or "")
        entry["经手记录"] = [{
            "动作": "送交", "经手人": str(values.get("交班负责人") or ""),
            "时刻": self._now(),
        }]
        entry["无效申请"] = []
        entry["处分记录"] = []
        rows.append(entry)
        self._sync_conclusion(
            entry, str(values.get("签收结论") or f"已送交@{entry.get('交班负责人')} 等待接班"))
        return entry, []

    def run_action(
        self, entry_id: int, action: str, *,
        remark: str | None = None,
        actor: str | None = None,
        target: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        with self._lock:
            ticket = store.find(self.MODULE, entry_id)
            if ticket is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            if action not in self.ACTION_RULES:
                return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
            operator = (actor or "").strip()
            if action in (*SIGN_ACTIONS, "改派") and not operator:
                return None, f"«{action}»需要注明经手人"

            if action != "改派":
                allowed, reason = self._check_disposition(ticket, action, operator)
                if not allowed:
                    return None, reason

            if action == "签收":
                return self._sign(ticket, operator, remark)
            if action == "拒收":
                return self._reject(ticket, operator, remark)
            if action == "撤回":
                return self._withdraw(ticket, operator, remark)
            return self._reassign(ticket, operator, target, remark)

    # ---------------------------------------------------------------- 权限与核验

    def schedule_grant(self, task_id: str, actor: str) -> dict[str, Any] | None:
        """查找授予 actor 对 task_id 排期权限的有效签收交接单。"""
        for ticket in store.rows(self.MODULE):
            if (
                ticket.get("关联重跑任务") == task_id
                and ticket.get("status") == STATUS_SIGNED
                and ticket.get("处分") == "签收"
                and ticket.get("接班负责人") == actor
            ):
                return ticket
        return None

    def verify(
        self, *, record_id: str | None = None, at: str | None = None,
    ) -> dict[str, Any]:
        """凭交接单核实班次切换：按迟到记录或按时刻查交班时刻与负责人。"""
        rows = store.rows(self.MODULE)
        if record_id:
            matches = [r for r in rows if r.get("关联迟到记录") == record_id]
            if not matches:
                return {"ok": False, "message": f"迟到记录 {record_id} 没有关联交接单"}
            matches.sort(key=lambda r: (int(r.get("轮次", 1)), int(r.get("id", 0))))
            ticket = matches[-1]
            return {
                "ok": True,
                "ticket": dict(ticket),
                "交班时刻": ticket.get("交班时刻", ""),
                "接班时刻": ticket.get("接班时刻", ""),
                "交班负责人": ticket.get("交班负责人", ""),
                "接班负责人": ticket.get("接班负责人", ""),
                "签收时间": ticket.get("签收时间", ""),
                "可核实": ticket.get("status") == STATUS_SIGNED,
            }
        if at:
            covers = [
                r for r in rows
                if str(r.get("交班时刻") or "") <= at < str(r.get("接班时刻") or "9999-12-31")
                and r.get("status") != STATUS_WITHDRAWN
            ]
            covers.sort(key=lambda r: int(r.get("id", 0)))
            if not covers:
                return {"ok": False, "message": f"时刻 {at} 没有覆盖它的交接班次"}
            return {
                "ok": True,
                "at": at,
                "tickets": [dict(r) for r in covers],
                "负责人": covers[0].get("交班负责人", ""),
                "可核实": any(r.get("status") == STATUS_SIGNED for r in covers),
            }
        return {"ok": False, "message": "请提供 record_id 或 at 进行核验"}
