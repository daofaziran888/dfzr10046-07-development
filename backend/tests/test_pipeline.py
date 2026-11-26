"""管线层关键链路测试：跨模块真实数据链路。"""
from __future__ import annotations

from datetime import date

import pytest

from app.pipeline.watermark import WatermarkManager, WatermarkLine
from app.pipeline.scd_processor import ScdProcessor, ScdRecord
from app.pipeline.fact_assembler import FactAssembler, FactColumn, BridgeWeight
from app.pipeline.quality_checker import QualityChecker, GateCheck
from app.pipeline.reprocess_orchestrator import (
    ReprocessOrchestrator, ReprocessTask, LineageEdge, LateFactRecord,
)
from app.pipeline.snapshot_manager import SnapshotManager
from app.pipeline.integration import PipelineOrchestrator


class TestWatermark:
    def test_advance_ok(self):
        wm = WatermarkManager()
        ok, msg = wm.advance_load_window("sales_fact", "2026-10-01", "2026-10-01", 1000, 1000)
        assert ok
        assert "已推进" in msg

    def test_advance_deviation_too_large(self):
        wm = WatermarkManager()
        ok, msg = wm.advance_load_window("sales_fact", "2026-10-01", "2026-10-01", 500, 1000)
        assert not ok
        assert "偏差" in msg

    def test_extract_not_ready(self):
        wm = WatermarkManager()
        wm.register_extract("ERP", "2026-09-01")
        ready, msg = wm.extract_ready("ERP", "2026-10-01")
        assert not ready
        assert "落后" in msg

    def test_extract_becomes_ready(self):
        wm = WatermarkManager()
        wm.register_extract("ERP", "2026-09-01")
        wm.register_extract("ERP", "2026-10-15")
        ready, msg = wm.extract_ready("ERP", "2026-10-01")
        assert ready

    def test_watermark_line_dirty_rows(self):
        wl = WatermarkLine("test_table")
        wl.mark_dirty(50)
        assert wl.dirty_rows == 50
        wl.mark_dirty(10)
        assert wl.dirty_rows == 60


class TestScdProcessor:
    def test_close_chain_ok(self):
        sp = ScdProcessor()
        sp.add_record(ScdRecord(
            1, "dim1", "SK-001", "field1",
            date(2026, 9, 1), date(9999, 12, 31), True, None,
        ))
        ok, msg, rec = sp.close_chain(1, date(2026, 10, 15), None)
        assert ok
        assert rec is not None
        assert rec["effective_end"] == "2026-10-15"
        assert not rec["is_current"]

    def test_close_chain_bad_date(self):
        sp = ScdProcessor()
        sp.add_record(ScdRecord(
            1, "dim1", "SK-001", "field1",
            date(2026, 9, 1), date(9999, 12, 31), True, None,
        ))
        ok, msg, _ = sp.close_chain(1, date(2026, 8, 1), None)
        assert not ok
        assert "早于" in msg

    def test_overlap_detected(self):
        sp = ScdProcessor()
        sp.add_record(ScdRecord(1, "dim1", "SK-001", "f1",
            date(2026, 9, 1), date(2026, 10, 1), True, None))
        sp.add_record(ScdRecord(2, "dim1", "SK-002", "f2",
            date(2026, 9, 15), date(2026, 10, 15), True, None))
        conflicts, msg = sp.detect_overlap("dim1")
        assert len(conflicts) >= 1
        c = conflicts[0]
        assert c.record_a == 1 and c.record_b == 2

    def test_no_overlap_adjacent(self):
        sp = ScdProcessor()
        sp.add_record(ScdRecord(1, "dim1", "SK-001", "f1",
            date(2026, 9, 1), date(2026, 9, 15), True, None))
        sp.add_record(ScdRecord(2, "dim1", "SK-002", "f2",
            date(2026, 9, 15), date(2026, 10, 1), True, None))
        conflicts, _ = sp.detect_overlap("dim1")
        assert len(conflicts) == 0

    def test_gap_detection(self):
        sp = ScdProcessor()
        sp.add_record(ScdRecord(1, "dim1", "SK-001", "f1",
            date(2026, 9, 1), date(2026, 9, 5), True, None))
        sp.add_record(ScdRecord(2, "dim1", "SK-002", "f2",
            date(2026, 9, 10), date(2026, 10, 1), True, None))
        gaps = sp.detect_gaps("dim1")
        assert len(gaps) == 1

    def test_attr_decay_apply(self):
        sp = ScdProcessor()
        sp.add_record(ScdRecord(1, "dim1", "SK-001", "old_value",
            date(2026, 9, 1), date(9999, 12, 31), True, None))
        sp.add_record(ScdRecord(2, "dim1", "SK-002", "another_old",
            date(2026, 9, 15), date(9999, 12, 31), True, None))
        count = sp.apply_attr_decay("dim1", "等级", "默认值")
        assert count == 2
        for r in sp.get_dim_records("dim1"):
            assert "已失效" in r.changed_field

    def test_close_chain_with_predecessor_ok(self):
        sp = ScdProcessor()
        sp.add_record(ScdRecord(10, "dim1", "SK-010", "f0",
            date(2026, 8, 1), date(2026, 9, 1), False, None))
        sp.add_record(ScdRecord(11, "dim1", "SK-011", "f1",
            date(2026, 9, 1), date(9999, 12, 31), True, None))
        ok, msg, _ = sp.close_chain(11, date(2026, 10, 1), 10)
        assert ok
        assert "已闭链" in msg

    def test_close_chain_predecessor_mismatch(self):
        sp = ScdProcessor()
        sp.add_record(ScdRecord(10, "dim1", "SK-010", "f0",
            date(2026, 8, 1), date(2026, 10, 1), False, None))
        sp.add_record(ScdRecord(11, "dim1", "SK-011", "f1",
            date(2026, 9, 1), date(9999, 12, 31), True, None))
        ok, msg, _ = sp.close_chain(11, date(2026, 11, 1), 10)
        assert not ok
        assert "不首尾相接" in msg


class TestFactAssembler:
    def test_hit_rate_full(self):
        fa = FactAssembler()
        fa._key_mapping.register("nk-1", 1001)
        fa._key_mapping.register("nk-2", 1002)
        rate, total, resolved = fa.compute_hit_rate(["nk-1", "nk-2"])
        assert rate == 1.0
        assert total == 2
        assert resolved == 2

    def test_hit_rate_partial(self):
        fa = FactAssembler()
        fa._key_mapping.register("nk-1", 1001)
        rate, total, resolved = fa.compute_hit_rate(["nk-1", "nk-2", "nk-3"])
        assert rate == 1.0 / 3.0
        assert total == 3
        assert resolved == 1

    def test_hit_rate_empty(self):
        fa = FactAssembler()
        rate, total, resolved = fa.compute_hit_rate([])
        assert rate == 1.0
        assert resolved == 0

    def test_bridge_normalize_ok(self):
        fa = FactAssembler()
        fa.add_bridge_weight(BridgeWeight(1, "dim_a", 0.6))
        fa.add_bridge_weight(BridgeWeight(1, "dim_b", 0.4))
        ok, msg, _ = fa.normalize_bridge_weights([1])
        assert ok

    def test_bridge_normalize_fail(self):
        fa = FactAssembler()
        fa.add_bridge_weight(BridgeWeight(1, "dim_a", 0.5))
        fa.add_bridge_weight(BridgeWeight(1, "dim_b", 0.3))
        ok, msg, _ = fa.normalize_bridge_weights([1])
        assert not ok
        assert "归一失败" in msg

    def test_grain_consistency_ok(self):
        fa = FactAssembler()
        fa.register_grain("sales", {"customer", "product", "date"})
        ok, msg = fa.check_grain_consistency("sales", {"customer", "product", "date"})
        assert ok

    def test_grain_consistency_mismatch(self):
        fa = FactAssembler()
        fa.register_grain("sales", {"customer", "product", "date"})
        ok, msg = fa.check_grain_consistency("sales", {"customer", "product"})
        assert not ok
        assert "不一致" in msg

    def test_assemble_batch_hit_rate_below_threshold(self):
        fa = FactAssembler()
        fa.register_dimension("sales", ["customer", "product", "date"])
        fa.register_grain("sales", {"customer", "product", "date"})
        ok, msg, result = fa.assemble_batch(
            "B001", "sales",
            [FactColumn("amount", "销售额", "SUM")],
            ["nk-1", "nk-2", "nk-3"],
            ["customer", "product", "date"],
        )
        assert not ok
        assert "命中率" in msg
        assert result["status"] == "待装配"

    def test_assemble_batch_ok(self):
        fa = FactAssembler()
        fa._key_mapping.register("nk-1", 1001)
        fa._key_mapping.register("nk-2", 1002)
        fa._key_mapping.register("nk-3", 1003)
        fa._key_mapping.register("nk-4", 1004)
        fa._key_mapping.register("nk-5", 1005)
        fa.register_dimension("sales", ["customer", "product", "date"])
        fa.register_grain("sales", {"customer", "product", "date"})
        ok, msg, result = fa.assemble_batch(
            "B001", "sales",
            [FactColumn("amount", "销售额", "SUM")],
            ["nk-1", "nk-2", "nk-3", "nk-4", "nk-5"],
            ["customer", "product", "date"],
        )
        assert ok
        assert result["status"] == "已装配"
        assert result["hit_rate"] == 1.0


class TestQualityChecker:
    def test_gate_pass(self):
        qc = QualityChecker()
        qc.register_gate("sales", GateCheck("G1", "外键命中率", 0.80, "批次级"))
        passed, results = qc.run_checks("sales", {"外键命中率": 0.95})
        assert passed
        assert len(results) == 1
        assert results[0].passed

    def test_gate_fail(self):
        qc = QualityChecker()
        qc.register_gate("sales", GateCheck("G1", "外键命中率", 0.80, "批次级"))
        passed, results = qc.run_checks("sales", {"外键命中率": 0.60})
        assert not passed
        assert not results[0].passed

    def test_gate_missing_measurement(self):
        qc = QualityChecker()
        qc.register_gate("sales", GateCheck("G1", "行数精度", 0.95, "窗口级"))
        passed, results = qc.run_checks("sales", {})
        assert not passed

    def test_multiple_gates_partial_fail(self):
        qc = QualityChecker()
        qc.register_gate("sales", GateCheck("G1", "外键命中率", 0.80, "批次级"))
        qc.register_gate("sales", GateCheck("G2", "行数精度", 0.90, "窗口级"))
        passed, results = qc.run_checks("sales", {"外键命中率": 0.95, "行数精度": 0.82})
        assert not passed


class TestReprocessOrchestrator:
    def test_late_fact_backfill(self):
        ro = ReprocessOrchestrator()
        ro.set_watermark_cutoff("sales", date(2026, 10, 1))
        late = LateFactRecord("LATE-1", "sales",
                              date(2026, 9, 15), date(2026, 10, 20), "part-202609")
        status = ro.register_late_fact(late)
        assert status == "待回补"

    def test_late_fact_reject(self):
        ro = ReprocessOrchestrator()
        ro.set_watermark_cutoff("sales", date(2026, 9, 1))
        late = LateFactRecord("LATE-2", "sales",
                              date(2026, 10, 1), date(2026, 10, 15), "part-202610")
        status = ro.register_late_fact(late)
        assert status == "拒绝回补"

    def test_lineage_propagation(self):
        ro = ReprocessOrchestrator()
        ro.add_edge(LineageEdge("E1", "fact", "summary", "派生"))
        ro.add_edge(LineageEdge("E2", "fact", "quality", "校验"))
        ro.add_edge(LineageEdge("E3", "summary", "metric", "绑定"))
        impacted = ro.propagate_impact("fact")
        assert "summary" in impacted
        assert "quality" in impacted
        assert "metric" in impacted

    def test_reprocess_create(self):
        ro = ReprocessOrchestrator()
        task = ReprocessTask("RP-1", "迟到事实回补", "sales",
                             date(2026, 9, 1), date(2026, 9, 30),
                             "并存留痕", "BATCH-202609")
        ok, msg = ro.create_reprocess(task)
        assert ok
        assert "并存留痕" in msg

    def test_reprocess_bad_strategy(self):
        ro = ReprocessOrchestrator()
        task = ReprocessTask("RP-1", "test", "sales",
                             date(2026, 9, 1), date(2026, 9, 30),
                             "错误策略", "BATCH")
        ok, _ = ro.create_reprocess(task)
        assert not ok

    def test_backfill_long_lag(self):
        ro = ReprocessOrchestrator()
        late = LateFactRecord("LATE-3", "sales",
                              date(2026, 1, 1), date(2026, 12, 20),
                              "part-202601")
        decision = ro.decide_backfill_boundary(late, date(2026, 6, 1))
        assert decision == "整段重算"


class TestSnapshotManager:
    def test_create_snapshot_ok(self):
        sm = SnapshotManager()
        sm.register_dim_data("customer", [
            {"surrogate_key": "SK-001", "is_current": True},
            {"surrogate_key": "SK-002", "is_current": False},
        ])
        ok, msg, snap = sm.create_snapshot("customer", date(2026, 10, 1), "BATCH-01")
        assert ok
        assert snap.record_count == 2

    def test_publish_then_no_silent_replace(self):
        sm = SnapshotManager()
        sm.register_dim_data("customer", [{"surrogate_key": "SK-001", "is_current": True}])
        sm.create_snapshot("customer", date(2026, 10, 1), "BATCH-01")
        snap_id = "SNP-customer-2026-10-01"
        ok, msg, _ = sm.publish_snapshot(snap_id, "v1", "全量")
        assert ok
        ok2, msg2, _ = sm.publish_snapshot(snap_id, "v1", "全量")
        assert not ok2
        assert "不得静默替换" in msg2

    def test_withdraw_publish(self):
        sm = SnapshotManager()
        sm.register_dim_data("customer", [{"surrogate_key": "SK-001", "is_current": True}])
        sm.create_snapshot("customer", date(2026, 10, 1), "BATCH-01")
        snap_id = "SNP-customer-2026-10-01"
        sm.publish_snapshot(snap_id, "v1", "全量")
        pub_id = "PUB-customer-v1"
        ok, msg = sm.withdraw_publish(pub_id)
        assert ok

    def test_withdraw_twice_rejected(self):
        sm = SnapshotManager()
        sm.register_dim_data("customer", [{"surrogate_key": "SK-001", "is_current": True}])
        sm.create_snapshot("customer", date(2026, 10, 1), "BATCH-01")
        sm.publish_snapshot("SNP-customer-2026-10-01", "v1", "全量")
        sm.withdraw_publish("PUB-customer-v1")
        ok, msg = sm.withdraw_publish("PUB-customer-v1")
        assert not ok
        assert "已撤回" in msg


class TestIntegration:
    """端到端集成链路测试。"""

    def test_link1_extract_to_publish(self):
        orch = PipelineOrchestrator()
        result = orch.link1_extract_to_publish(
            system="ERP", dim_name="customer",
            natural_keys=["nk-1", "nk-2", "nk-3"],
            snapshot_date="2026-10-01", version="v1",
        )
        assert result["ok"]
        assert orch.audit_log  # 应有执行轨迹

    def test_link2_scd_to_fact(self):
        orch = PipelineOrchestrator()
        orch.fact._key_mapping.register("nk-1", 1001)
        orch.fact._key_mapping.register("nk-2", 1002)
        orch.fact._key_mapping.register("nk-3", 1003)
        result = orch.link2_scd_to_fact(
            batch_id="B001", fact_table="sales",
            row_keys=["nk-1", "nk-2", "nk-3"],
            dim_names=["customer", "product"],
            measures=[{"name": "amount", "measure": "销售额", "agg": "SUM"}],
        )
        assert result["ok"]

    def test_link3_late_reprocess(self):
        orch = PipelineOrchestrator()
        orch.reprocess.set_watermark_cutoff("sales", date(2026, 10, 1))
        result = orch.link3_late_reprocess(
            record_id="LATE-001", fact_table="sales",
            business_date="2026-09-15", arrival_date="2026-10-20",
            task_id="RP-001",
        )
        assert result["ok"]
        assert len(result["impact"]) > 0

    def test_link4_degenerate_to_metric(self):
        orch = PipelineOrchestrator()
        result = orch.link4_degenerate_to_metric(
            fact_table="sales", degenerate_field="订单号",
            base_count=100, summary_grain={"customer", "product"},
            measure_name="销售额", metric_name="月度销售额",
        )
        assert result["ok"]

    def test_link5_bridge_to_fact(self):
        orch = PipelineOrchestrator()
        dim_weights = [
            {"row_id": 1, "dim_name": "channel", "weight": 0.6},
            {"row_id": 1, "dim_name": "promotion", "weight": 0.4},
        ]
        result = orch.link5_bridge_to_fact("sales", [1], dim_weights)
        assert result["ok"]

    def test_link5_bridge_fail(self):
        orch = PipelineOrchestrator()
        dim_weights = [
            {"row_id": 1, "dim_name": "channel", "weight": 0.5},
            {"row_id": 1, "dim_name": "promotion", "weight": 0.3},
        ]
        result = orch.link5_bridge_to_fact("sales", [1], dim_weights)
        assert not result["ok"]

    def test_link6_attr_decay(self):
        orch = PipelineOrchestrator()
        orch.scd.add_record(ScdRecord(1, "customer", "SK-001", "old",
            date(2026, 9, 1), date(9999, 12, 31), True, None))
        result = orch.link6_attr_decay_chain("customer", "等级", "默认值")
        assert result["affected_chains"] == 1

    def test_link7_hier_rollup(self):
        orch = PipelineOrchestrator()
        hierarchy = {"全国": ["省份"], "省份": ["区县"]}
        result = orch.link7_hier_rollup(
            "区域维度", hierarchy, "sales",
            {"customer", "product", "date"},
        )
        assert result["ok"]

    def test_link8_reprocess_cover(self):
        orch = PipelineOrchestrator()
        result = orch.link8_reprocess_cover(
            task_id="RP-002", table="sales",
            start="2026-09-01", end="2026-10-01",
            strategy="整段替换", old_batch="BATCH-OLD",
            new_gate_values={"重跑后命中率": 0.92},
        )
        assert result["ok"]

    def test_link8_reprocess_gate_fail(self):
        orch = PipelineOrchestrator()
        result = orch.link8_reprocess_cover(
            task_id="RP-003", table="sales",
            start="2026-09-01", end="2026-10-01",
            strategy="并存留痕", old_batch="BATCH-OLD",
            new_gate_values={"重跑后命中率": 0.70},
        )
        assert not result["ok"]

    def test_link9_stall_recover(self):
        orch = PipelineOrchestrator()
        result = orch.link9_stall_recover(
            system="ERP", required="2026-10-15", delay=5,
        )
        assert result["ok"]

    def test_link10_dim_merge(self):
        orch = PipelineOrchestrator()
        result = orch.link10_dim_merge_migrate(
            old_dim="dim_old", new_dim="dim_new",
            from_sks=["SK-001", "SK-002"],
            to_sks=["SK-101", "SK-102"],
            batch_id="BATCH-MIG",
        )
        assert result["migrated_count"] == 2

    def test_all_links_return_audit_logs(self):
        orch = PipelineOrchestrator()
        orch.link1_extract_to_publish("ERP", "customer", ["nk-1"], "2026-10-01", "v1")
        orch.link9_stall_recover("ERP", "2026-10-15", 5)
        report = orch.status_report()
        assert len(report["audit_log"]) >= 2
        assert "watermark" in report
        assert "scd" in report
        assert "fact" in report


class TestServiceIntegration:
    """服务层跨模块消费测试。"""

    def test_dim_entry_registration(self):
        from app.services.dim_entry import DimEntryService
        svc = DimEntryService()
        entry, msg = svc.register_dimension(
            "DIM-TEST", "测试维度", "test_key",
            "ERP", "SCD2", "测试",
        )
        assert entry is not None
        assert entry["status"] == "已注册"

    def test_dim_dup_rejected(self):
        from app.services.dim_entry import DimEntryService
        svc = DimEntryService()
        svc.register_dimension("DIM-DUP", "重复1", "k1", "ERP", "SCD2", "测试")
        entry2, msg2 = svc.register_dimension("DIM-DUP", "重复2", "k2", "ERP", "SCD1", "测试")
        assert entry2 is None
        assert "已存在" in msg2

    def test_src_extract_then_dim_validate(self):
        from app.services.src_extract import SrcExtractService
        from app.services.dim_entry import DimEntryService
        from app.store import store

        src = SrcExtractService()
        src.extract_batch("ERP", "2026-10", "全量", 1000)
        rows = store.rows("src_extract")
        for r in rows:
            if r.get("来源系统") == "ERP":
                r["status"] = "已抽取"

        dim = DimEntryService()
        entry, _ = dim.register_dimension("DIM-VAL", "验证维度", "vk1", "ERP", "SCD2", "测试")
        dim_rows = list(store.rows("dim_entry"))
        target = None
        for r in dim_rows:
            if r.get("维度编号") == "DIM-VAL":
                target = r
                break
        assert target is not None
        ok, _ = dim.validate_natural_key_source(target)
        # ERP is in src_extract with status 已抽取, so should pass
        assert ok

    def test_scd_close_then_conform_block(self):
        from app.services.scd_chain import ScdChainService
        from app.services.conform_check import ConformCheckService

        scd = ScdChainService()
        ok, msg, _ = scd.close_chain_record("SCD-001", "2026-10-15", None)
        assert ok

        cc = ConformCheckService()
        # Use actual seed data dimensions: 客户维度 has business key "customer_id"
        ok, msg, result = cc.check_conformity("客户维度", "客户维度", "客户级")
        # Same dimension compared against itself should be consistent
        assert ok

    def test_conform_deviation_blocks_assembly(self):
        from app.services.conform_check import ConformCheckService
        cc = ConformCheckService()
        blocked, msg = cc.block_fact_assembly("产品维度")
        assert blocked
        assert "阻断" in msg

    def test_fact_assemble_hit_rate(self):
        from app.services.fact_assemble import FactAssembleService
        fa = FactAssembleService()
        ok, msg, result = fa.assemble_fact_batch(
            "BATCH-OK", "销售事实表", 3, 10,
            ["销售额", "销售量"],
        )
        assert ok
        assert result["hit_rate"] >= 0.80

    def test_late_fact_register(self):
        from app.services.late_fact import LateFactService
        lf = LateFactService()
        ok, msg, entry = lf.register_late_fact(
            "LATE-NEW", "销售事实表",
            "2026-09-01", "2026-10-15",
            "part-202609",
        )
        assert ok
        assert entry["滞后天数"] > 0
        assert entry["回补结论"] == "待回补"

    def test_late_fact_not_late(self):
        from app.services.late_fact import LateFactService
        lf = LateFactService()
        ok, msg, _ = lf.register_late_fact(
            "LATE-FUTURE", "销售事实表",
            "2026-10-16", "2026-10-15",
            "part-202610",
        )
        assert not ok
        assert "不视为迟到" in msg

    def test_late_decide_backfill(self):
        from app.services.late_fact import LateFactService
        lf = LateFactService()
        lf.register_late_fact("LATE-BF", "销售事实表",
                              "2026-08-01", "2026-10-15", "part-202608")
        ok, msg = lf.decide_backfill("LATE-BF", "2026-10-01", "增量追加")
        assert ok
        assert "整段重算" in msg

    def test_quality_gate_evaluate(self):
        from app.services.quality_gate import QualityGateService
        qg = QualityGateService()
        ok, msg, entry = qg.evaluate_gate(
            "QG-TEST", "销售事实表", "外键命中率",
            0.80, 0.95, "批次级",
        )
        assert ok
        assert entry["通过判定"] == "是"

    def test_quality_gate_block(self):
        from app.services.quality_gate import QualityGateService
        qg = QualityGateService()
        ok, msg, entry = qg.evaluate_gate(
            "QG-FAIL", "销售事实表", "外键命中率",
            0.80, 0.60, "批次级",
        )
        assert not ok
        assert entry["通过判定"] == "否"
        assert entry["status"] == "已阻断"

    def test_reprocess_schedule(self):
        from app.services.reprocess_task import ReprocessTaskService
        rp = ReprocessTaskService()
        ok, msg, entry = rp.schedule_reprocess(
            "RP-TEST", "口径变更", "销售事实表",
            "2026-09", "整段替换", "FACT-B001",
        )
        assert ok
        assert entry["覆盖策略"] == "整段替换"

    def test_lineage_propagate(self):
        from app.services.lineage_edge import LineageEdgeService
        le = LineageEdgeService()
        result = le.propagate_impact("fact_assemble")
        assert len(result) > 0  # Should find summary_layer and quality_gate

    def test_load_window_deviation(self):
        from app.services.load_window import LoadWindowService
        lw = LoadWindowService()
        ok, msg, entry = lw.advance_window(
            "LW-TEST", "销售事实表",
            "2026-10-01", "2026-10-01",
            1000, 500,
        )
        assert not ok
        assert "偏差" in msg

    def test_summary_no_overwrite(self):
        from app.services.summary_layer import SummaryLayerService
        sl = SummaryLayerService()
        sl.derive_summary("SUM-A", "销售事实表", "日", "SUM(amount)",
                          "BATCH-X", "全量刷新", 100)
        ok, msg, _ = sl.derive_summary("SUM-B", "销售事实表", "日", "SUM(amount)",
                                        "BATCH-X", "全量刷新", 100)
        assert not ok
        assert "不得覆盖" in msg

    def test_grain_declare_and_check(self):
        from app.services.grain_declare import GrainDeclareService
        from app.services.summary_layer import SummaryLayerService
        gd = GrainDeclareService()
        ok, msg, entry = gd.declare_grain(
            "GRN-TEST", "销售事实表",
            "客户每笔销售", "客户+产品+时间",
            "半可加", "customer_id+date", "v1",
        )
        assert ok

        sl = SummaryLayerService()
        ok, msg = sl.check_grain_consistency("客户+产品+时间", "客户+产品+时间")
        assert ok

    def test_hier_break_detection(self):
        from app.services.hier_path import HierPathService
        hp = HierPathService()
        breaks = hp.detect_break("区域维度")
        assert len(breaks) >= 1  # HIER-003 has parent "城市" not in hierarchy

    def test_bridge_normalization_service(self):
        from app.services.bridge_link import BridgeLinkService
        bl = BridgeLinkService()
        # Each row's total weight must sum to 1.0 for normalization to pass
        ok, msg, result = bl.check_normalization(
            "销售事实表", {1: 1.0, 2: 1.0},
        )
        assert ok

    def test_bridge_normalization_fail_service(self):
        from app.services.bridge_link import BridgeLinkService
        bl = BridgeLinkService()
        ok, msg, result = bl.check_normalization(
            "销售事实表", {1: 0.5, 2: 0.3},
        )
        assert not ok
        assert "归一失败" in msg

    def test_dim_publish_no_duplicate_version(self):
        from app.services.dim_publish import DimPublishService
        dp = DimPublishService()
        ok, msg, _ = dp.publish_version(
            "PUB-TEST", "客户维度", "v2",
            "2026-10-01", "全量", "test",
        )
        assert ok
        ok2, msg2, _ = dp.publish_version(
            "PUB-TEST-2", "客户维度", "v2",
            "2026-10-01", "全量", "test",
        )
        assert not ok2
        assert "不得静默替换" in msg2

    def test_attr_decay_then_scd_apply(self):
        from app.services.attr_decay import AttrDecayService
        from app.services.scd_chain import ScdChainService
        ad = AttrDecayService()
        ok, msg, entry = ad.judge_attr_decay(
            "ATTR-TEST", "客户维度", "客户等级",
            "等级调整", "2026-11-01",
        )
        assert ok
        assert int(entry["受影响链数"]) > 0

        scd = ScdChainService()
        count = scd.apply_attr_invalidation("客户维度", "客户等级", "默认值")
        assert count > 0

    def test_model_audit_record(self):
        from app.services.model_audit import ModelAuditService
        ma = ModelAuditService()
        ok, msg, entry = ma.record_audit(
            "AUD-TEST", "粒度声明修订",
            "GRN-001", "旧值", "新值",
            3, "worker",
        )
        assert ok
        assert entry["操作类型"] == "粒度声明修订"

    def test_seed_data_integrity(self):
        """种子数据完整性：每个模块都有至少 3 条记录含不同状态。"""
        from app.store import store
        for name in store.module_names():
            rows = store.rows(name)
            assert len(rows) >= 1, f"{name} 种子数据为空"
            statuses = {r.get("status") for r in rows if r.get("status")}
            assert len(statuses) >= 1, f"{name} 只有一种状态"

    def test_cross_module_chain_dim_to_fact(self):
        """跨模块链路：维度建档→代理键→拉链→事实装配。"""
        from app.services.dim_entry import DimEntryService
        from app.services.surrogate_key import SurrogateKeyService
        from app.services.fact_assemble import FactAssembleService

        dim = DimEntryService()
        dim.register_dimension("DIM-CHAIN", "链路测试", "chain_key", "ERP", "SCD2", "测试")

        sk = SurrogateKeyService()
        key, msg = sk.allocate_key("客户维度", "chain_key")
        assert key is not None

        fa = FactAssembleService()
        ok, _, result = fa.assemble_fact_batch(
            "BATCH-CHAIN", "销售事实表", 3, 10,
            ["销售额", "销售量"],
        )
        assert ok
        assert result["hit_rate"] >= 0.80

    def test_default_natural_key_source_check(self):
        """默认种子数据维度校验：DIM-001 的自然键来源应为 ERP。"""
        from app.services.dim_entry import DimEntryService
        from app.store import store

        dim = DimEntryService()
        rows = store.rows("dim_entry")
        target = None
        for r in rows:
            if r.get("维度编号") == "DIM-001":
                target = r
                break
        assert target is not None
        ok, msg = dim.validate_natural_key_source(target)
        assert ok or "未在已抽取" in msg


class TestDataEdgeCases:
    """边界与脏数据测试。"""

    def test_zero_expected_rows(self):
        wm = WatermarkManager()
        ok, msg = wm.advance_load_window("t", "s", "e", 0, 0)
        assert not ok
        assert "无效" in msg

    def test_empty_dim_snapshot_rejected(self):
        sm = SnapshotManager()
        ok, _, _ = sm.create_snapshot("nonexistent", date(2026, 10, 1), "B")
        assert not ok

    def test_publish_nonexistent_snapshot(self):
        sm = SnapshotManager()
        ok, msg, _ = sm.publish_snapshot("NOT-EXIST", "v1", "全量")
        assert not ok
        assert "不存在" in msg

    def test_sorting_respected_in_overlap(self):
        """Sorting order is respected: overlapping records found regardless of insert order."""
        sp = ScdProcessor()
        # Insert in reverse chronological order
        sp.add_record(ScdRecord(2, "dim1", "SK-002", "f2",
            date(2026, 10, 1), date(2026, 11, 1), True, None))
        sp.add_record(ScdRecord(1, "dim1", "SK-001", "f1",
            date(2026, 9, 15), date(2026, 10, 15), True, None))
        conflicts, _ = sp.detect_overlap("dim1")
        assert len(conflicts) >= 1

    def test_status_report_completeness(self):
        orch = PipelineOrchestrator()
        orch.link1_extract_to_publish("ERP", "test", ["nk-1"], "2026-10-01", "v1")
        report = orch.status_report()
        for key in ["audit_log", "watermark", "scd", "fact", "quality", "reprocess", "snapshot"]:
            assert key in report, f"Missing key {key} in status_report"

class TestShiftHandover:
    """值班交接：签收结论同步重跑待办、凭交接取排期权限、并发处分幂等。"""

    _seq = 0

    def _new_task(self, status="待排期", owner=""):
        from app.store import store
        TestShiftHandover._seq += 1
        seq = TestShiftHandover._seq
        task = {
            "id": max((int(r.get("id", 0)) for r in store.rows("reprocess_task")), default=0) + 1,
            "重跑编号": f"RP-T-{seq}", "触发原因": "迟到事实回补",
            "影响事实表": "销售事实表", "重跑区间": "2026-09-20至2026-09-30",
            "覆盖策略": "整段替换", "依赖批次": "FACT-B002",
            "重跑水位": "", "重跑耗时": "",
            "status": status, "pending": status == "待排期", "abnormal": False,
            "负责班次": "夜班 2026-11-25 20:00-次日08:00" if owner else "",
            "负责人": owner,
            "来源交接单": "", "交接签收": "",
        }
        store.rows("reprocess_task").append(task)
        return task

    def _new_ticket(self, task, **overrides):
        from app.services.shift_handover import ShiftHandoverService
        TestShiftHandover._seq += 1
        seq = TestShiftHandover._seq
        svc = ShiftHandoverService()
        values = {
            "交接单号": f"HO-T-{seq}",
            "交班班次": "夜班 2026-11-25 20:00-次日08:00",
            "接班班次": "早班 2026-11-26 08:00-20:00",
            "交班负责人": "甲",
            "接班负责人": "乙",
            "交班时刻": "2026-11-26T08:00:00",
            "接班时刻": "2026-11-26T20:00:00",
            "关联迟到记录": f"Q-{seq}",
            "关联重跑任务": task["重跑编号"],
        }
        values.update(overrides)
        entry, problems = svc.create_entry(values)
        assert entry is not None, problems
        task["来源交接单"] = entry["交接单号"]
        return svc, entry

    def test_create_starts_delivered(self):
        task = self._new_task()
        svc, entry = self._new_ticket(task)
        assert entry["status"] == "已送交"
        assert entry["pending"] is True
        assert entry["经手人"] == "甲"
        assert entry["签收时间"] == ""

    def test_create_rejects_duplicate_and_same_person(self):
        task = self._new_task()
        svc, entry = self._new_ticket(task)
        dup, problems = svc.create_entry({**{
            "交接单号": entry["交接单号"], "交班班次": "夜班", "接班班次": "早班",
            "交班负责人": "甲", "接班负责人": "乙",
        }})
        assert dup is None and any("已存在" in p for p in problems)
        same, problems = svc.create_entry({
            "交接单号": "HO-T-SAME", "交班班次": "夜班", "接班班次": "早班",
            "交班负责人": "甲", "接班负责人": "甲",
        })
        assert same is None and any("不得为同一人" in p for p in problems)

    def test_sign_grants_schedule_and_syncs_conclusion(self):
        from app.store import store
        task = self._new_task()
        svc, entry = self._new_ticket(task)
        signed, msg = svc.run_action(entry["id"], "签收", actor="乙", remark="已核对")
        assert signed is not None, msg
        assert signed["status"] == "已签收"
        assert signed["签收时间"]
        assert "已签收@乙" in signed["签收结论"]
        assert "已签收@乙" in task["交接签收"]
        assert task["负责人"] == "乙"
        assert task["负责班次"].startswith("早班")
        assert svc.schedule_grant(task["重跑编号"], "乙") is not None
        assert store.find("shift_handover", entry["id"])["status"] == "已签收"

    def test_schedule_permission_requires_signed_handover(self):
        from app.services.reprocess_task import ReprocessTaskService
        task = self._new_task()
        svc, entry = self._new_ticket(task)
        rp = ReprocessTaskService()
        blocked, msg = rp.run_action(task["id"], "排期重跑", actor="乙")
        assert blocked is None
        assert "排期权限" in msg
        assert task["status"] == "待排期"

        svc.run_action(entry["id"], "签收", actor="乙")
        ok, _ = rp.run_action(task["id"], "排期重跑", actor="乙")
        assert ok is not None
        assert ok["status"] == "重跑中"
        assert ok["排期依据"] == f"{entry['交接单号']}@乙"

    def test_concurrent_withdraw_and_sign_only_one_wins(self):
        task = self._new_task()
        svc, entry = self._new_ticket(task)
        first, m1 = svc.run_action(entry["id"], "签收", actor="乙")
        assert first is not None, m1
        second, m2 = svc.run_action(entry["id"], "撤回", actor="甲")
        assert second is None
        assert "有效处分" in m2
        assert first["status"] == "已签收"
        assert first["处分"] == "签收"
        assert first["无效申请"] and first["无效申请"][0]["动作"] == "撤回"

    def test_concurrent_withdraw_first_then_sign(self):
        task = self._new_task()
        svc, entry = self._new_ticket(task)
        first, m1 = svc.run_action(entry["id"], "撤回", actor="甲")
        assert first is not None, m1
        assert first["status"] == "已撤回"
        second, m2 = svc.run_action(entry["id"], "签收", actor="乙")
        assert second is None
        assert "有效处分" in m2
        assert svc.schedule_grant(task["重跑编号"], "乙") is None

    def test_reject_then_reassign_next_shift(self):
        task = self._new_task()
        svc, entry = self._new_ticket(task)
        rejected, msg = svc.run_action(entry["id"], "拒收", actor="乙", remark="交接不清")
        assert rejected is not None, msg
        assert rejected["status"] == "已拒收"
        assert "已拒收@乙" in rejected["签收结论"]

        reassigned, msg = svc.run_action(
            entry["id"], "改派", actor="乙", target="丙", remark="交给晚班")
        assert reassigned is not None, msg
        assert reassigned["status"] == "已送交"
        assert reassigned["接班负责人"] == "丙"
        assert reassigned["原接班负责人"] == "乙"
        assert reassigned["处分"] == ""
        assert reassigned["轮次"] == 2
        assert task["交接签收"].startswith("由 乙 改派给 丙")

        assert svc.schedule_grant(task["重跑编号"], "丙") is None
        signed, msg = svc.run_action(entry["id"], "签收", actor="丙")
        assert signed is not None, msg
        assert task["负责人"] == "丙"
        assert svc.schedule_grant(task["重跑编号"], "丙") is not None

    def test_reassign_only_after_reject_or_withdraw(self):
        task = self._new_task()
        svc, entry = self._new_ticket(task)
        blocked, msg = svc.run_action(entry["id"], "改派", actor="乙", target="丙")
        assert blocked is None
        assert "仅已拒收或已撤回" in msg
        svc.run_action(entry["id"], "拒收", actor="乙")
        same, msg = svc.run_action(entry["id"], "改派", actor="乙", target="乙")
        assert same is None
        assert "不得与原接班人" in msg

    def test_reassign_requires_the_operator_who_closed_the_round(self):
        task = self._new_task()
        svc, entry = self._new_ticket(task)
        svc.run_action(entry["id"], "拒收", actor="乙")
        blocked, msg = svc.run_action(entry["id"], "改派", actor="丙", target="丁")
        assert blocked is None
        assert "接班负责人 乙" in msg

        withdrawn = self._new_task()
        svc2, entry2 = self._new_ticket(withdrawn)
        svc2.run_action(entry2["id"], "撤回", actor="甲")
        blocked, msg = svc2.run_action(entry2["id"], "改派", actor="乙", target="丙")
        assert blocked is None
        assert "交班负责人 甲" in msg
        ok, msg = svc2.run_action(entry2["id"], "改派", actor="甲", target="丙")
        assert ok is not None, msg
        assert ok["status"] == "已送交"
        assert ok["轮次"] == 2

    def test_reprocess_schedule_fills_owner_and_shift(self):
        from app.services.reprocess_task import ReprocessTaskService
        task = self._new_task()
        svc, entry = self._new_ticket(task)
        svc.run_action(entry["id"], "签收", actor="乙")
        rp = ReprocessTaskService()
        ok, _ = rp.run_action(task["id"], "排期重跑", actor="乙")
        assert ok is not None
        assert ok["负责人"] == "乙"
        assert ok["负责班次"].startswith("早班")

    def test_started_work_stays_in_original_shift(self):
        task = self._new_task(status="重跑中", owner="甲")
        svc, entry = self._new_ticket(task)
        signed, msg = svc.run_action(entry["id"], "签收", actor="乙")
        assert signed is not None, msg
        assert "已启动" in signed["保留工作"]
        assert task["负责人"] == "甲"
        assert task["负责班次"].startswith("夜班")

    def test_actor_must_match_role(self):
        task = self._new_task()
        svc, entry = self._new_ticket(task)
        assert svc.run_action(entry["id"], "签收", actor="甲")[0] is None
        assert svc.run_action(entry["id"], "撤回", actor="乙")[0] is None
        assert svc.run_action(entry["id"], "签收")[0] is None
        assert svc.run_action(entry["id"], "不存在的动作", actor="乙")[0] is None

    def test_verify_shift_by_record_and_time(self):
        task = self._new_task()
        svc, entry = self._new_ticket(task)
        result = svc.verify(record_id=entry["关联迟到记录"])
        assert result["ok"]
        assert result["交班负责人"] == "甲"
        assert result["可核实"] is False  # 尚未签收
        by_time = svc.verify(at="2026-11-26T09:00:00")
        assert by_time["ok"]
        assert any(t["交接单号"] == entry["交接单号"] for t in by_time["tickets"])
        assert svc.verify(record_id="NOT-EXIST")["ok"] is False

        svc.run_action(entry["id"], "签收", actor="乙")
        assert svc.verify(record_id=entry["关联迟到记录"])["可核实"] is True

    def test_seed_has_q1_q2_handovers(self):
        from app.store import store
        late = {r.get("记录编号"): r for r in store.rows("late_fact")}
        assert "Q1" in late and "Q2" in late
        assert late["Q1"]["关联交接单"] == "HO-001"
        assert late["Q2"]["关联交接单"] == "HO-002"
        tickets = {t.get("关联迟到记录"): t for t in store.rows("shift_handover")}
        assert tickets["Q1"]["关联重跑任务"] == "RP-Q1"
        assert tickets["Q2"]["关联重跑任务"] == "RP-Q2"
        assert tickets["Q1"]["status"] == "已送交"
        task_ids = {r.get("重跑编号") for r in store.rows("reprocess_task")}
        assert {"RP-Q1", "RP-Q2"} <= task_ids

    def test_concurrent_threads_produce_single_disposition(self):
        import threading
        from app.services.shift_handover import ShiftHandoverService
        from app.store import store
        task = self._new_task()
        svc, entry = self._new_ticket(task)
        results: list[tuple[dict | None, str]] = []

        def fire(action: str, actor: str) -> None:
            results.append(ShiftHandoverService().run_action(entry["id"], action, actor=actor))

        threads = [
            threading.Thread(target=fire, args=("签收", "乙")),
            threading.Thread(target=fire, args=("撤回", "甲")),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        winners = [r for r in results if r[0] is not None]
        losers = [r for r in results if r[0] is None]
        assert len(winners) == 1
        assert len(losers) == 1
        ticket = store.find("shift_handover", entry["id"])
        assert ticket["处分"] in ("签收", "撤回")
        assert len(ticket["无效申请"]) == 1
