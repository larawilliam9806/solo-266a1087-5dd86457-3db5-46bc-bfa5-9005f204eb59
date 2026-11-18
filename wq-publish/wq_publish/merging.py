"""两台仪器双文件合并的领域逻辑。

合并规则
--------
* 每个文件先走 ``validation.validate_csv`` 的原有列校验（文件内重复键整份拒绝）；
* 只有一份文件出现的 (样本号, 检测项目)：直接采用该份结果；
* 两份都有且浓度相同：合为一行，来源记为“双份一致”；
* 两份都有但浓度不同：冲突项，必须逐项选择来源并填写理由，未选完不得发布。

来源值固定为 ``A`` / ``B`` / ``both``，配合版本级的文件名形成可追溯记录。
"""
from __future__ import annotations

REASON_MAX_LEN = 500
VALID_CHOICES = {"A", "B"}


class MergeValidationError(Exception):
    """裁决表单不完整或非法（未选来源 / 未填理由等）。"""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("；".join(errors))


def build_entries(rows_a: list[dict], rows_b: list[dict]) -> list[dict]:
    """把两个文件的行合并为按 (样本号, 检测项目) 排序的待裁决条目。"""
    map_a = {(r["sample_no"], r["analyte"]): r["concentration"] for r in rows_a}
    map_b = {(r["sample_no"], r["analyte"]): r["concentration"] for r in rows_b}
    entries: list[dict] = []
    for sample_no, analyte in sorted(set(map_a) | set(map_b)):
        value_a = map_a.get((sample_no, analyte))
        value_b = map_b.get((sample_no, analyte))
        if value_a is not None and value_b is not None:
            if value_a == value_b:
                status = "same"
            else:
                status = "conflict"
        elif value_a is not None:
            status = "only_a"
        else:
            status = "only_b"
        entries.append(
            {
                "sample_no": sample_no,
                "analyte": analyte,
                "value_a": value_a,
                "value_b": value_b,
                "status": status,
                "chosen": None,
                "reason": "",
            }
        )
    return entries


def entry_is_complete(entry: dict) -> bool:
    if entry["status"] != "conflict":
        return True
    return entry.get("chosen") in VALID_CHOICES and bool((entry.get("reason") or "").strip())


def summarize(entries: list[dict]) -> dict:
    counts = {
        "total": len(entries),
        "only_a": 0,
        "only_b": 0,
        "same": 0,
        "conflict": 0,
    }
    for entry in entries:
        counts[entry["status"]] += 1
    counts["unresolved"] = sum(
        1 for entry in entries
        if entry["status"] == "conflict" and not entry_is_complete(entry)
    )
    counts["complete"] = counts["unresolved"] == 0
    return counts


def apply_decisions(
    entries: list[dict], decisions: dict[int, dict], *, require_complete: bool
) -> list[str]:
    """把表单裁决写入 entries（原地修改），返回面向数据员的错误列表。

    ``require_complete=False`` 用于“保存选择”，允许只选了一部分；
    发布时必须用 ``True``，任何冲突项缺来源或理由都会被拒绝。
    """
    errors: list[str] = []

    for index, decision in decisions.items():
        if not isinstance(index, int) or index < 0 or index >= len(entries):
            errors.append(f"存在无效的裁决项（序号 {index}）")
            continue
        entry = entries[index]
        if entry["status"] != "conflict":
            errors.append(
                f"样本 {entry['sample_no']} / 项目 {entry['analyte']}"
                "不是冲突项，无需选择来源"
            )
            continue

        choice = (decision.get("choice") or "").strip().upper()
        reason = (decision.get("reason") or "").strip()
        if choice and choice not in VALID_CHOICES:
            errors.append(
                f"样本 {entry['sample_no']} / 项目 {entry['analyte']}："
                "来源只能是仪器 A 或仪器 B"
            )
        if len(reason) > REASON_MAX_LEN:
            errors.append(
                f"样本 {entry['sample_no']} / 项目 {entry['analyte']}："
                f"理由不能超过 {REASON_MAX_LEN} 字"
            )
        entry["chosen"] = choice if choice in VALID_CHOICES else None
        entry["reason"] = reason

    if require_complete:
        for entry in entries:
            if entry["status"] != "conflict":
                continue
            label = f"样本 {entry['sample_no']} / 项目 {entry['analyte']}"
            if entry.get("chosen") not in VALID_CHOICES:
                errors.append(f"冲突项 {label}：请选择采用仪器 A 或仪器 B 的浓度")
            elif not (entry.get("reason") or "").strip():
                errors.append(f"冲突项 {label}：请填写选择来源的理由")

    # 去重并保持顺序，避免同一问题重复提示
    return list(dict.fromkeys(errors))


def build_rows_and_sources(entries: list[dict]) -> tuple[list[dict], list[dict]]:
    """生成最终发布行与逐行来源记录；调用前必须已通过完整性校验。"""
    rows: list[dict] = []
    sources: list[dict] = []
    for entry in sorted(entries, key=lambda e: (e["sample_no"], e["analyte"])):
        status = entry["status"]
        sample_no = entry["sample_no"]
        analyte = entry["analyte"]
        if status == "only_a":
            value, source, conflict, reason = entry["value_a"], "A", 0, None
        elif status == "only_b":
            value, source, conflict, reason = entry["value_b"], "B", 0, None
        elif status == "same":
            value, source, conflict, reason = entry["value_a"], "both", 0, None
        elif status == "conflict":
            chosen = entry.get("chosen")
            if chosen not in VALID_CHOICES:
                raise ValueError(
                    f"冲突项 {sample_no} / {analyte} 尚未选择来源，不能生成发布数据"
                )
            value = entry["value_a"] if chosen == "A" else entry["value_b"]
            source, conflict, reason = chosen, 1, (entry.get("reason") or "").strip()
        else:  # pragma: no cover - 状态由 build_entries 固定生成
            raise ValueError(f"未知合并状态：{status}")

        rows.append(
            {"sample_no": sample_no, "analyte": analyte, "concentration": value}
        )
        sources.append(
            {
                "sample_no": sample_no,
                "analyte": analyte,
                "source": source,
                "conflict": conflict,
                "reason": reason,
                "value_a": entry["value_a"],
                "value_b": entry["value_b"],
            }
        )
    return rows, sources


def source_label(source: str | None) -> str:
    return {"A": "仪器 A", "B": "仪器 B", "both": "双份一致"}.get(source or "", "—")
