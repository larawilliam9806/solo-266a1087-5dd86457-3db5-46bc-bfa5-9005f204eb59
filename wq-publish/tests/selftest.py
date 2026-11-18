"""关键流程自测（不依赖 pytest，直接 python -I tests/selftest.py 运行）。

覆盖：
1. CSV 校验：中文表头、重复 (样本,项目)、负数/非数字/空值/缺列/缺批次号
2. 端到端：上传 → 预览(增/改/删/不变分类) → 发布 → 登记版本/行数 → 下游文件哈希一致
3. 并发：两个页面从同一版本发起，仅一个发布成功，另一个 409 须重新预览
4. 崩溃恢复：文件缺失、文件被改坏、遗留临时文件、游离文件、改名后未提交
5. 撤回语义：整批替换后旧行消失；会话取消/重复发布防护
6. 双文件合并：文件内重复键整份拒绝、仅一份采用、双份一致合一、
   冲突逐项选择来源+理由、未选完不得发布、逐行来源入库、按版本核对、
   确认前版本变化使旧裁决失效、发布失败不留部分更新、会话取消
"""
import hashlib
import io
import os
import re
import shutil
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from wq_publish.app import create_app
from wq_publish.storage import LIVE_FILENAME
from wq_publish.validation import render_csv, validate_csv

PASS, FAIL = 0, 0
EXAMPLE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "examples")


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {detail}")


def read_example(fname):
    with open(os.path.join(EXAMPLE_DIR, fname), "rb") as fh:
        return fh.read()


def upload(client, fname, batch_no="B1"):
    return client.post(
        "/preview",
        data={"batch_no": batch_no,
              "csv_file": (io.BytesIO(read_example(fname)), fname)},
        content_type="multipart/form-data",
    )


def merge_upload(client, fname_a, fname_b, batch_no="M1"):
    return client.post(
        "/merge/preview",
        data={"batch_no": batch_no,
              "csv_a": (io.BytesIO(read_example(fname_a)), fname_a),
              "csv_b": (io.BytesIO(read_example(fname_b)), fname_b)},
        content_type="multipart/form-data",
    )


def merge_token(resp):
    match = re.search(rb'/merge/([0-9a-f]+)/publish', resp.data)
    return match.group(1).decode() if match else None


# ---------------- 1. 校验单元测试 ----------------
print("== 1. CSV 校验 ==")
rows, errs = validate_csv(read_example("batch_v1.csv"), "B1")
check("合法 CSV 通过并解析出 5 行", errs == [] and len(rows) == 5, str(errs))

rows, errs = validate_csv(read_example("batch_invalid.csv"), "B1")
check("重复(样本,项目)被拒", any("重复" in e for e in errs), str(errs))
check("负浓度被拒", any("负数" in e for e in errs), str(errs))
check("空浓度被拒", any("浓度为空" in e for e in errs), str(errs))
check("空项目被拒", any("检测项目为空" in e for e in errs), str(errs))

bad = "样本号,浓度\nS1,1.0\n".encode()
rows, errs = validate_csv(bad, "B1")
check("缺少检测项目列被拒", rows is None and any("表头" in e for e in errs), str(errs))

rows, errs = validate_csv(read_example("batch_v1.csv"), " ")
check("空批次号被拒", any("批次号" in e for e in errs), str(errs))

for text in ["S1,氨氮,nan", "S1,氨氮,inf", "S1,氨氮,abc"]:
    raw = f"样本号,检测项目,浓度\n{text}\n".encode()
    rows, errs = validate_csv(raw, "B1")
    check(f"非法浓度 {text!r} 被拒", rows is None and len(errs) > 0, str(errs))

raw_zero = "样本号,检测项目,浓度\nS1,氨氮,0\n".encode()
rows, errs = validate_csv(raw_zero, "B1")
check("零浓度（非负边界）通过", errs == [] and rows[0]["concentration"] == 0.0, str(errs))

# ---------------- 2-5. 端到端（隔离临时目录） ----------------
tmp = tempfile.mkdtemp(prefix="wq-selftest-")
data_dir = os.path.join(tmp, "data")
down_dir = os.path.join(tmp, "downstream")
app = create_app(data_dir=data_dir, downstream_dir=down_dir)
client = app.test_client()

print("== 2. 首次发布 v1 ==")
resp = upload(client, "batch_v1.csv", "BATCH-001")
check("预览页 200 且显示增删改", resp.status_code == 200
      and "新增".encode() in resp.data and "撤回".encode() in resp.data)
m = re.search(rb'/publish/([0-9a-f]+)', resp.data)
check("预览页含发布令牌", m is not None)
token1 = m.group(1).decode()

resp = client.post(f"/publish/{token1}")
check("确认发布后 302 回首页", resp.status_code == 302)
reg = app.storage.current_registry()
check("登记为 v1、5 行", reg["current_version"] == 1 and reg["row_count"] == 5,
      f"got {dict(reg) if reg else None}")
live = os.path.join(down_dir, LIVE_FILENAME)
check("下游 CSV 已生成", os.path.exists(live))
file_hash = hashlib.sha256(open(live, "rb").read()).hexdigest()
check("文件哈希与登记一致", file_hash == reg["content_hash"])
check("下游目录无临时文件残留",
      not [n for n in os.listdir(down_dir) if n.startswith(".tmp-")])
resp = client.get("/published.csv")
check("已发布 CSV 可下载", resp.status_code == 200 and b"S001" in resp.data)
resp = client.post(f"/publish/{token1}")
check("同一会话不可重复发布（返回 409 过期页）", resp.status_code == 409
      and app.storage.get_session(token1)["status"] == "published")

print("== 3. 修订预览的增/改/删/不变分类 ==")
resp = upload(client, "batch_v2_revision.csv", "BATCH-002")
body = resp.data.decode()
# v2 相对 v1：S004氨氮新增；S001氨氮、S002COD 修改；S002氨氮 撤回；S001总磷/S003总磷 不变
check("新增 1 行", "+1" in body and "S004" in body)
check("修改 2 行", "~2" in body and "1.25" in body and "35.0" in body)
check("撤回 1 行（S002 氨氮）", "−1" in body and "S002" in body)
check("不变 2 行", "不变项 2" in body)
token2 = re.search(rb'/publish/([0-9a-f]+)', resp.data).group(1).decode()
client.post(f"/publish/{token2}")
reg = app.storage.current_registry()
check("发布后登记为 v2、5 行", reg["current_version"] == 2 and reg["row_count"] == 5)
current = app.storage.current_rows()
check("撤回生效：S002 氨氮已不存在",
      not any(r["sample_no"] == "S002" and r["analyte"] == "氨氮" for r in current))
check("修改生效：S001 氨氮=1.3",
      any(r["sample_no"] == "S001" and r["analyte"] == "氨氮"
          and r["concentration"] == 1.3 for r in current))
file_hash = hashlib.sha256(open(live, "rb").read()).hexdigest()
check("v2 文件哈希与登记一致", file_hash == reg["content_hash"])

print("== 4. 同版本并发修订：只允许一个成功 ==")
r_a = upload(client, "batch_v1.csv", "CONCUR-A")
r_b = upload(client, "batch_v2_revision.csv", "CONCUR-B")
tok_a = re.search(rb'/publish/([0-9a-f]+)', r_a.data).group(1).decode()
tok_b = re.search(rb'/publish/([0-9a-f]+)', r_b.data).group(1).decode()
ok = client.post(f"/publish/{tok_a}")
check("A 发布成功 → v3", ok.status_code == 302
      and app.storage.current_registry()["current_version"] == 3)
conflict = client.post(f"/publish/{tok_b}")
check("B 发布被拒并返回 409 过期预览页", conflict.status_code == 409
      and "该预览已过期".encode() in conflict.data)
sess_b = app.storage.get_session(tok_b)
check("B 会话被标记 obsolete", sess_b["status"] == "obsolete")
reg_before = app.storage.current_registry()["current_version"]
retry = upload(client, "batch_v2_revision.csv", "CONCUR-B-RETRY")
tok_b2 = re.search(rb'/publish/([0-9a-f]+)', retry.data).group(1).decode()
client.post(f"/publish/{tok_b2}")
check("B 重新预览后可发布 → v4",
      app.storage.current_registry()["current_version"] == reg_before + 1)
check("A 的发布未被覆盖：v4 文件行数=5",
      app.storage.current_registry()["row_count"] == 5)

print("== 5. 崩溃后重启自动恢复 ==")
# 5a 下游文件被删除
os.unlink(live)
app2 = create_app(data_dir=data_dir, downstream_dir=down_dir)
reg = app2.storage.current_registry()
check("5a 文件缺失后重建且哈希与登记一致",
      os.path.exists(live)
      and hashlib.sha256(open(live, "rb").read()).hexdigest() == reg["content_hash"])

# 5b 文件被外部改坏
with open(live, "ab") as fh:
    fh.write(b"CORRUPTED\n")
app3 = create_app(data_dir=data_dir, downstream_dir=down_dir)
reg = app3.storage.current_registry()
check("5b 损坏文件被按登记重写",
      hashlib.sha256(open(live, "rb").read()).hexdigest() == reg["content_hash"])

# 5c 遗留半写临时文件
tmp_orphan = os.path.join(down_dir, ".tmp-publish-deadbeef.csv")
open(tmp_orphan, "wb").write(b"partial")
create_app(data_dir=data_dir, downstream_dir=down_dir)
check("5c 遗留临时文件被清理", not os.path.exists(tmp_orphan))

# 5d 下游目录出现未登记游离文件
stranger = os.path.join(down_dir, "stranger.csv")
open(stranger, "wb").write(b"x")
create_app(data_dir=data_dir, downstream_dir=down_dir)
check("5d 游离文件被移入 quarantine",
      not os.path.exists(stranger)
      and any(f.endswith("stranger.csv")
              for f in os.listdir(os.path.join(data_dir, "quarantine"))))

# 5e 模拟“文件已改名但事务未提交”：文件内容比登记新，重启应回滚到登记版本
with app3.storage._connect() as conn:
    old_csv = app3.storage._render_version(conn, reg["current_version"])
newer = render_csv([{"sample_no": "ZX", "analyte": "氨氮", "concentration": 9.9}])
app3.storage._atomic_write(newer)
check("预置：文件已被换成未登记的新版本",
      open(live, "rb").read().decode() != old_csv)
app4 = create_app(data_dir=data_dir, downstream_dir=down_dir)
check("5e 改名后未提交 → 恢复为登记的完整旧版",
      open(live, "rb").read().decode() == old_csv)
events = app4.storage.recent_events(50)
check("恢复动作写入事件日志", any(e["kind"] == "recover" for e in events))

print("== 6. 会话取消 ==")
r = upload(client, "batch_v1.csv", "CANCEL-ME")
tok = re.search(rb'/publish/([0-9a-f]+)', r.data).group(1).decode()
v_before = app.storage.current_registry()["current_version"]
client.post(f"/cancel/{tok}")
check("取消后发布被拒绝（409 且会话 cancelled）",
      client.post(f"/publish/{tok}").status_code == 409
      and app.storage.get_session(tok)["status"] == "cancelled")
check("取消不影响当前版本",
      app.storage.current_registry()["current_version"] == v_before)

# ---------------- 7. 双文件合并 ----------------
print("== 7. 双文件合并校验与预览 ==")


def entry_of(session, sample_no, analyte):
    for i, e in enumerate(session["entries"]):
        if e["sample_no"] == sample_no and e["analyte"] == analyte:
            return i, e
    raise AssertionError(f"缺少条目 {sample_no}/{analyte}")


v_before_merge = app.storage.current_registry()["current_version"]
resp = merge_upload(client, "merge_a.csv", "merge_dup_b.csv", "MERGE-DUP")
check("B 文件内重复键 → 整份拒收（302 回上传页）", resp.status_code == 302)
check("被拒时未创建合并会话",
      app.storage.merge_versions() == []
      and app.storage.current_registry()["current_version"] == v_before_merge)

resp = merge_upload(client, "merge_a.csv", "merge_b.csv", "MERGE-001")
body = resp.data.decode()
check("合并预览 200 并展示四类计数", resp.status_code == 200
      and "浓度冲突" in body and "双份一致" in body and "仅 A 出现" in body)
tok_m = merge_token(resp)
check("合并预览含发布令牌", tok_m is not None)
sess = app.storage.get_merge_session(tok_m)
check("仅 A 1 项（S003 总磷）",
      entry_of(sess, "S003", "总磷")[1]["status"] == "only_a")
check("仅 B 1 项（S003 氨氮）",
      entry_of(sess, "S003", "氨氮")[1]["status"] == "only_b")
check("双份一致 3 项", sum(1 for e in sess["entries"] if e["status"] == "same") == 3)
check("合并后共 6 个（样本,项目）", len(sess["entries"]) == 6)
i_conf, conf = entry_of(sess, "S001", "氨氮")
check("S001 氨氮为冲突项且值不同", conf["status"] == "conflict"
      and conf["value_a"] == 1.25 and conf["value_b"] == 1.30)

print("== 8. 未裁决不得发布；保存裁决后整批发布 ==")
resp = client.post(f"/merge/{tok_m}/publish")
check("未选来源时拒绝发布（200 且提示冲突项）", resp.status_code == 200
      and "请选择采用仪器 A 或仪器 B".encode() in resp.data)
check("未裁决发布不改变版本",
      app.storage.current_registry()["current_version"] == v_before_merge)
check("未裁决发布会话仍 pending",
      app.storage.get_merge_session(tok_m)["status"] == "pending")

resp = client.post(f"/merge/{tok_m}/publish", data={f"choice_{i_conf}": "A"})
check("仅选来源未填理由仍拒绝", resp.status_code == 200
      and "请填写选择来源的理由".encode() in resp.data)

resp = client.post(f"/merge/{tok_m}/save", data={f"choice_{i_conf}": "B"})
check("保存部分裁决成功并回显", resp.status_code == 200
      and app.storage.get_merge_session(tok_m)["entries"][i_conf]["chosen"] == "B")

resp = client.post(
    f"/merge/{tok_m}/publish",
    data={f"choice_{i_conf}": "A", f"reason_{i_conf}": "以仪器 A 的复测结果为准"},
)
check("全部裁决后发布成功", resp.status_code == 302)
new_version = app.storage.current_registry()["current_version"]
check("合并发布版本递增为 v%d" % (v_before_merge + 1),
      new_version == v_before_merge + 1)
check("合并结果共 6 行（每个样本项目仅一行）",
      app.storage.current_registry()["row_count"] == 6
      and len({(r["sample_no"], r["analyte"]) for r in app.storage.current_rows()}) == 6)
rows = app.storage.current_rows()
check("S001 氨氮 采用仪器 A 的 1.25，只保留一行",
      [(r["concentration"]) for r in rows
       if r["sample_no"] == "S001" and r["analyte"] == "氨氮"] == [1.25])
check("仅 B 的 S003 氨氮 被采用 = 2.20",
      any(r["sample_no"] == "S003" and r["analyte"] == "氨氮"
          and r["concentration"] == 2.2 for r in rows))
check("仅 A 的 S003 总磷 被采用 = 0.00",
      any(r["sample_no"] == "S003" and r["analyte"] == "总磷"
          and r["concentration"] == 0.0 for r in rows))
live = os.path.join(down_dir, LIVE_FILENAME)
check("下游 CSV 哈希与登记一致",
      hashlib.sha256(open(live, "rb").read()).hexdigest()
      == app.storage.current_registry()["content_hash"])
sources = {(s["sample_no"], s["analyte"]): s for s in
           app.storage.merge_sources(new_version)}
check("逐行来源齐全 6 条", len(sources) == 6)
check("双份一致来源记为 both",
      sources[("S001", "总磷")]["source"] == "both")
check("仅 A/仅 B 来源正确",
      sources[("S003", "总磷")]["source"] == "A"
      and sources[("S003", "氨氮")]["source"] == "B")
conf_src = sources[("S001", "氨氮")]
check("冲突行记录选定来源、原始双值与理由",
      conf_src["source"] == "A" and conf_src["conflict"] == 1
      and conf_src["value_a"] == 1.25 and conf_src["value_b"] == 1.30
      and conf_src["reason"] == "以仪器 A 的复测结果为准")
check("已发布会话标记 published",
      app.storage.get_merge_session(tok_m)["status"] == "published")
check("重复发布同一合并会话被拒", client.post(
    f"/merge/{tok_m}/publish").status_code in (302, 409))

print("== 9. 网页按版本核对（重启后仍可核对） ==")
resp = client.get("/merge/versions")
check("合并版本列表页 200 且含新版本", resp.status_code == 200
      and f"v{new_version}".encode() in resp.data)
resp = client.get(f"/merge/versions/{new_version}")
detail = resp.data.decode()
check("版本核对页显示来源与冲突理由", resp.status_code == 200
      and "仪器 A" in detail and "双份一致" in detail
      and "以仪器 A 的复测结果为准" in detail)

app_restart = create_app(data_dir=data_dir, downstream_dir=down_dir)
detail_after = app_restart.storage.merge_version_rows(new_version)
check("重启后仍能读出该版本的逐行来源",
      len(detail_after["sources"]) == 6
      and detail_after["sources"][("S001", "氨氮")]["reason"]
      == "以仪器 A 的复测结果为准")
check("重启后合并版本列表仍包含该版本",
      any(v["version"] == new_version for v in app_restart.storage.merge_versions()))

print("== 10. 确认前版本变化 → 旧裁决整体失效 ==")
old_version = app.storage.current_registry()["current_version"]
resp = merge_upload(client, "merge_a.csv", "merge_b.csv", "MERGE-STALE")
tok_s = merge_token(resp)
sess_s = app.storage.get_merge_session(tok_s)
i_s, _ = entry_of(sess_s, "S001", "氨氮")
client.post(f"/merge/{tok_s}/save",
            data={f"choice_{i_s}": "A", f"reason_{i_s}": "旧裁决理由"})
# 另一页面（单文件修订）先发布，基线被超越
r_other = upload(client, "batch_v2_revision.csv", "OTHER-PUBLISH")
tok_other = re.search(rb'/publish/([0-9a-f]+)', r_other.data).group(1).decode()
client.post(f"/publish/{tok_other}")
v_other = app.storage.current_registry()["current_version"]
check("另一页面发布成功，基线改变", v_other == old_version + 1)

resp = client.post(f"/merge/{tok_s}/publish",
                   data={f"choice_{i_s}": "A", f"reason_{i_s}": "旧裁决理由"})
check("旧合并会话发布返回 409 且提示版本变化", resp.status_code == 409
      and "版本" in resp.data.decode())
check("旧合并会话被标记 obsolete（裁决失效）",
      app.storage.get_merge_session(tok_s)["status"] == "obsolete")
check("失效后已保存的来源与理由被清空",
      all(e["chosen"] is None and e["reason"] == ""
          for e in app.storage.get_merge_session(tok_s)["entries"]))
check("旧合并会话未写入任何来源记录",
      not any(s["batch_no"] == "MERGE-STALE"
              for v in app.storage.merge_versions()
              for s in app.storage.merge_sources(v["version"])))
check("版本未被旧会话覆盖",
      app.storage.current_registry()["current_version"] == v_other)
resp = client.post(f"/merge/{tok_s}/save",
                   data={f"choice_{i_s}": "A", f"reason_{i_s}": "x"})
check("失效会话也不能再保存裁决", resp.status_code == 409)

print("== 11. 发布失败不留部分更新 ==")
resp = merge_upload(client, "merge_a.csv", "merge_b.csv", "MERGE-FAIL")
tok_f = merge_token(resp)
sess_f = app.storage.get_merge_session(tok_f)
i_f, _ = entry_of(sess_f, "S001", "氨氮")
v_fail_base = app.storage.current_registry()["current_version"]
live_before_fail = open(live, "rb").read()
original_write = app.storage._atomic_write_bytes


def boom(_payload):
    raise OSError("模拟下游磁盘写入失败")


app.storage._atomic_write_bytes = boom
try:
    client.post(f"/merge/{tok_f}/publish",
                data={f"choice_{i_f}": "A", f"reason_{i_f}": "失败演练"})
finally:
    app.storage._atomic_write_bytes = original_write
reg_after = app.storage.current_registry()
with sqlite3.connect(app.storage.db_path) as conn:
    partial_results = conn.execute(
        "SELECT COUNT(*) FROM results WHERE version=?", (v_fail_base + 1,)
    ).fetchone()[0]
    partial_sources = conn.execute(
        "SELECT COUNT(*) FROM merge_sources WHERE version=?", (v_fail_base + 1,)
    ).fetchone()[0]
check("发布失败不改登记版本", reg_after["current_version"] == v_fail_base)
check("发布失败不留 results/merge_sources 记录",
      partial_results == 0 and partial_sources == 0)
check("发布失败不改下游文件哈希",
      hashlib.sha256(open(live, "rb").read()).hexdigest()
      == reg_after["content_hash"])
check("发布失败后下游文件逐字节保持原样",
      open(live, "rb").read() == live_before_fail)
check("发布失败后会话仍可重新发布",
      app.storage.get_merge_session(tok_f)["status"] == "pending")
check("无临时文件残留",
      not [n for n in os.listdir(down_dir) if n.startswith(".tmp-")])

# 数据库写入阶段失败：文件已换新，必须回滚文件到发布前内容
original_log = app.storage._log


def failing_log(_conn, _kind, _detail):
    raise sqlite3.OperationalError("模拟写库失败")


app.storage._log = failing_log
try:
    client.post(f"/merge/{tok_f}/publish",
                data={f"choice_{i_f}": "A", f"reason_{i_f}": "失败演练2"})
finally:
    app.storage._log = original_log
with sqlite3.connect(app.storage.db_path) as conn:
    partial_results = conn.execute(
        "SELECT COUNT(*) FROM results WHERE version=?", (v_fail_base + 1,)
    ).fetchone()[0]
check("写库失败后登记未变",
      app.storage.current_registry()["current_version"] == v_fail_base)
check("写库失败后 results/来源记录均未残留",
      partial_results == 0
      and not any(v["version"] == v_fail_base + 1
                  for v in app.storage.merge_versions()))
check("写库失败后下游文件已回滚为发布前内容",
      open(live, "rb").read() == live_before_fail)
check("写库失败后会话仍 pending",
      app.storage.get_merge_session(tok_f)["status"] == "pending")
client.post(f"/merge/{tok_f}/publish",
            data={f"choice_{i_f}": "A", f"reason_{i_f}": "重试成功"})
check("重试后发布成功",
      app.storage.current_registry()["current_version"] == v_fail_base + 1)

print("== 12. 合并会话取消 ==")
resp = merge_upload(client, "merge_a.csv", "merge_b.csv", "MERGE-CANCEL")
tok_c = merge_token(resp)
v_cancel = app.storage.current_registry()["current_version"]
client.post(f"/merge/{tok_c}/cancel")
check("取消后发布被拒且状态 cancelled",
      client.post(f"/merge/{tok_c}/publish").status_code == 409
      and app.storage.get_merge_session(tok_c)["status"] == "cancelled")
check("取消不影响当前版本",
      app.storage.current_registry()["current_version"] == v_cancel)

shutil.rmtree(tmp, ignore_errors=True)
print(f"\n结果：{PASS} 通过，{FAIL} 失败")
sys.exit(1 if FAIL else 0)
