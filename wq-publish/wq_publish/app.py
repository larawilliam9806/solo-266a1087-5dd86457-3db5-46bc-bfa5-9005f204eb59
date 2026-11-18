"""Flask 路由：上传校验 → 预览增删改 → 确认整批替换。"""
from __future__ import annotations

import os
import secrets
import sqlite3

from flask import (
    Flask,
    abort,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)

from .merging import (
    apply_decisions,
    build_entries,
    source_label,
    summarize,
)
from .storage import (
    LIVE_FILENAME,
    MergeSessionError,
    PublishConflictError,
    Storage,
)
from .validation import (
    format_concentration,
    parse_csv,
    validate_batch_no,
    validate_csv,
)

MAX_UPLOAD_BYTES = 16 * 1024 * 1024


def _merge_form_decisions(entries: list[dict], form) -> dict[int, dict]:
    """从表单读取冲突裁决；只保留真正提交了内容的冲突项。

    表单里没有出现的冲突项不做修改，从而让“保存已选项”保留会话里已有的
    裁决；发布时再由 ``apply_decisions(require_complete=True)`` 统一卡住
    未裁决项。
    """
    decisions: dict[int, dict] = {}
    for index, entry in enumerate(entries):
        if entry["status"] != "conflict":
            continue
        choice = form.get(f"choice_{index}")
        reason = form.get(f"reason_{index}", "")
        if choice is not None or reason.strip():
            decisions[index] = {"choice": choice, "reason": reason}
    return decisions


def _merge_summary_text(counts: dict) -> str:
    return (
        f"仅 A {counts['only_a']} / 仅 B {counts['only_b']} / "
        f"双份一致 {counts['same']} / 冲突 {counts['conflict']}"
    )


def create_app(data_dir: str | None = None, downstream_dir: str | None = None) -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_BYTES

    base = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    data_dir = data_dir or os.environ.get("WQ_DATA_DIR") or os.path.join(base, "data")
    downstream_dir = (
        downstream_dir
        or os.environ.get("WQ_DOWNSTREAM_DIR")
        or os.path.join(base, "downstream")
    )

    secret_path = os.path.join(data_dir, "flask_secret")
    os.makedirs(data_dir, exist_ok=True)
    if os.path.exists(secret_path):
        with open(secret_path, "rb") as fh:
            app.secret_key = fh.read()
    else:
        key = secrets.token_bytes(32)
        with open(secret_path, "wb") as fh:
            fh.write(key)
        os.chmod(secret_path, 0o600)
        app.secret_key = key

    storage = Storage(data_dir, downstream_dir)
    app.storage = storage  # 供自测脚本直接访问

    @app.template_filter("conc")
    def _conc(value):
        return format_concentration(value)

    @app.template_filter("source")
    def _source(value):
        return source_label(value)

    @app.get("/")
    def index():
        reg = storage.current_registry()
        rows = storage.current_rows()
        events = storage.recent_events()
        merge_versions = storage.merge_versions()
        return render_template(
            "index.html", reg=reg, rows=rows, events=events,
            merge_versions=merge_versions,
            live_filename=LIVE_FILENAME,
        )

    @app.post("/preview")
    def preview():
        upload = request.files.get("csv_file")
        batch_no = request.form.get("batch_no", "")
        if upload is None or not upload.filename:
            flash("请选择仪器导出的 CSV 文件", "error")
            return redirect(url_for("index"))
        raw = upload.read()
        if len(raw) > MAX_UPLOAD_BYTES:
            flash("CSV 文件超过 16MB 上限", "error")
            return redirect(url_for("index"))

        rows, errors = validate_csv(raw, batch_no)
        if errors:
            for msg in errors:
                flash(msg, "error")
            return redirect(url_for("index"))

        reg = storage.current_registry()
        base_version = reg["current_version"] if reg else 0
        token = storage.create_session(batch_no.strip(), base_version, rows)
        diff = storage.preview_diff(rows)
        storage.log(
            "preview",
            f"会话 {token[:8]} 基于 v{base_version} 预览批次 {batch_no.strip()}："
            f"新增 {len(diff['inserts'])} / 修改 {len(diff['updates'])} / "
            f"撤回 {len(diff['deletes'])} / 不变 {len(diff['unchanged'])}",
        )
        return render_template(
            "preview.html",
            token=token,
            batch_no=batch_no.strip(),
            base_version=base_version,
            current_version=base_version,
            rows=rows,
            diff=diff,
            live_filename=LIVE_FILENAME,
        )

    @app.post("/publish/<token>")
    def publish(token):
        session = storage.get_session(token)
        if session is None:
            flash("修订会话不存在或已过期，请重新上传 CSV", "error")
            return redirect(url_for("index"))

        reg = storage.current_registry()
        current_version = reg["current_version"] if reg else 0
        if session["status"] != "pending" or session["base_version"] != current_version:
            # 会话在预览期间被别的发布超越：作废会话，展示基于旧版的差异，要求重新预览
            if session["status"] == "pending":
                storage.mark_obsolete(token, current_version)
                stale_reason = (
                    f"您基于 v{session['base_version']} 发起修订，"
                    f"但当前版本已变为 v{current_version}（另一页面已成功发布）"
                )
            else:
                reason_map = {
                    "published": "该修订已经发布，不能重复发布",
                    "obsolete": "该预览已被更新的发布超越而作废",
                    "cancelled": "该修订已被放弃（取消）",
                }
                stale_reason = reason_map.get(session["status"], "该修订会话已失效")
            diff = storage.preview_diff(session["rows"])
            storage.log(
                "conflict",
                f"会话 {token[:8]} 发布被拒（基线 v{session['base_version']}，"
                f"当前 v{current_version}，状态 {session['status']}）",
            )
            return render_template(
                "preview.html",
                token=token,
                batch_no=session["batch_no"],
                base_version=session["base_version"],
                current_version=current_version,
                rows=session["rows"],
                diff=diff,
                stale=True,
                stale_reason=stale_reason,
                live_filename=LIVE_FILENAME,
            ), 409

        try:
            new_version, row_count = storage.publish(token)
        except PublishConflictError as exc:
            flash(str(exc), "error")
            return redirect(url_for("index"))
        except sqlite3.OperationalError as exc:  # 极端并发下写锁等待超时
            app.logger.warning("publish lock contention: %s", exc)
            flash("当前有其他发布正在写入，请稍后重新预览再发布", "error")
            return redirect(url_for("index"))
        flash(f"发布成功：v{new_version} 已生效，共 {row_count} 行", "success")
        return redirect(url_for("index"))

    @app.post("/cancel/<token>")
    def cancel(token):
        if storage.cancel_session(token):
            flash("已放弃本次修订，当前发布版本未受影响", "info")
        return redirect(url_for("index"))

    # ---------- 双文件合并 ----------
    def _render_merge(
        *,
        token,
        batch_no,
        base_version,
        current_version,
        filename_a,
        filename_b,
        entries,
        stale=False,
        stale_reason="",
    ):
        return render_template(
            "merge_preview.html",
            token=token,
            batch_no=batch_no,
            base_version=base_version,
            current_version=current_version,
            filename_a=filename_a,
            filename_b=filename_b,
            entries=entries,
            counts=summarize(entries),
            stale=stale,
            stale_reason=stale_reason,
            live_filename=LIVE_FILENAME,
        )

    def _render_merge_from_session(token, session, entries):
        reg = storage.current_registry()
        current_version = reg["current_version"] if reg else 0
        return _render_merge(
            token=token,
            batch_no=session["batch_no"],
            base_version=session["base_version"],
            current_version=current_version,
            filename_a=session["filename_a"],
            filename_b=session["filename_b"],
            entries=entries,
        )

    def _render_merge_conflict(token, session, entries, exc):
        current_version = exc.current_version
        if current_version is None:
            reg = storage.current_registry()
            current_version = reg["current_version"] if reg else 0
        if exc.status == "obsolete":
            reason = (
                f"该合并预览基于 v{session['base_version']} 发起，"
                f"但当前版本已变为 v{current_version}。"
                "此前保存的来源选择与理由已整体失效，本次不会发布任何内容"
            )
        else:
            reason = str(exc)
        storage.log(
            "conflict",
            f"合并会话 {token[:8]} 发布/保存被拒（基线 v{session['base_version']}，"
            f"当前 v{current_version}，状态 {session['status'] or exc.status}）",
        )
        return _render_merge(
            token=token,
            batch_no=session["batch_no"],
            base_version=session["base_version"],
            current_version=current_version,
            filename_a=session["filename_a"],
            filename_b=session["filename_b"],
            entries=entries,
            stale=True,
            stale_reason=reason,
        ), 409

    @app.get("/merge")
    def merge_page():
        return render_template(
            "merge_index.html", live_filename=LIVE_FILENAME
        )

    @app.post("/merge/preview")
    def merge_preview():
        batch_no = request.form.get("batch_no", "")
        upload_a = request.files.get("csv_a")
        upload_b = request.files.get("csv_b")
        if upload_a is None or not upload_a.filename:
            flash("请选择仪器 A 导出的 CSV 文件", "error")
            return redirect(url_for("merge_page"))
        if upload_b is None or not upload_b.filename:
            flash("请选择仪器 B 导出的 CSV 文件", "error")
            return redirect(url_for("merge_page"))

        batch_errors = validate_batch_no(batch_no)
        if batch_errors:
            for msg in batch_errors:
                flash(msg, "error")
            return redirect(url_for("merge_page"))

        raw_a = upload_a.read()
        raw_b = upload_b.read()
        if len(raw_a) > MAX_UPLOAD_BYTES or len(raw_b) > MAX_UPLOAD_BYTES:
            flash("单个 CSV 文件不能超过 16MB 上限", "error")
            return redirect(url_for("merge_page"))

        # 沿用原有列校验：任一份文件内部有重复键/非法值即整份拒绝，不进入预览
        rows_a, errors_a = parse_csv(raw_a)
        rows_b, errors_b = parse_csv(raw_b)
        if errors_a or errors_b:
            if errors_a:
                flash(f"仪器 A（{upload_a.filename}）校验失败，整份拒绝：", "error")
                for msg in errors_a:
                    flash(msg, "error")
            if errors_b:
                flash(f"仪器 B（{upload_b.filename}）校验失败，整份拒绝：", "error")
                for msg in errors_b:
                    flash(msg, "error")
            return redirect(url_for("merge_page"))

        entries = build_entries(rows_a, rows_b)
        reg = storage.current_registry()
        base_version = reg["current_version"] if reg else 0
        batch_no_clean = batch_no.strip()
        token = storage.create_merge_session(
            batch_no_clean, base_version, upload_a.filename, upload_b.filename, entries
        )
        counts = summarize(entries)
        storage.log(
            "merge_preview",
            f"合并会话 {token[:8]} 基于 v{base_version} 预览批次 {batch_no_clean}"
            f"（A={upload_a.filename} / B={upload_b.filename}）：{_merge_summary_text(counts)}",
        )
        return _render_merge(
            token=token,
            batch_no=batch_no_clean,
            base_version=base_version,
            current_version=base_version,
            filename_a=upload_a.filename,
            filename_b=upload_b.filename,
            entries=entries,
        )

    @app.post("/merge/<token>/save")
    def merge_save(token):
        session = storage.get_merge_session(token)
        if session is None:
            flash("合并会话不存在或已过期，请重新上传两个文件", "error")
            return redirect(url_for("merge_page"))

        entries = session["entries"]
        decisions = _merge_form_decisions(entries, request.form)
        errors = apply_decisions(entries, decisions, require_complete=False)
        if errors:
            for msg in errors:
                flash(msg, "error")
            return _render_merge_from_session(token, session, entries)

        reg = storage.current_registry()
        current_version = reg["current_version"] if reg else 0
        try:
            storage.save_merge_decisions(
                token, entries, current_version=current_version
            )
        except MergeSessionError as exc:
            return _render_merge_conflict(token, session, entries, exc)
        flash("已保存冲突项的来源与理由，全部选完后即可发布", "info")
        return _render_merge(
            token=token,
            batch_no=session["batch_no"],
            base_version=session["base_version"],
            current_version=current_version,
            filename_a=session["filename_a"],
            filename_b=session["filename_b"],
            entries=entries,
        )

    @app.post("/merge/<token>/publish")
    def merge_publish(token):
        session = storage.get_merge_session(token)
        if session is None:
            flash("合并会话不存在或已过期，请重新上传两个文件", "error")
            return redirect(url_for("merge_page"))

        reg = storage.current_registry()
        current_version = reg["current_version"] if reg else 0
        if session["status"] != "pending" or session["base_version"] != current_version:
            if session["status"] == "pending":
                # 会话在预览期间被别的发布超越：立即作废，旧裁决一律失效
                storage.mark_merge_obsolete(token, current_version)
                session = storage.get_merge_session(token)
            exc = MergeSessionError(
                Storage._merge_status_message(session["status"]),
                current_version=current_version,
                base_version=session["base_version"],
                status=session["status"],
            )
            return _render_merge_conflict(token, session, session["entries"], exc)

        entries = session["entries"]
        decisions = _merge_form_decisions(entries, request.form)
        errors = apply_decisions(entries, decisions, require_complete=True)
        if errors:
            for msg in errors:
                flash(msg, "error")
            return _render_merge_from_session(token, session, entries)

        try:
            new_version, row_count = storage.publish_merge(token, entries=entries)
        except MergeSessionError as exc:
            return _render_merge_conflict(token, session, entries, exc)
        except OSError as exc:  # 下游磁盘写入失败：事务已回滚、文件已恢复
            app.logger.warning("merge publish file write failed: %s", exc)
            flash("发布失败：下游文件写入出错，本次未留下任何版本或来源记录，请重试", "error")
            return _render_merge_from_session(token, session, entries)
        except sqlite3.OperationalError as exc:  # 写锁等待超时
            app.logger.warning("merge publish lock contention: %s", exc)
            flash("当前有其他发布正在写入，请稍后重新预览再发布", "error")
            return redirect(url_for("merge_page"))
        flash(
            f"合并发布成功：v{new_version} 已生效，共 {row_count} 行，"
            f"逐行来源已登记，可在合并版本页核对",
            "success",
        )
        return redirect(url_for("merge_versions"))

    @app.post("/merge/<token>/cancel")
    def merge_cancel(token):
        if storage.cancel_merge_session(token):
            flash("已放弃本次合并，当前发布版本未受影响", "info")
        return redirect(url_for("merge_page"))

    @app.get("/merge/versions")
    def merge_versions():
        versions = storage.merge_versions()
        reg = storage.current_registry()
        return render_template(
            "merge_versions.html",
            versions=versions,
            current_version=reg["current_version"] if reg else 0,
            live_filename=LIVE_FILENAME,
        )

    @app.get("/merge/versions/<int:version>")
    def merge_version_detail(version):
        detail = storage.merge_version_rows(version)
        if not detail["sources"]:
            abort(404, description=f"v{version} 不是两次文件合并发布的版本")
        reg = storage.current_registry()
        return render_template(
            "merge_version.html",
            version=version,
            detail=detail,
            is_current=bool(reg and reg["current_version"] == version),
            live_filename=LIVE_FILENAME,
        )

    @app.get("/published.csv")
    def published_csv():
        if not os.path.exists(storage.live_path):
            abort(404, description="尚无已发布的 CSV")
        return send_file(
            storage.live_path, mimetype="text/csv",
            download_name=LIVE_FILENAME, as_attachment=False,
        )

    @app.errorhandler(413)
    def too_large(_):
        flash("上传文件超过 16MB 上限", "error")
        return redirect(url_for("index"))

    return app
