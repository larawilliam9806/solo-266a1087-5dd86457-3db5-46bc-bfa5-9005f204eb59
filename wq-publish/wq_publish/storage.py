"""SQLite 登记 + 下游目录原子发布 + 启动一致性恢复。

设计要点
--------
* SQLite 是版本/行数登记的唯一真相源；下游目录中的 CSV 只是它的镜像。
* 发布在单个 ``BEGIN IMMEDIATE`` 事务内完成：先写同目录临时文件并 fsync，
  再 ``os.replace`` 原子改名（下游只会看到完整的旧文件或新文件），
  最后提交事务登记新版本。
* 提交后进程若在改名完成前崩溃：重启时按数据库登记重新生成 CSV，
  并清理遗留临时文件；登记里不存在的游离文件会被移入 quarantine 隔离。
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone

from .merging import build_rows_and_sources, entry_is_complete
from .validation import format_concentration, render_csv

LIVE_FILENAME = "published_results.csv"


class PublishConflictError(Exception):
    """确认发布时基线版本已过期，需要重新预览。"""


class MergeSessionError(Exception):
    """双文件合并会话不可用（不存在 / 已发布 / 已取消 / 基线过期）。

    属性 ``current_version`` 供页面展示当前登记版本；``stale`` 表示会话曾
    基于旧基线，其已保存的裁决已整体失效。
    """

    def __init__(
        self,
        message: str,
        *,
        current_version: int | None = None,
        base_version: int | None = None,
        status: str | None = None,
    ):
        super().__init__(message)
        self.current_version = current_version
        self.base_version = base_version
        self.status = status

    @property
    def stale(self) -> bool:
        return self.status == "obsolete"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Storage:
    def __init__(self, data_dir: str, downstream_dir: str):
        self.data_dir = os.path.abspath(data_dir)
        self.downstream_dir = os.path.abspath(downstream_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.downstream_dir, exist_ok=True)
        self.db_path = os.path.join(self.data_dir, "wq_publish.db")
        self.live_path = os.path.join(self.downstream_dir, LIVE_FILENAME)
        self.quarantine_dir = os.path.join(self.data_dir, "quarantine")
        os.makedirs(self.quarantine_dir, exist_ok=True)
        self._init_db()
        self.recover()

    # ---------- 基础 ----------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=15000")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS registry (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    current_version INTEGER NOT NULL,
                    batch_no TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    published_at TEXT NOT NULL,
                    content_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version INTEGER NOT NULL,
                    batch_no TEXT NOT NULL,
                    sample_no TEXT NOT NULL,
                    analyte TEXT NOT NULL,
                    concentration REAL NOT NULL CHECK (concentration >= 0),
                    UNIQUE (version, sample_no, analyte)
                );
                CREATE INDEX IF NOT EXISTS idx_results_version
                    ON results(version);
                CREATE TABLE IF NOT EXISTS revision_sessions (
                    token TEXT PRIMARY KEY,
                    batch_no TEXT NOT NULL,
                    base_version INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('pending','published','obsolete','cancelled'))
                );
                CREATE TABLE IF NOT EXISTS event_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    detail TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS merge_sessions (
                    token TEXT PRIMARY KEY,
                    batch_no TEXT NOT NULL,
                    base_version INTEGER NOT NULL,
                    filename_a TEXT NOT NULL,
                    filename_b TEXT NOT NULL,
                    entries TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL
                        CHECK (status IN ('pending','published','obsolete','cancelled'))
                );
                CREATE TABLE IF NOT EXISTS merge_sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version INTEGER NOT NULL,
                    batch_no TEXT NOT NULL,
                    filename_a TEXT NOT NULL,
                    filename_b TEXT NOT NULL,
                    sample_no TEXT NOT NULL,
                    analyte TEXT NOT NULL,
                    concentration REAL NOT NULL CHECK (concentration >= 0),
                    source TEXT NOT NULL CHECK (source IN ('A','B','both')),
                    conflict INTEGER NOT NULL CHECK (conflict IN (0,1)),
                    value_a REAL,
                    value_b REAL,
                    reason TEXT,
                    UNIQUE (version, sample_no, analyte)
                );
                CREATE INDEX IF NOT EXISTS idx_merge_sources_version
                    ON merge_sources(version);
                """
            )

    def _log(self, conn: sqlite3.Connection, kind: str, detail: str) -> None:
        conn.execute(
            "INSERT INTO event_log(ts, kind, detail) VALUES (?,?,?)",
            (_now(), kind, detail),
        )

    def log(self, kind: str, detail: str) -> None:
        with self._connect() as conn:
            self._log(conn, kind, detail)

    # ---------- 启动恢复 ----------
    def recover(self) -> list[str]:
        """重启后核对下游文件与登记的一致性，返回本次恢复动作说明。"""
        actions: list[str] = []
        with self._connect() as conn:
            reg = conn.execute("SELECT * FROM registry WHERE id = 1").fetchone()

            # 1) 清理崩溃遗留的临时文件（可能只写了一半，绝不能被下游读到）
            for name in os.listdir(self.downstream_dir):
                if name.startswith(".tmp-publish-"):
                    path = os.path.join(self.downstream_dir, name)
                    try:
                        os.unlink(path)
                    except OSError:
                        pass
                    msg = f"清理遗留临时文件 downstream/{name}"
                    actions.append(msg)
                    self._log(conn, "recover", msg)

            # 2) 登记为空：正式文件（若存在）属于无登记游离文件，隔离并保留
            if reg is None:
                if os.path.exists(self.live_path):
                    dest = self._quarantine(LIVE_FILENAME)
                    msg = f"数据库无任何发布登记，将下游游离文件移入 {os.path.basename(dest)}"
                    actions.append(msg)
                    self._log(conn, "recover", msg)
                return actions

            expected_csv = self._render_version(conn, reg["current_version"])
            expected_hash = hashlib.sha256(expected_csv.encode("utf-8")).hexdigest()

            # 3) 文件缺失/哈希不符：以登记为准原子重建
            need_rewrite = True
            if os.path.exists(self.live_path):
                with open(self.live_path, "rb") as fh:
                    actual_hash = hashlib.sha256(fh.read()).hexdigest()
                need_rewrite = actual_hash != expected_hash

            if need_rewrite:
                self._atomic_write(expected_csv)
                msg = (
                    f"下游 CSV 与登记 v{reg['current_version']} 不一致，"
                    "已按数据库登记重建（恢复为完整新版本）"
                )
                actions.append(msg)
                self._log(conn, "recover", msg)
            else:
                self._log(
                    conn,
                    "recover",
                    f"启动核对通过：下游 CSV 与登记 v{reg['current_version']} 一致",
                )

            # 4) 同目录下游文件白名单：非登记文件一律隔离
            for name in os.listdir(self.downstream_dir):
                full = os.path.join(self.downstream_dir, name)
                if os.path.isfile(full) and name != LIVE_FILENAME:
                    dest = self._quarantine(name)
                    msg = f"下游目录存在未登记文件 {name}，已移入 {os.path.basename(dest)}"
                    actions.append(msg)
                    self._log(conn, "recover", msg)

            # 5) 超过 24 小时仍未确认的预览会话作废，避免无限期堆积
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(
                timespec="seconds"
            )
            stale = conn.execute(
                "SELECT token FROM revision_sessions "
                "WHERE status='pending' AND created_at < ?",
                (cutoff,),
            ).fetchall()
            for s in stale:
                conn.execute(
                    "UPDATE revision_sessions SET status='obsolete' WHERE token=?",
                    (s["token"],),
                )
                self._log(
                    conn, "recover",
                    f"预览会话 {s['token'][:8]} 超过 24 小时未确认，已作废",
                )
            stale_merges = conn.execute(
                "SELECT token FROM merge_sessions "
                "WHERE status='pending' AND created_at < ?",
                (cutoff,),
            ).fetchall()
            for s in stale_merges:
                conn.execute(
                    "UPDATE merge_sessions SET status='obsolete' WHERE token=?",
                    (s["token"],),
                )
                self._log(
                    conn, "recover",
                    f"双文件合并会话 {s['token'][:8]} 超过 24 小时未确认，已作废",
                )
        return actions

    def _quarantine(self, name: str) -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        dest = os.path.join(self.quarantine_dir, f"{stamp}-{name}")
        os.replace(os.path.join(self.downstream_dir, name), dest)
        return dest

    def _atomic_write(self, csv_text: str) -> None:
        self._atomic_write_bytes(csv_text.encode("utf-8"))

    def _atomic_write_bytes(self, payload: bytes) -> None:
        """同目录临时文件 + fsync + 原子改名，保证下游只看到完整文件。"""
        tmp_path = os.path.join(
            self.downstream_dir, f".tmp-publish-{uuid.uuid4().hex}.csv"
        )
        fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, self.live_path)
            self._fsync_dir(self.downstream_dir)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    @staticmethod
    def _fsync_dir(path: str) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    # ---------- 读 ----------
    def current_registry(self):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM registry WHERE id = 1").fetchone()

    def current_rows(self) -> list[dict]:
        with self._connect() as conn:
            reg = conn.execute("SELECT * FROM registry WHERE id = 1").fetchone()
            if reg is None:
                return []
            return self._version_rows(conn, reg["current_version"])

    def _version_rows(self, conn: sqlite3.Connection, version: int) -> list[dict]:
        cur = conn.execute(
            "SELECT sample_no, analyte, concentration FROM results "
            "WHERE version = ? ORDER BY sample_no, analyte",
            (version,),
        )
        return [dict(r) for r in cur.fetchall()]

    def _render_version(self, conn: sqlite3.Connection, version: int) -> str:
        return render_csv(self._version_rows(conn, version))

    def recent_events(self, limit: int = 20):
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM event_log ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()

    def get_session(self, token: str):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM revision_sessions WHERE token = ?", (token,)
            ).fetchone()
            if row is None:
                return None
            data = dict(row)
            data["rows"] = json.loads(data["payload"])
            return data

    def create_session(self, batch_no: str, base_version: int, rows: list[dict]) -> str:
        token = uuid.uuid4().hex
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO revision_sessions(token, batch_no, base_version, payload,"
                " created_at, status) VALUES (?,?,?,?,?,?)",
                (
                    token,
                    batch_no,
                    base_version,
                    json.dumps(rows, ensure_ascii=False),
                    _now(),
                    "pending",
                ),
            )
        return token

    def mark_obsolete(self, token: str, current_version: int) -> None:
        """预览在确认前已被其他发布超越：作废该会话，强制重新预览。"""
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE revision_sessions SET status='obsolete' "
                "WHERE token=? AND status='pending'",
                (token,),
            )
            if cur.rowcount:
                self._log(
                    conn, "conflict",
                    f"会话 {token[:8]} 已过期（当前 v{current_version}），"
                    "拒绝覆盖，需重新预览",
                )

    def cancel_session(self, token: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE revision_sessions SET status='cancelled' "
                "WHERE token=? AND status='pending'",
                (token,),
            )
            if cur.rowcount:
                self._log(conn, "cancel", f"放弃修订会话 {token[:8]}")
                return cur.rowcount > 0
        return False

    # ---------- 预览差异 ----------
    def preview_diff(self, new_rows: list[dict]) -> dict:
        """与当前已发布版本比对，返回 增/改/删/不变 四类差异。"""
        old = {(r["sample_no"], r["analyte"]): r["concentration"]
               for r in self.current_rows()}
        new = {(r["sample_no"], r["analyte"]): r["concentration"] for r in new_rows}
        inserts, updates, deletes, unchanged = [], [], [], []
        for key, val in new.items():
            if key not in old:
                inserts.append({"sample_no": key[0], "analyte": key[1], "new": val})
            elif old[key] != val:
                updates.append(
                    {"sample_no": key[0], "analyte": key[1],
                     "old": old[key], "new": val}
                )
            else:
                unchanged.append({"sample_no": key[0], "analyte": key[1], "value": val})
        for key, val in old.items():
            if key not in new:
                deletes.append({"sample_no": key[0], "analyte": key[1], "old": val})
        inserts.sort(key=lambda d: (d["sample_no"], d["analyte"]))
        updates.sort(key=lambda d: (d["sample_no"], d["analyte"]))
        deletes.sort(key=lambda d: (d["sample_no"], d["analyte"]))
        unchanged.sort(key=lambda d: (d["sample_no"], d["analyte"]))
        return {
            "inserts": inserts, "updates": updates,
            "deletes": deletes, "unchanged": unchanged,
        }

    # ---------- 确认发布（核心事务） ----------
    def publish(self, token: str) -> tuple[int, int]:
        """确认整批替换。返回 (新版本号, 行数)。

        并发控制：BEGIN IMMEDIATE 立即拿写锁，事务内再次核对会话基线版本，
        若期间已有别人发布（current_version > base_version），本会话作废，
        抛出 PublishConflictError，调用方须重新预览。
        """
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            session = conn.execute(
                "SELECT * FROM revision_sessions WHERE token=?", (token,)
            ).fetchone()
            if session is None:
                raise PublishConflictError("修订会话不存在或已失效，请重新上传预览")
            if session["status"] != "pending":
                label = {"published": "已发布", "obsolete": "已过期",
                         "cancelled": "已取消"}.get(session["status"], session["status"])
                raise PublishConflictError(f"该修订会话{label}，请重新上传预览")

            reg = conn.execute("SELECT * FROM registry WHERE id=1").fetchone()
            current_version = reg["current_version"] if reg else 0
            if session["base_version"] != current_version:
                conn.execute(
                    "UPDATE revision_sessions SET status='obsolete' WHERE token=?",
                    (token,),
                )
                self._log(
                    conn, "conflict",
                    f"会话 {token[:8]} 基于 v{session['base_version']} 发起，"
                    f"当前已为 v{current_version}，拒绝覆盖，需重新预览",
                )
                raise PublishConflictError(
                    f"版本已变化：您基于 v{session['base_version']} 预览，"
                    f"当前已是 v{current_version}，请重新预览后再发布"
                )

            rows = json.loads(session["payload"])
            new_version = current_version + 1
            csv_text = render_csv(rows)
            content_hash = hashlib.sha256(csv_text.encode("utf-8")).hexdigest()

            # 先原子替换文件（旧文件在 replace 前始终完整可用）
            self._atomic_write(csv_text)

            # 再落库登记；若此后进程崩溃，重启 recover() 以登记为准重建/核对
            conn.executemany(
                "INSERT INTO results(version, batch_no, sample_no, analyte, concentration)"
                " VALUES (?,?,?,?,?)",
                [
                    (new_version, session["batch_no"], r["sample_no"],
                     r["analyte"], r["concentration"])
                    for r in rows
                ],
            )
            conn.execute(
                "INSERT INTO registry(id, current_version, batch_no, row_count,"
                " published_at, content_hash) VALUES (1,?,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET current_version=excluded.current_version,"
                " batch_no=excluded.batch_no, row_count=excluded.row_count,"
                " published_at=excluded.published_at, content_hash=excluded.content_hash",
                (new_version, session["batch_no"], len(rows), _now(), content_hash),
            )
            conn.execute(
                "UPDATE revision_sessions SET status='published' WHERE token=?",
                (token,),
            )
            self._log(
                conn, "publish",
                f"发布 v{new_version}：批次 {session['batch_no']}，{len(rows)} 行，"
                f"sha256={content_hash[:12]}…",
            )
            return new_version, len(rows)

    # ---------- 页面辅助 ----------
    @staticmethod
    def display_concentration(value: float) -> str:
        return format_concentration(value)

    # ---------- 双文件合并 ----------
    def create_merge_session(
        self,
        batch_no: str,
        base_version: int,
        filename_a: str,
        filename_b: str,
        entries: list[dict],
    ) -> str:
        token = uuid.uuid4().hex
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO merge_sessions(token, batch_no, base_version, filename_a,"
                " filename_b, entries, created_at, status) VALUES (?,?,?,?,?,?,?,?)",
                (
                    token,
                    batch_no,
                    base_version,
                    filename_a,
                    filename_b,
                    json.dumps(entries, ensure_ascii=False),
                    _now(),
                    "pending",
                ),
            )
        return token

    def get_merge_session(self, token: str):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM merge_sessions WHERE token = ?", (token,)
            ).fetchone()
            if row is None:
                return None
            data = dict(row)
            data["entries"] = json.loads(data["entries"])
            return data

    def save_merge_decisions(
        self, token: str, entries: list[dict], *, current_version: int
    ) -> None:
        """保存当前裁决。基线已变（当前版在其他页面发布过）则整份作废。

        失效检测与写入在同一 ``BEGIN IMMEDIATE`` 事务内完成，避免裁决写到
        旧基线上后仍被误认为有效。
        """
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            session = conn.execute(
                "SELECT * FROM merge_sessions WHERE token = ?", (token,)
            ).fetchone()
            if session is None:
                raise MergeSessionError("合并会话不存在或已失效，请重新上传两个文件")
            if session["status"] != "pending":
                raise MergeSessionError(
                    self._merge_status_message(session["status"]),
                    current_version=current_version,
                    base_version=session["base_version"],
                    status=session["status"],
                )
            reg = conn.execute("SELECT * FROM registry WHERE id = 1").fetchone()
            actual_version = reg["current_version"] if reg else 0
            if session["base_version"] != actual_version:
                entries = self._clear_decisions(json.loads(session["entries"]))
                conn.execute(
                    "UPDATE merge_sessions SET status='obsolete', entries=? "
                    "WHERE token=?",
                    (json.dumps(entries, ensure_ascii=False), token),
                )
                self._log(
                    conn, "conflict",
                    f"合并会话 {token[:8]} 基于 v{session['base_version']} 发起，"
                    f"当前已为 v{actual_version}，已保存的裁决整体失效",
                )
                conn.commit()  # 作废标记必须落库，不受后续异常回滚影响
                raise MergeSessionError(
                    f"当前版本已从 v{session['base_version']} 变为 v{actual_version}，"
                    "此前的来源选择与理由全部失效，请重新上传两个文件",
                    current_version=actual_version,
                    base_version=session["base_version"],
                    status="obsolete",
                )
            conn.execute(
                "UPDATE merge_sessions SET entries=? WHERE token=?",
                (json.dumps(entries, ensure_ascii=False), token),
            )

    @staticmethod
    def _merge_status_message(status: str) -> str:
        return {
            "published": "该合并批次已经发布，不能重复发布",
            "obsolete": "该合并预览已被更新的发布超越而作废，裁决已失效，请重新预览",
            "cancelled": "该合并批次已被放弃（取消）",
        }.get(status, "该合并会话已失效")

    def mark_merge_obsolete(self, token: str, current_version: int) -> None:
        """基线已被其他发布超越：作废会话，已保存的裁决整体失效。"""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT entries, base_version FROM merge_sessions WHERE token=?",
                (token,),
            ).fetchone()
            if row is None:
                return
            entries = self._clear_decisions(json.loads(row["entries"]))
            cur = conn.execute(
                "UPDATE merge_sessions SET status='obsolete', entries=? "
                "WHERE token=? AND status='pending'",
                (json.dumps(entries, ensure_ascii=False), token),
            )
            if cur.rowcount:
                self._log(
                    conn, "conflict",
                    f"合并会话 {token[:8]} 已过期（当前 v{current_version}），"
                    "旧裁决全部清空失效，拒绝发布，需重新预览",
                )

    @staticmethod
    def _clear_decisions(entries: list[dict]) -> list[dict]:
        for entry in entries:
            entry["chosen"] = None
            entry["reason"] = ""
        return entries

    def cancel_merge_session(self, token: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE merge_sessions SET status='cancelled' "
                "WHERE token=? AND status='pending'",
                (token,),
            )
            if cur.rowcount:
                self._log(conn, "cancel", f"放弃双文件合并会话 {token[:8]}")
                return True
        return False

    def publish_merge(self, token: str, entries: list[dict] | None = None) -> tuple[int, int]:
        """整批发布两个文件的合并结果，返回 (新版本号, 行数)。

        发布前重新校验：会话仍 pending、基线未变、所有冲突项均已选来源且
        填写理由。``entries`` 是本次确认表单提交的裁决；不传则以会话已保存
        的裁决为准。

        失败语义：文件先原子替换；随后若结果/来源/登记任一步写库失败，事务
        回滚并把正式文件恢复为发布前内容，因此不会留下文件、登记、来源三者
        中的任何部分更新。若进程在改名后、提交前崩溃，重启 ``recover()`` 会
        按数据库登记内容重建正式文件，撤下未登记的版本。
        """
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            session = conn.execute(
                "SELECT * FROM merge_sessions WHERE token = ?", (token,)
            ).fetchone()
            if session is None:
                raise MergeSessionError("合并会话不存在或已失效，请重新上传两个文件")
            if session["status"] != "pending":
                raise MergeSessionError(self._merge_status_message(session["status"]))

            reg = conn.execute("SELECT * FROM registry WHERE id = 1").fetchone()
            current_version = reg["current_version"] if reg else 0
            if session["base_version"] != current_version:
                entries = self._clear_decisions(json.loads(session["entries"]))
                conn.execute(
                    "UPDATE merge_sessions SET status='obsolete', entries=? "
                    "WHERE token=?",
                    (json.dumps(entries, ensure_ascii=False), token),
                )
                self._log(
                    conn, "conflict",
                    f"合并会话 {token[:8]} 基于 v{session['base_version']} 发起，"
                    f"当前已为 v{current_version}，拒绝覆盖，需重新预览",
                )
                conn.commit()  # 作废标记必须落库，不受后续异常回滚影响
                raise MergeSessionError(
                    f"版本已变化：您基于 v{session['base_version']} 预览，"
                    f"当前已是 v{current_version}，请重新上传两个文件",
                    current_version=current_version,
                    base_version=session["base_version"],
                    status="obsolete",
                )

            if entries is None:
                entries = json.loads(session["entries"])
            else:
                entries = json.loads(json.dumps(entries, ensure_ascii=False))
            incomplete = [
                e for e in entries
                if e["status"] == "conflict" and not entry_is_complete(e)
            ]
            if incomplete:
                raise MergeSessionError(
                    f"仍有 {len(incomplete)} 个冲突项未选择来源或未填写理由，不能发布"
                )

            rows, sources = build_rows_and_sources(entries)
            concentration_by_key = {
                (r["sample_no"], r["analyte"]): r["concentration"] for r in rows
            }
            new_version = current_version + 1
            csv_text = render_csv(rows)
            payload = csv_text.encode("utf-8")
            content_hash = hashlib.sha256(payload).hexdigest()

            # 先保存一份旧正式文件内容；发布失败时用它原样恢复。
            old_payload = None
            had_live = os.path.exists(self.live_path)
            if had_live:
                with open(self.live_path, "rb") as fh:
                    old_payload = fh.read()

            # 1) 原子换文件（下游只会看到完整旧版或完整新版）
            # 2) 在同一事务内写结果、逐行来源、登记与会话状态
            # 3) 任一步失败 → 回滚数据库并把正式文件恢复原样，不留部分更新
            try:
                self._atomic_write_bytes(payload)
            except BaseException:
                conn.rollback()
                raise
            try:
                conn.executemany(
                    "INSERT INTO results(version, batch_no, sample_no, analyte,"
                    " concentration) VALUES (?,?,?,?,?)",
                    [
                        (new_version, session["batch_no"], r["sample_no"],
                         r["analyte"], r["concentration"])
                        for r in rows
                    ],
                )
                conn.executemany(
                    "INSERT INTO merge_sources(version, batch_no, filename_a,"
                    " filename_b, sample_no, analyte, concentration, source, conflict,"
                    " value_a, value_b, reason) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    [
                        (
                            new_version, session["batch_no"], session["filename_a"],
                            session["filename_b"], s["sample_no"], s["analyte"],
                            concentration_by_key[(s["sample_no"], s["analyte"])],
                            s["source"], s["conflict"], s["value_a"], s["value_b"],
                            s["reason"],
                        )
                        for s in sources
                    ],
                )
                conn.execute(
                    "INSERT INTO registry(id, current_version, batch_no, row_count,"
                    " published_at, content_hash) VALUES (1,?,?,?,?,?) "
                    "ON CONFLICT(id) DO UPDATE SET"
                    " current_version=excluded.current_version,"
                    " batch_no=excluded.batch_no, row_count=excluded.row_count,"
                    " published_at=excluded.published_at,"
                    " content_hash=excluded.content_hash",
                    (new_version, session["batch_no"], len(rows), _now(),
                     content_hash),
                )
                # 提交本次确认的裁决（含发布时刚填写的冲突理由）
                conn.execute(
                    "UPDATE merge_sessions SET entries=? WHERE token=?",
                    (json.dumps(entries, ensure_ascii=False), token),
                )
                conn.execute(
                    "UPDATE merge_sessions SET status='published' WHERE token=?",
                    (token,),
                )
                self._log(
                    conn, "merge_publish",
                    f"合并发布 v{new_version}：批次 {session['batch_no']}，"
                    f"A={session['filename_a']} / B={session['filename_b']}，"
                    f"{len(rows)} 行，冲突已裁决 "
                    f"{sum(1 for e in entries if e['status'] == 'conflict')} 项，"
                    f"sha256={content_hash[:12]}…",
                )
                conn.commit()
            except BaseException:
                conn.rollback()
                self._restore_live_file(old_payload, had_live)
                raise
            return new_version, len(rows)

    def _restore_live_file(self, old_payload: bytes | None, had_live: bool) -> None:
        """发布失败时把正式文件恢复为发布前内容（尽力而为，不掩盖原异常）。"""
        try:
            if had_live and old_payload is not None:
                self._atomic_write_bytes(old_payload)
            elif not had_live and os.path.exists(self.live_path):
                os.unlink(self.live_path)
                self._fsync_dir(self.downstream_dir)
        except Exception:
            # 恢复失败时保留原文件不动，重启 recover() 会以数据库登记为准修复。
            pass

    def merge_sources(self, version: int):
        with self._connect() as conn:
            return [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM merge_sources WHERE version=? "
                    "ORDER BY sample_no, analyte",
                    (version,),
                ).fetchall()
            ]

    def merge_versions(self) -> list[dict]:
        """列出带逐行来源记录的历史合并版本（新→旧）。"""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT version, batch_no, filename_a, filename_b,"
                " COUNT(*) AS row_count, SUM(conflict) AS conflict_count"
                " FROM merge_sources"
                " GROUP BY version, batch_no, filename_a, filename_b"
                " ORDER BY version DESC"
            ).fetchall()
            return [
                {
                    **dict(row),
                    "published_at": self._merge_published_at(conn, row["version"]),
                }
                for row in rows
            ]

    def _merge_published_at(self, conn: sqlite3.Connection, version: int) -> str | None:
        row = conn.execute(
            "SELECT ts FROM event_log WHERE kind='merge_publish' AND detail LIKE ? "
            "ORDER BY id LIMIT 1",
            (f"%合并发布 v{version}：%",),
        ).fetchone()
        return row["ts"] if row else None

    def merge_version_rows(self, version: int):
        """按版本读取已发布合并结果，供页面核对。"""
        with self._connect() as conn:
            reg = conn.execute(
                "SELECT * FROM registry WHERE current_version=?", (version,)
            ).fetchone()
            sources = conn.execute(
                "SELECT * FROM merge_sources WHERE version=?"
                " ORDER BY sample_no, analyte",
                (version,),
            ).fetchall()
            # 结果行只保留在 results 表里；旧版本未合并的改用 sources 自带浓度，
            # 但合并版本必然同时有 results 行，此处以 results 为准。
            result_rows = conn.execute(
                "SELECT sample_no, analyte, concentration FROM results"
                " WHERE version=? ORDER BY sample_no, analyte",
                (version,),
            ).fetchall()
            return {
                "rows": [dict(r) for r in result_rows],
                "sources": {
                    (r["sample_no"], r["analyte"]): dict(r) for r in sources
                },
                "registry": dict(reg) if reg else None,
                "batch_no": sources[0]["batch_no"] if sources else None,
                "filename_a": sources[0]["filename_a"] if sources else None,
                "filename_b": sources[0]["filename_b"] if sources else None,
            }
