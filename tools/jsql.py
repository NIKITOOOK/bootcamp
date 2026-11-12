"""Запуск SQL в ядре JupyterHub (Trino) с локальной машины.

  python tools/jsql.py ping
  python tools/jsql.py run sql/mart/case_05.sql [--show 20] [--max-rows 200000]
  python tools/jsql.py run-all sql/stage sql/mart
  python tools/jsql.py exec "print(1)"
  python tools/jsql.py tables [raw|stage|mart]
  python tools/jsql.py describe raw.pg__matches

Настройки в .env (рядом с репозиторием или в текущей папке):
  JUPYTER_URL=http://153.51.166.221:8888/user/<login>/
  JUPYTER_TOKEN=...
Необязательно: TRINO_HOST=trino, TRINO_PORT=8080, TRINO_USER=jupyter, TRINO_CATALOG=lakehouse

Проверки пишутся в шапке .sql файла:
  -- check: rows > 0
  -- check: unique(match_id, hero_id)
  -- check: not_null(match_id, win_rate)
  -- check: range(win_rate, 0, 1)
  -- check: accepted(side, radiant, dire)
"""

import argparse
import csv
import datetime as dt
import json
import os
import re
import sys
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse

import requests
import websocket

ROOT = Path(__file__).resolve().parent.parent
RUNS = ROOT / "runs"
KERNEL_FILE = RUNS / ".kernel_id"
MARK_BEGIN = "<<<JSQL>>>"
MARK_END = "<<<END>>>"

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")


def load_env():
    for path in (Path.cwd() / ".env", ROOT / ".env", ROOT.parent / ".env"):
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
            break
    url = os.environ.get("JUPYTER_URL", "").rstrip("/")
    token = os.environ.get("JUPYTER_TOKEN", "")
    if not url or not token:
        sys.exit("Нет JUPYTER_URL или JUPYTER_TOKEN в .env")
    if "/user/" not in url:
        login = os.environ.get("JUPYTER_USER")
        if not login:
            sys.exit("JUPYTER_URL должен быть вида http://host:8888/user/<login>/ или задайте JUPYTER_USER")
        url = f"{url}/user/{login}"
    return url, token


class Kernel:
    def __init__(self, url, token):
        self.url = url
        self.headers = {"Authorization": f"token {token}"}
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.kernel_id = None
        self.ws = None

    def api(self, method, path, **kw):
        try:
            resp = self.session.request(method, f"{self.url}/api/{path}", timeout=30, **kw)
        except requests.ConnectionError:
            sys.exit(f"Нет связи с {urlparse(self.url).netloc}: проверьте сеть и что JupyterHub доступен")
        if resp.status_code in (401, 403):
            sys.exit(f"{resp.status_code}: токен не подходит или истёк")
        if resp.status_code in (302, 404, 503) and path == "status":
            sys.exit("Сервер JupyterHub не запущен: откройте JupyterHub в браузере и нажмите Start My Server")
        resp.raise_for_status()
        return resp.json() if resp.content else None

    def start(self):
        self.api("GET", "status")
        RUNS.mkdir(exist_ok=True)
        alive = {k["id"] for k in self.api("GET", "kernels")}
        if KERNEL_FILE.exists() and KERNEL_FILE.read_text().strip() in alive:
            self.kernel_id = KERNEL_FILE.read_text().strip()
        else:
            self.kernel_id = self.api("POST", "kernels", json={"name": "python3"})["id"]
            KERNEL_FILE.write_text(self.kernel_id)
        parsed = urlparse(self.url)
        scheme = "wss" if parsed.scheme == "https" else "ws"
        ws_url = f"{scheme}://{parsed.netloc}{parsed.path}/api/kernels/{self.kernel_id}/channels"
        self.ws = websocket.create_connection(
            ws_url, header=[f"{k}: {v}" for k, v in self.headers.items()], timeout=600
        )

    def execute(self, code, timeout=900):
        msg_id = uuid.uuid4().hex
        self.ws.send(json.dumps({
            "header": {"msg_id": msg_id, "username": "jsql", "session": uuid.uuid4().hex,
                       "msg_type": "execute_request", "version": "5.3"},
            "parent_header": {}, "metadata": {}, "channel": "shell",
            "content": {"code": code, "silent": False, "store_history": False,
                        "user_expressions": {}, "allow_stdin": False, "stop_on_error": True},
        }))
        out, err = [], None
        deadline = time.time() + timeout
        while time.time() < deadline:
            msg = json.loads(self.ws.recv())
            if msg.get("parent_header", {}).get("msg_id") != msg_id:
                continue
            kind, content = msg["msg_type"], msg["content"]
            if kind == "stream":
                out.append(content["text"])
            elif kind in ("execute_result", "display_data"):
                out.append(content["data"].get("text/plain", "") + "\n")
            elif kind == "error":
                err = "\n".join(re.sub(r"\x1b\[[0-9;]*m", "", line) for line in content["traceback"])
            elif kind == "status" and content["execution_state"] == "idle":
                return "".join(out), err
        raise TimeoutError(f"нет ответа от ядра за {timeout} с")

    def close(self):
        if self.ws:
            self.ws.close()


KERNEL_SQL = r'''
import json as _j, math as _m, datetime as _d, decimal as _dc, trino as _t
def _jsql(sql, checks, max_rows, conn):
    def _v(x):
        if isinstance(x, (_d.date, _d.datetime, _d.time)): return x.isoformat()
        if isinstance(x, _dc.Decimal): return float(x)
        if isinstance(x, float) and (_m.isnan(x) or _m.isinf(x)): return None
        if isinstance(x, (bytes, bytearray)): return x.hex()
        if isinstance(x, (list, dict, tuple)): return _j.loads(_j.dumps(x, default=str))
        return x
    c = _t.dbapi.connect(**conn)
    res = {"ok": True}
    try:
        cur = c.cursor()
        t0 = _d.datetime.now()
        statements = [s for s in sql.split(";\n") if s.strip()]
        for s in statements[:-1]:
            cur.execute(s); cur.fetchall()
        cur.execute(statements[-1])
        rows = cur.fetchmany(max_rows + 1)
        res["truncated"] = len(rows) > max_rows
        rows = rows[:max_rows]
        res["seconds"] = round((_d.datetime.now() - t0).total_seconds(), 2)
        cols = [d[0] for d in (cur.description or [])]
        res["columns"] = cols
        res["rows"] = [[_v(x) for x in r] for r in rows]
        idx = {n: i for i, n in enumerate(cols)}
        out = []
        for chk in checks:
            name, ok, info = chk, True, ""
            try:
                m = __import__("re").match(r"(\w+)\s*(?:\((.*)\))?\s*(.*)", chk)
                fn, args, tail = m.group(1), [a.strip() for a in (m.group(2) or "").split(",") if a.strip()], m.group(3)
                if fn == "rows":
                    op, num = tail.split()[0], int(tail.split()[1])
                    ok = eval(f"{len(rows)} {op} {num}"); info = f"rows={len(rows)}"
                elif fn == "unique":
                    keys = [tuple(r[idx[a]] for a in args) for r in rows]
                    dup = len(keys) - len(set(keys)); ok = dup == 0; info = f"duplicates={dup}"
                elif fn == "not_null":
                    bad = {a: sum(r[idx[a]] is None for r in rows) for a in args}
                    ok = not any(bad.values()); info = ", ".join(f"{k}={v}" for k, v in bad.items() if v) or "0"
                elif fn == "range":
                    col, lo, hi = args[0], float(args[1]), float(args[2])
                    vals = [r[idx[col]] for r in rows if r[idx[col]] is not None]
                    bad = sum(not (lo <= float(v) <= hi) for v in vals); ok = bad == 0
                    info = f"out_of_range={bad}, min={min(vals) if vals else None}, max={max(vals) if vals else None}"
                elif fn == "accepted":
                    col, allowed = args[0], set(args[1:])
                    bad = sorted({str(r[idx[col]]) for r in rows} - allowed)[:10]; ok = not bad; info = f"unexpected={bad}"
                else:
                    ok, info = False, "неизвестная проверка"
            except Exception as e:
                ok, info = False, f"{type(e).__name__}: {e}"
            out.append({"check": name, "ok": bool(ok), "info": info})
        res["checks"] = out
        if res["truncated"]:
            out.append({"check": f"rows <= {max_rows}", "ok": False, "info": "результат обрезан, проверки неполные"})
    except Exception as e:
        res = {"ok": False, "error": f"{type(e).__name__}: {e}"}
    finally:
        c.close()
    print("<<<JSQL>>>" + _j.dumps(res, ensure_ascii=False, default=str) + "<<<END>>>")
'''


def trino_conn():
    return {
        "host": os.environ.get("TRINO_HOST", "trino"),
        "port": int(os.environ.get("TRINO_PORT", "8080")),
        "user": os.environ.get("TRINO_USER", "jupyter"),
        "catalog": os.environ.get("TRINO_CATALOG", "lakehouse"),
    }


def parse_checks(sql):
    return [m.group(1).strip() for m in re.finditer(r"^\s*--\s*check:\s*(.+)$", sql, re.M)]


def strip_sql(sql):
    body = "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))
    return body.strip().rstrip(";").strip()


def run_sql(kernel, sql, checks, max_rows):
    code = KERNEL_SQL + f"\n_jsql({strip_sql(sql)!r}, {checks!r}, {max_rows}, {trino_conn()!r})\n"
    out, err = kernel.execute(code)
    if err:
        return {"ok": False, "error": err}
    m = re.search(re.escape(MARK_BEGIN) + r"(.*)" + re.escape(MARK_END), out, re.S)
    if not m:
        return {"ok": False, "error": "нет ответа от ядра:\n" + out[-2000:]}
    return json.loads(m.group(1))


def cell(v, width=40):
    s = "NULL" if v is None else str(v)
    s = s.replace("|", "\\|").replace("\n", " ")
    return s if len(s) <= width else s[: width - 1] + "…"


def render(name, res, show):
    lines = [f"## {name}"]
    if not res["ok"]:
        lines += ["", "**ОШИБКА**", "```", res["error"].strip()[-3000:], "```"]
        return "\n".join(lines)
    cols, rows = res["columns"], res["rows"]
    lines.append(f"rows={len(rows)}{' (обрезано)' if res['truncated'] else ''}, {res['seconds']} c")
    for c in res["checks"]:
        lines.append(f"- [{'OK' if c['ok'] else 'FAIL'}] {c['check']}: {c['info']}")
    if cols:
        lines += ["", "| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
        lines += ["| " + " | ".join(cell(v) for v in r) + " |" for r in rows[:show]]
    return "\n".join(lines)


def save(name, res, report):
    RUNS.mkdir(exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    if res.get("ok") and res.get("columns"):
        with open(RUNS / f"{name}.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(res["columns"])
            w.writerows(res["rows"])
    status = "ERROR" if not res.get("ok") else ("FAIL" if any(not c["ok"] for c in res["checks"]) else "OK")
    with open(RUNS / "log.md", "a", encoding="utf-8") as f:
        f.write(f"\n<!-- {stamp} {status} -->\n{report}\n")
    return status


def cmd_run(kernel, files, show, max_rows):
    statuses = []
    for path in files:
        sql = Path(path).read_text(encoding="utf-8")
        res = run_sql(kernel, sql, parse_checks(sql), max_rows)
        report = render(Path(path).as_posix(), res, show)
        statuses.append((path, save(Path(path).stem, res, report)))
        print(report + "\n")
    if len(statuses) > 1:
        print("## Итог")
        for path, st in statuses:
            print(f"- {st} {path}")
    return 0 if all(st == "OK" for _, st in statuses) else 1


def main():
    p = argparse.ArgumentParser(description="SQL в Trino через ядро JupyterHub")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("ping")
    r = sub.add_parser("run")
    r.add_argument("files", nargs="+")
    ra = sub.add_parser("run-all")
    ra.add_argument("dirs", nargs="+")
    for x in (r, ra):
        x.add_argument("--show", type=int, default=20)
        x.add_argument("--max-rows", type=int, default=200000)
    q = sub.add_parser("query")
    q.add_argument("sql")
    q.add_argument("--show", type=int, default=50)
    e = sub.add_parser("exec")
    e.add_argument("code")
    t = sub.add_parser("tables")
    t.add_argument("schema", nargs="?")
    d = sub.add_parser("describe")
    d.add_argument("table")
    sub.add_parser("restart")
    a = p.parse_args()

    url, token = load_env()
    k = Kernel(url, token)
    try:
        if a.cmd == "restart":
            if KERNEL_FILE.exists():
                k.api("DELETE", f"kernels/{KERNEL_FILE.read_text().strip()}")
                KERNEL_FILE.unlink()
            print("ядро удалено")
            return 0
        k.start()
        if a.cmd == "ping":
            out, err = k.execute("import trino, sys; print(sys.version.split()[0], trino.__version__)")
            print(err or f"kernel {k.kernel_id}: python {out.strip()}")
            res = run_sql(k, "select count(*) as tables from lakehouse.information_schema.tables "
                             "where table_schema in ('raw','stage','mart')", [], 10)
            print(render("trino", res, 5))
            return 0 if res["ok"] else 1
        if a.cmd == "run":
            return cmd_run(k, a.files, a.show, a.max_rows)
        if a.cmd == "run-all":
            files = sorted(str(f) for d in a.dirs for f in Path(d).glob("*.sql"))
            return cmd_run(k, files, a.show, a.max_rows)
        if a.cmd == "query":
            res = run_sql(k, a.sql, [], 10000)
            print(render("query", res, a.show))
            return 0 if res["ok"] else 1
        if a.cmd == "exec":
            out, err = k.execute(a.code)
            print(out)
            if err:
                print(err, file=sys.stderr)
                return 1
            return 0
        if a.cmd == "tables":
            where = f"= '{a.schema}'" if a.schema else "in ('raw','stage','mart')"
            res = run_sql(k, f"select table_schema, table_name from lakehouse.information_schema.tables "
                             f"where table_schema {where} order by 1, 2", [], 5000)
            print(render("tables", res, 5000))
        if a.cmd == "describe":
            schema, table = a.table.split(".")
            res = run_sql(k, f"select column_name, data_type from lakehouse.information_schema.columns "
                             f"where table_schema = '{schema}' and table_name = '{table}' "
                             f"order by ordinal_position", [], 5000)
            print(render(a.table, res, 5000))
            if res["ok"]:
                res = run_sql(k, f"select * from lakehouse.{a.table} limit 5", [], 5)
                print("\n" + render("sample", res, 5))
        return 0
    finally:
        k.close()


if __name__ == "__main__":
    sys.exit(main())
