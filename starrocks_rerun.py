"""StarRocks 数据重跑后端。

提供测试 / 生产环境 ODS 表搜索，以及从同名 ``ori`` 表重新灌数的异步执行能力。
数据重跑只执行 ``INSERT OVERWRITE``，不创建、删除或修改表结构。
"""

from __future__ import annotations

import importlib
import json
import os
import re
import sys
import threading
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any


PIPELINE_ROOT = Path(r"D:\code\python\pythonProj\get_ddl").resolve()
STARROCKS_SCHEMA = "ods"
SOURCE_SCHEMA = "ori"
TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,127}$")
QUERY_SPLIT_PATTERN = re.compile(r"[,，\r\n]+")
MAX_QUERY_CHARS = 500
MAX_SEARCH_LIMIT = 500
MAX_TABLES_PER_RUN = 200
MAX_LOG_CHARS = 2 * 1024 * 1024
MAX_HISTORY_RECORDS = 100
HISTORY_ROOT = Path(__file__).resolve().parent / ".runtime" / "starrocks-rerun-history"
OPERATION_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")


ENVIRONMENTS: dict[str, dict[str, str]] = {
    "test": {
        "label": "测试环境",
        "module": "pythonProj.get_ddl.util.getSRConnectTest",
        "function": "getSRConnectTest",
    },
    "prod": {
        "label": "生产环境",
        "module": "pythonProj.get_ddl.util.getSRConnectPro",
        "function": "getSRConnectPro",
    },
}

OPERATIONS: dict[str, dict[str, Any]] = {}
OPERATIONS_LOCK = threading.RLock()
ACTIVE_OPERATION_ID: str | None = None


class StarRocksRerunError(RuntimeError):
    """StarRocks 数据重跑失败。"""



def list_environments() -> dict[str, list[dict[str, Any]]]:
    """返回前端可选择的环境，不暴露连接信息。"""
    return {
        "environments": [
            {
                "id": environment_id,
                "label": config["label"],
                "production": environment_id == "prod",
            }
            for environment_id, config in ENVIRONMENTS.items()
        ]
    }


def _require_environment(environment: Any) -> str:
    if not isinstance(environment, str) or environment not in ENVIRONMENTS:
        raise ValueError("environment 必须是 test 或 prod")
    return environment


def _require_table_name(table: Any) -> str:
    if not isinstance(table, str) or not TABLE_NAME_PATTERN.fullmatch(table):
        raise ValueError(f"StarRocks 表名不合法：{table!r}")
    return table


def _quote_identifier(identifier: str) -> str:
    """仅对白名单校验后的标识符加反引号。"""
    _require_table_name(identifier)
    return f"`{identifier}`"


def _open_connection(environment: str) -> tuple[Any, Any]:
    """复用正式项目现有连接模块，账号密码不进入本项目接口。"""
    config = ENVIRONMENTS[environment]
    import_root = str(PIPELINE_ROOT.parent.parent)
    path_added = import_root not in sys.path
    if path_added:
        sys.path.insert(0, import_root)
    try:
        module = importlib.import_module(config["module"])
        factory = getattr(module, config["function"])
        result = factory()
    finally:
        if path_added:
            sys.path.remove(import_root)

    if not isinstance(result, tuple) or len(result) != 2:
        raise StarRocksRerunError("StarRocks 连接函数返回格式不正确")
    cursor, connection = result
    return cursor, connection


def _close_connection(cursor: Any, connection: Any) -> None:
    try:
        if cursor is not None:
            cursor.close()
    finally:
        if connection is not None and connection is not cursor:
            connection.close()


def _split_query(query: str) -> list[str]:
    terms: list[str] = []
    for raw_term in QUERY_SPLIT_PATTERN.split(query):
        term = raw_term.strip().casefold()
        if term and term not in terms:
            terms.append(term)
    return terms


def search_tables(environment: Any, query: Any, limit: Any = 300) -> dict[str, Any]:
    """查询选定环境中的真实 ODS 表，并标记同名 ORI 表是否存在。"""
    environment_id = _require_environment(environment)
    if not isinstance(query, str):
        raise ValueError("query 必须是字符串")
    if len(query) > MAX_QUERY_CHARS:
        raise ValueError(f"query 不能超过 {MAX_QUERY_CHARS} 个字符")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_SEARCH_LIMIT:
        raise ValueError(f"limit 必须是 1 到 {MAX_SEARCH_LIMIT} 的整数")

    cursor = None
    connection = None
    try:
        cursor, connection = _open_connection(environment_id)
        cursor.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema IN (%s, %s) AND table_type = %s",
            (STARROCKS_SCHEMA, SOURCE_SCHEMA, "BASE TABLE"),
        )
        existence: dict[str, set[str]] = {}
        display_names: dict[str, str] = {}
        for row in cursor.fetchall():
            if not row or len(row) < 2:
                continue
            schema = str(row[0] or "").casefold()
            table = str(row[1] or "").strip()
            if schema not in {STARROCKS_SCHEMA, SOURCE_SCHEMA} or not TABLE_NAME_PATTERN.fullmatch(table):
                continue
            key = table.casefold()
            existence.setdefault(key, set()).add(schema)
            if schema == STARROCKS_SCHEMA or key not in display_names:
                display_names[key] = table
    finally:
        _close_connection(cursor, connection)

    terms = _split_query(query)
    items: list[dict[str, Any]] = []
    for key, schemas in existence.items():
        if STARROCKS_SCHEMA not in schemas:
            continue
        if terms and not any(term in key for term in terms):
            continue
        items.append(
            {
                "table": display_names[key],
                "odsExists": True,
                "oriExists": SOURCE_SCHEMA in schemas,
                "exact": any(term == key for term in terms),
            }
        )
    items.sort(key=lambda item: (not item["exact"], item["table"].casefold()))
    return {
        "environment": environment_id,
        "items": items[:limit],
        "total": len(items),
    }


def _append_log(operation_id: str, message: str) -> None:
    with OPERATIONS_LOCK:
        operation = OPERATIONS.get(operation_id)
        if operation is None:
            return
        operation["log"] = (operation["log"] + message)[-MAX_LOG_CHARS:]


def _history_path(operation_id: str) -> Path:
    """返回校验后的历史记录路径，避免路径穿越。"""
    if not OPERATION_ID_PATTERN.fullmatch(operation_id):
        raise ValueError("数据重跑记录 ID 不合法")
    return HISTORY_ROOT / f"{operation_id}.json"


def _operation_snapshot(operation: dict[str, Any]) -> dict[str, Any]:
    """复制可变数组，供接口返回和持久化安全使用。"""
    return {
        **operation,
        "environments": list(operation.get("environments") or []),
        "tables": [dict(item) for item in operation.get("tables") or []],
        "results": [dict(item) for item in operation.get("results") or []],
    }


def _persist_operation(operation: dict[str, Any]) -> None:
    """原子保存完成记录，并只保留最近 100 次。"""
    HISTORY_ROOT.mkdir(parents=True, exist_ok=True)
    path = _history_path(str(operation["operationId"]))
    temporary = path.with_suffix(f".{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(operation, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(temporary, path)
    history_files = sorted(
        HISTORY_ROOT.glob("*.json"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    for expired in history_files[MAX_HISTORY_RECORDS:]:
        try:
            expired.unlink()
        except OSError:
            pass


def _history_summary(operation: dict[str, Any]) -> dict[str, Any]:
    tables = operation.get("tables") or []
    results = operation.get("results") or []
    return {
        "operationId": operation.get("operationId"),
        "environmentLabel": operation.get("environmentLabel"),
        "status": operation.get("status"),
        "startedAt": operation.get("startedAt"),
        "finishedAt": operation.get("finishedAt"),
        "total": operation.get("total", len(tables)),
        "completed": operation.get("completed", 0),

        "failedCount": sum(1 for item in results if item.get("status") == "failed"),
        "tables": [
            {
                "environment": item.get("environment"),
                "table": item.get("table"),
            }
            for item in tables
        ],
    }


def list_history() -> dict[str, list[dict[str, Any]]]:
    """返回最近的数据重跑记录摘要，包含当前内存中的活动操作。"""
    records_by_id: dict[str, dict[str, Any]] = {}
    if HISTORY_ROOT.exists():
        for path in HISTORY_ROOT.glob("*.json"):
            try:
                operation = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(operation, dict) and OPERATION_ID_PATTERN.fullmatch(
                    str(operation.get("operationId") or "")
                ):
                    records_by_id[str(operation["operationId"])] = _history_summary(operation)
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                continue
    with OPERATIONS_LOCK:
        for operation in OPERATIONS.values():
            records_by_id[str(operation["operationId"])] = _history_summary(operation)
    records = sorted(
        records_by_id.values(),
        key=lambda item: str(item.get("startedAt") or ""),
        reverse=True,
    )
    return {"records": records[:MAX_HISTORY_RECORDS]}


def get_history(operation_id: Any) -> dict[str, Any] | None:
    """读取内存中或磁盘上的完整重跑记录。"""
    if not isinstance(operation_id, str) or not OPERATION_ID_PATTERN.fullmatch(operation_id):
        return None
    operation = get_operation(operation_id)
    if operation is not None:
        return operation
    path = _history_path(operation_id)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _set_operation_result(
    operation_id: str,
    *,
    status: str,
    error: str | None = None,
) -> None:
    global ACTIVE_OPERATION_ID
    snapshot = None
    with OPERATIONS_LOCK:
        operation = OPERATIONS.get(operation_id)
        if operation is None:
            return
        operation["status"] = status
        operation["error"] = error
        operation["finishedAt"] = datetime.now().isoformat(timespec="seconds")
        snapshot = _operation_snapshot(operation)
        if ACTIVE_OPERATION_ID == operation_id:
            ACTIVE_OPERATION_ID = None
    if snapshot is not None:
        try:
            _persist_operation(snapshot)
        except OSError as exc:
            _append_log(operation_id, f"[历史记录] 保存失败：{exc}\n")


def _table_exists(cursor: Any, schema: str, table: str) -> bool:
    cursor.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s AND table_type = %s",
        (schema, table, "BASE TABLE"),
    )
    row = cursor.fetchone()
    return bool(row and int(row[0]) > 0)



def _execute_table(
    operation_id: str,
    cursor: Any,
    connection: Any,
    table: str,
) -> dict[str, Any]:
    target = f"{STARROCKS_SCHEMA}.{_quote_identifier(table)}"
    source = f"{SOURCE_SCHEMA}.{_quote_identifier(table)}"
    if not _table_exists(cursor, STARROCKS_SCHEMA, table):
        raise StarRocksRerunError(f"目标表 ods.{table} 不存在")
    if not _table_exists(cursor, SOURCE_SCHEMA, table):
        raise StarRocksRerunError(f"来源表 ori.{table} 不存在，无法重新灌数")

    _append_log(operation_id, f"[{table}] INSERT OVERWRITE 从 ori 重新灌数\n")
    cursor.execute(f"INSERT OVERWRITE {target} SELECT * FROM {source}")
    connection.commit()
    return {"table": table, "status": "succeeded"}


def _run_operation(
    operation_id: str,
    environments: list[str],
    tables: list[dict[str, Any]],
) -> None:
    failed_count = 0
    completed = 0
    try:
        labels = "、".join(ENVIRONMENTS[item]["label"] for item in environments)
        _append_log(operation_id, f"数据重跑开始：{labels}，共 {len(tables)} 张表。\n")
        for environment in environments:
            environment_tables = [
                item for item in tables if item["environment"] == environment
            ]
            if not environment_tables:
                continue
            cursor = None
            connection = None
            environment_label = ENVIRONMENTS[environment]["label"]
            _append_log(operation_id, f"\n===== {environment_label} =====\n")
            try:
                cursor, connection = _open_connection(environment)
                for item in environment_tables:
                    completed += 1
                    table = item["table"]
                    _append_log(
                        operation_id,
                        f"\n[{completed}/{len(tables)}] {environment_label} / {table}\n",
                    )
                    try:
                        result = _execute_table(
                            operation_id, cursor, connection, table
                        )
                        result["environment"] = environment
                        result["environmentLabel"] = environment_label
                        _append_log(operation_id, f"[{table}] 执行成功\n")
                    except Exception as exc:
                        failed_count += 1
                        try:
                            connection.rollback()
                        except Exception:
                            pass
                        result = {
                            "environment": environment,
                            "environmentLabel": environment_label,
                            "table": table,
                            "status": "failed",
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                        _append_log(
                            operation_id,
                            f"[{table}] 执行失败：{result['error']}\n",
                        )
                    with OPERATIONS_LOCK:
                        operation = OPERATIONS.get(operation_id)
                        if operation is not None:
                            operation["results"].append(result)
                            operation["completed"] = completed
            finally:
                _close_connection(cursor, connection)

        if failed_count:
            error = f"{failed_count} 张表执行失败，请查看逐表日志"
            _append_log(operation_id, f"\n数据重跑结束：{error}。\n")
            _set_operation_result(operation_id, status="failed", error=error)
        else:
            _append_log(operation_id, "\n数据重跑全部完成。\n")
            _set_operation_result(operation_id, status="succeeded")
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        _append_log(operation_id, f"\n[后端异常] {error}\n{traceback.format_exc()}")
        _set_operation_result(operation_id, status="failed", error=error)


def start_rerun(payload: dict[str, Any]) -> dict[str, Any]:
    """校验请求并启动异步重跑。"""
    global ACTIVE_OPERATION_ID
    raw_environments = payload.get("environments")
    if not isinstance(raw_environments, list) or not raw_environments:
        raise ValueError("environments 必须是非空数组")
    environments: list[str] = []
    for raw_environment in raw_environments:
        environment = _require_environment(raw_environment)
        if environment not in environments:
            environments.append(environment)
    if payload.get("confirmed") is not True:
        raise ValueError("执行数据重跑必须显式确认")
    if "prod" in environments and payload.get("productionConfirmed") is not True:
        raise ValueError("生产环境数据重跑必须单独确认")

    raw_tables = payload.get("tables")
    if not isinstance(raw_tables, list) or not raw_tables:
        raise ValueError("tables 必须是非空数组")
    if len(raw_tables) > MAX_TABLES_PER_RUN:
        raise ValueError(f"单次最多重跑 {MAX_TABLES_PER_RUN} 张表")

    tables: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw_tables:
        if not isinstance(item, dict):
            raise ValueError("tables 中的每一项必须是对象")
        environment = _require_environment(item.get("environment"))
        if environment not in environments:
            raise ValueError("表所属环境不在 environments 中")
        table = _require_table_name(item.get("table"))

        key = f"{environment}\u0001{table.casefold()}"
        if key in seen:
            raise ValueError(f"表重复：{ENVIRONMENTS[environment]['label']} / {table}")
        seen.add(key)
        tables.append({"environment": environment, "table": table})

    operation_id = uuid.uuid4().hex
    with OPERATIONS_LOCK:
        if ACTIVE_OPERATION_ID is not None:
            active = OPERATIONS.get(ACTIVE_OPERATION_ID)
            if active is not None and active.get("status") == "running":
                raise StarRocksRerunError("已有数据重跑正在执行，请等待完成")
        ACTIVE_OPERATION_ID = operation_id
        OPERATIONS[operation_id] = {
            "operationId": operation_id,
            "environments": list(environments),
            "environmentLabel": "、".join(
                ENVIRONMENTS[item]["label"] for item in environments
            ),
            "status": "running",
            "log": "",
            "error": None,
            "startedAt": datetime.now().isoformat(timespec="seconds"),
            "finishedAt": None,
            "total": len(tables),
            "completed": 0,
            "tables": [dict(item) for item in tables],
            "results": [],
        }

    thread = threading.Thread(
        target=_run_operation,
        args=(operation_id, environments, tables),
        name=f"starrocks-rerun-{operation_id[:8]}",
        daemon=True,
    )
    try:
        thread.start()
    except Exception:
        with OPERATIONS_LOCK:
            OPERATIONS.pop(operation_id, None)
            if ACTIVE_OPERATION_ID == operation_id:
                ACTIVE_OPERATION_ID = None
        raise
    return {
        "ok": True,
        "operationId": operation_id,
        "status": "running",
        "environments": environments,
        "total": len(tables),
    }


def get_operation(operation_id: Any) -> dict[str, Any] | None:
    """返回操作快照，避免请求线程读取到可变对象。"""
    if not isinstance(operation_id, str) or not OPERATION_ID_PATTERN.fullmatch(operation_id):
        return None
    with OPERATIONS_LOCK:
        operation = OPERATIONS.get(operation_id)
        if operation is None:
            return None
        return _operation_snapshot(operation)
