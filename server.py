"""入湖流水线控制台本地后端。

服务监听本机全部 IPv4 地址，负责提供前端、读写正式资源文件、启动真实
run_pipeline.py，并持续返回真实日志和逐表状态。
"""

from __future__ import annotations

import argparse
import ast
import base64
import importlib
import ipaddress
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import traceback
import uuid
from datetime import datetime
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import seatunnel_manager
import starrocks_rerun
import agency_orchestrator
import agency_providers


# 前端静态文件目录。
UI_ROOT = Path(__file__).resolve().parent

# 正式入湖项目目录，明确不使用工作区中的代码副本。
PIPELINE_ROOT = Path(r"D:\code\python\pythonProj\get_ddl").resolve()
PIPELINE_SCRIPT = PIPELINE_ROOT / "run_pipeline.py"
RESOURCE_ROOT = PIPELINE_ROOT / "resourceFile"
RESOURCE_FILE = RESOURCE_ROOT / "resource.text"
SYSTEM_MAPPING_FILE = RESOURCE_ROOT / "用户填写系统标准映射.txt"
SCRIPT_MAPPING_FILE = RESOURCE_ROOT / "job_py_script_mapping.txt"
CLOB_WHITELIST_FILE = RESOURCE_ROOT / "clob_tables.txt"
MANAGED_SOURCE_FILE = RESOURCE_ROOT / "managed_data_sources.json"

RUNTIME_ROOT = UI_ROOT / ".runtime"
HISTORY_ROOT = RUNTIME_ROOT / "pipeline-history"
HISTORY_INDEX_FILE = HISTORY_ROOT / "index.json"

MAX_BODY_BYTES = 2 * 1024 * 1024
MAX_LOG_CHARS = 4 * 1024 * 1024
MAX_HISTORY_LOG_CHARS = 1024 * 1024
MAX_HISTORY_RECORDS = 100
ACTIVE_PIPELINE_STATUSES = {"running", "stopping"}
RUN_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")
MANAGED_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]{1,39}$")
DATABASE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_$#-]{1,128}$")
SCRIPT_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,79}$")
ODS_PREFIX_PATTERN = re.compile(r"^ods_[a-z0-9_]{2,80}_$")
RESOURCE_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]{1,79}$")
CATALOG_CACHE_SECONDS = 5 * 60
CATALOG_MAX_TARGETS = 20
CATALOG_MAX_QUERY_CHARS = 500
CATALOG_MAX_LIMIT = 500
CATALOG_DEFAULT_LIMIT = 200
CATALOG_QUERY_SPLIT_PATTERN = re.compile(r"[,，\r\n]+")

CATALOG_CACHE_LOCK = threading.RLock()
CATALOG_CACHE: dict[tuple[str, str], tuple[float, tuple[str, ...]]] = {}
IMPORT_PATH_LOCK = threading.RLock()
MANAGED_SOURCE_LOCK = threading.RLock()


def load_db_check_configs(path: Path = PIPELINE_SCRIPT) -> dict[str, tuple[str, str, str, str]]:
    """通过 AST 读取连接元数据，不执行正式流水线代码。"""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
    except (OSError, UnicodeDecodeError, SyntaxError) as exc:
        raise RuntimeError("无法读取正式 DB_CHECK_CONFIGS") from exc

    config_node: ast.expr | None = None
    for statement in tree.body:
        if isinstance(statement, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "DB_CHECK_CONFIGS"
            for target in statement.targets
        ):
            config_node = statement.value
            break
        if (
            isinstance(statement, ast.AnnAssign)
            and isinstance(statement.target, ast.Name)
            and statement.target.id == "DB_CHECK_CONFIGS"
        ):
            config_node = statement.value
            break

    if config_node is None:
        raise RuntimeError("正式脚本缺少 DB_CHECK_CONFIGS 字面量")
    try:
        raw_configs = ast.literal_eval(config_node)
    except (ValueError, TypeError, SyntaxError) as exc:
        raise RuntimeError("DB_CHECK_CONFIGS 必须是可安全解析的字面量") from exc
    if not isinstance(raw_configs, dict):
        raise RuntimeError("DB_CHECK_CONFIGS 必须是字典")

    configs: dict[str, tuple[str, str, str, str]] = {}
    for job, config in raw_configs.items():
        if (
            isinstance(job, str)
            and isinstance(config, (tuple, list))
            and len(config) == 4
            and all(isinstance(value, str) for value in config)
        ):
            configs[job] = (config[0], config[1], config[2], config[3])
    return configs


def parse_system_mapping(text: str) -> dict[str, list[str]]:
    """解析“job: + 缩进别名”格式的系统标准映射。"""
    mappings: dict[str, list[str]] = {}
    current_job: str | None = None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if not raw_line[:1].isspace() and line.endswith(":"):
            current_job = line[:-1].strip()
            if current_job:
                mappings.setdefault(current_job, [])
            continue
        if (
            current_job is not None
            and raw_line[:1].isspace()
            and line not in mappings[current_job]
        ):
            mappings[current_job].append(line)
    return mappings


def catalog_system_id(job: str) -> str:
    """按约定将 job 归入业务系统。"""
    normalized = job.casefold()
    if normalized.startswith("s2b"):
        return "s2b"
    if normalized.startswith("hbgt"):
        return "hbgt"
    prefix, separator, _ = normalized.partition("2sr")
    return prefix if separator else normalized


def split_catalog_databases(value: str) -> list[str]:
    """拆分配置中的逗号分隔数据库或 Schema。"""
    databases: list[str] = []
    for item in value.split(","):
        database = item.strip()
        if database and database not in databases:
            databases.append(database)
    return databases


def load_catalog() -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    """生成公开目录及仅供后端使用的连接元数据白名单。"""
    systems, _, source_configs = load_legacy_sources()
    sources_by_system_id: dict[str, list[dict[str, Any]]] = {}
    for system in systems:
        data_sources = system["dataSources"]
        if isinstance(data_sources, list):
            sources_by_system_id[system["id"]] = data_sources
    system_ids = set(sources_by_system_id)
    registry = load_managed_source_registry()

    for managed_system in registry["systems"]:
        system_id = managed_system["id"]
        if system_id in system_ids:
            continue
        sources: list[dict[str, Any]] = []
        systems.append(
            {
                "id": system_id,
                "label": managed_system["label"],
                "dataSources": sources,
                "managed": True,
                "readOnly": False,
            }
        )
        sources_by_system_id[system_id] = sources
        system_ids.add(system_id)

    for source in registry["sources"]:
        if source.get("validationStatus") != "valid":
            continue
        system_id = source["systemId"]
        managed_sources = sources_by_system_id.get(system_id)
        if managed_sources is None:
            managed_sources = []
            systems.append(
                {
                    "id": system_id,
                    "label": source["systemLabel"],
                    "dataSources": managed_sources,
                    "managed": True,
                    "readOnly": False,
                }
            )
            sources_by_system_id[system_id] = managed_sources
        public_source = managed_source_public(source)
        managed_sources.append(public_source)
        source_configs[source["id"]] = {
            "mode": "managed",
            "type": source["type"],
            "host": source["host"],
            "port": source["port"],
            "username": source["username"],
            "databases": list(source["databases"]),
            "oracleMode": source["oracleMode"],
            "oracleService": source["oracleService"],
            "passwordEncrypted": source["passwordEncrypted"],
        }

    return systems, source_configs


def split_catalog_query(query: str) -> list[str]:
    """按中英文逗号和换行拆分查询词。"""
    terms: list[str] = []
    for item in CATALOG_QUERY_SPLIT_PATTERN.split(query):
        term = item.strip().casefold()
        if term and term not in terms:
            terms.append(term)
    return terms


def filter_catalog_items(
    rows: list[tuple[str, str, str]],
    terms: list[str],
    limit: int,
) -> list[dict[str, Any]]:
    """执行大小写不敏感子串匹配，并将完全匹配项置顶。"""
    items: list[dict[str, Any]] = []
    for source_id, database, table in rows:
        normalized_table = table.casefold()
        if terms and not any(term in normalized_table for term in terms):
            continue
        items.append(
            {
                "sourceId": source_id,
                "database": database,
                "table": table,
                "exact": any(term == normalized_table for term in terms),
            }
        )
    items.sort(
        key=lambda item: (
            not item["exact"],
            item["sourceId"].casefold(),
            item["database"].casefold(),
            item["table"].casefold(),
        )
    )
    return items[:limit]


def open_catalog_connection(
    config_or_module: dict[str, Any] | str,
    function_name: str | None = None,
) -> tuple[Any, Any]:
    """打开 legacy 或 managed 数据源连接。"""
    if isinstance(config_or_module, dict):
        config = config_or_module
        if config.get("mode") == "managed":
            return open_managed_connection(config)
        module_path = config["module"]
        function_name = config["function"]
    else:
        module_path = config_or_module
    if not function_name:
        raise RuntimeError("legacy 数据源缺少连接函数")

    import_root = str(PIPELINE_ROOT.parent.parent)
    with IMPORT_PATH_LOCK:
        path_added = import_root not in sys.path
        if path_added:
            sys.path.insert(0, import_root)
        try:
            module = importlib.import_module(module_path)
            connection_factory = getattr(module, function_name)
            result = connection_factory()
        finally:
            if path_added:
                sys.path.remove(import_root)

    if isinstance(result, tuple) and len(result) == 2:
        cursor, connection = result
    else:
        cursor = result
        connection = getattr(cursor, "connection", None)
    return cursor, connection


def query_catalog_tables(source_id: str, database: str, config: dict[str, Any]) -> list[str]:
    """查询单个白名单数据库中的基础表，并确保释放连接资源。"""
    cache_key = (source_id, database)
    now = time.monotonic()
    with CATALOG_CACHE_LOCK:
        cached = CATALOG_CACHE.get(cache_key)
        if cached is not None and now - cached[0] < CATALOG_CACHE_SECONDS:
            return list(cached[1])

    cursor = None
    connection = None
    try:
        cursor, connection = open_catalog_connection(config)
        if config["type"] == "oracle":
            cursor.execute(
                "SELECT table_name FROM all_tables WHERE owner = :owner",
                {"owner": database.upper()},
            )
        else:
            cursor.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_type = %s",
                (database, "BASE TABLE"),
            )
        tables = sorted(
            {
                str(row[0])
                for row in cursor.fetchall()
                if row and str(row[0]).strip()
            },
            key=str.casefold,
        )
        with CATALOG_CACHE_LOCK:
            CATALOG_CACHE[cache_key] = (time.monotonic(), tuple(tables))
        return tables
    finally:
        try:
            if cursor is not None:
                cursor.close()
        finally:
            if connection is not None and connection is not cursor:
                connection.close()


def parse_catalog_request(
    payload: dict[str, Any],
    source_configs: dict[str, dict[str, Any]],
) -> tuple[list[tuple[str, str, dict[str, Any]]], list[str], int]:
    """校验表目录请求，并将目标约束到目录白名单。"""
    targets = payload.get("targets")
    query = payload.get("query", "")
    limit = payload.get("limit", CATALOG_DEFAULT_LIMIT)
    if not isinstance(targets, list) or not targets:
        raise ValueError("targets 必须是非空数组")
    if len(targets) > CATALOG_MAX_TARGETS:
        raise ValueError(f"targets 不能超过 {CATALOG_MAX_TARGETS} 个")
    if not isinstance(query, str):
        raise ValueError("query 必须是字符串")
    if len(query) > CATALOG_MAX_QUERY_CHARS:
        raise ValueError(f"query 不能超过 {CATALOG_MAX_QUERY_CHARS} 个字符")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= CATALOG_MAX_LIMIT:
        raise ValueError(f"limit 必须是 1 到 {CATALOG_MAX_LIMIT} 的整数")

    selected: list[tuple[str, str, dict[str, Any]]] = []
    selected_keys: set[tuple[str, str]] = set()
    for target in targets:
        if not isinstance(target, dict):
            raise ValueError("targets 中的每一项必须是对象")
        source_id = target.get("sourceId")
        databases = target.get("databases")
        if not isinstance(source_id, str) or source_id not in source_configs:
            raise ValueError("sourceId 不在数据目录白名单中")
        if not isinstance(databases, list) or not databases:
            raise ValueError("databases 必须是非空数组")

        config = source_configs[source_id]
        allowed_databases = set(config["databases"])
        for database in databases:
            if not isinstance(database, str) or database not in allowed_databases:
                raise ValueError("database 不在数据目录白名单中")
            key = (source_id, database)
            if key not in selected_keys:
                selected_keys.add(key)
                selected.append((source_id, database, config))
    return selected, split_catalog_query(query), limit


def read_text(path: Path) -> str:
    """以 UTF-8-SIG 读取文本，兼容历史 BOM 文件。"""
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8-sig")


def write_text_atomic(path: Path, content: str) -> None:
    """原子写入配置，避免服务异常退出时留下半份文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = content.replace("\r\n", "\n").replace("\r", "\n")
    if normalized and not normalized.endswith("\n"):
        normalized += "\n"

    temp_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temp_path.write_text(normalized, encoding="utf-8")
    os.replace(temp_path, path)


def write_json_atomic(path: Path, payload: Any) -> None:
    """原子写入 JSON，避免并发读取到不完整的运行记录。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(temp_path, path)



def managed_pipeline_ready(source: dict[str, Any]) -> bool:
    """确认 managed 数据源可由正式流水线动态加载并执行初始化脚本。"""
    test_script = str(source.get("testScript") or "")
    prod_script = str(source.get("prodScript") or "")
    if (
        source.get("validationStatus") != "valid"
        or not test_script
        or not prod_script
        or not source.get("odsPrefix")
        or not source.get("starrocksResource")
        or len(source.get("databases") or []) != 1
    ):
        return False
    connector_file = PIPELINE_ROOT / "util" / "managedDataSource.py"
    conf_file = PIPELINE_ROOT / "seatunnel" / "getSeaTunnelConf2.py"
    try:
        pipeline_text = PIPELINE_SCRIPT.read_text(encoding="utf-8-sig")
        connector_text = connector_file.read_text(encoding="utf-8-sig")
        conf_text = conf_file.read_text(encoding="utf-8-sig")
    except (OSError, UnicodeDecodeError):
        return False
    return (
        "get_managed_db_check_configs" in pipeline_text
        and "get_managed_system_mapping" in pipeline_text
        and "get_managed_script_mapping" in pipeline_text
        and "get_managed_job_mappings" in pipeline_text
        and "JOB_MAPPING.update(get_managed_job_mappings())" in pipeline_text
        and "get_managed_seatunnel_config" in connector_text
        and "get_managed_seatunnel_config" in conf_text
        and (PIPELINE_ROOT / "starrocks" / "managedInit.py").is_file()
        and (PIPELINE_ROOT / "starrocks" / "test" / f"{test_script}.py").is_file()
        and (PIPELINE_ROOT / "starrocks" / f"{prod_script}.py").is_file()
    )


def load_legacy_sources() -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    """将正式 AST 配置和系统映射转换为只读 legacy 数据源。"""
    configs = load_db_check_configs()
    mappings = parse_system_mapping(read_text(SYSTEM_MAPPING_FILE))
    systems: list[dict[str, Any]] = []
    flat_sources: list[dict[str, Any]] = []
    sources_by_system_id: dict[str, list[dict[str, Any]]] = {}
    source_configs: dict[str, dict[str, Any]] = {}

    for job, aliases in mappings.items():
        config = configs.get(job)
        if not aliases or config is None:
            continue
        db_type, module_path, function_name, database_value = config
        normalized_type = db_type.casefold()
        databases = split_catalog_databases(database_value)
        if normalized_type not in {"oracle", "mysql"} or not databases:
            continue

        system_id = catalog_system_id(job)
        system_label = system_id.upper()
        sources = sources_by_system_id.get(system_id)
        if sources is None:
            sources = []
            sources_by_system_id[system_id] = sources
            systems.append(
                {
                    "id": system_id,
                    "label": system_label,
                    "dataSources": sources,
                    "managed": False,
                    "readOnly": True,
                }
            )
        source = {
            "id": job,
            "systemId": system_id,
            "systemLabel": system_label,
            "label": aliases[0],
            "type": normalized_type,
            "databases": databases,
            "aliases": aliases,
            "pipelineReady": True,
            "managed": False,
            "readOnly": True,
        }
        sources.append(source)
        flat_sources.append(source)
        source_configs[job] = {
            "mode": "legacy",
            "type": normalized_type,
            "module": module_path,
            "function": function_name,
            "databases": databases,
        }
    return systems, flat_sources, source_configs


def empty_managed_source_registry() -> dict[str, list[dict[str, Any]]]:
    """生成空的数据源注册表。"""
    return {"systems": [], "sources": []}


def load_managed_source_registry() -> dict[str, list[dict[str, Any]]]:
    """在锁内读取 managed 注册表，避免与原子替换并发。"""
    with MANAGED_SOURCE_LOCK:
        if not MANAGED_SOURCE_FILE.exists():
            return empty_managed_source_registry()
        try:
            payload = json.loads(MANAGED_SOURCE_FILE.read_text(encoding="utf-8-sig"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("managed 数据源注册表无法读取") from exc
        if (
            not isinstance(payload, dict)
            or not isinstance(payload.get("systems"), list)
            or not isinstance(payload.get("sources"), list)
            or not all(isinstance(item, dict) for item in payload["systems"])
            or not all(isinstance(item, dict) for item in payload["sources"])
        ):
            raise RuntimeError("managed 数据源注册表格式不合法")
        return {
            "systems": [dict(item) for item in payload["systems"]],
            "sources": [dict(item) for item in payload["sources"]],
        }


def save_managed_source_registry(registry: dict[str, list[dict[str, Any]]]) -> None:
    """在注册表锁内使用原子 JSON 写入。"""
    with MANAGED_SOURCE_LOCK:
        write_json_atomic(MANAGED_SOURCE_FILE, registry)


def encrypt_managed_password(password: str) -> str:
    """使用当前 Windows 用户的 DPAPI 加密密码。"""
    if os.name != "nt":
        raise RuntimeError("managed 数据源密码仅支持 Windows DPAPI")
    try:
        win32crypt = importlib.import_module("win32crypt")

        encrypted = win32crypt.CryptProtectData(
            password.encode("utf-8"),
            "Lake ingestion managed source",
            None,
            None,
            None,
            0,
        )
    except Exception as exc:
        raise RuntimeError("无法使用 Windows DPAPI 加密密码") from exc
    return base64.b64encode(encrypted).decode("ascii")


def decrypt_managed_password(encrypted_password: str) -> str:
    """解密注册表中的 DPAPI 密文，不向调用方暴露密文。"""
    if os.name != "nt":
        raise RuntimeError("managed 数据源密码仅支持 Windows DPAPI")
    try:
        win32crypt = importlib.import_module("win32crypt")

        encrypted = base64.b64decode(encrypted_password, validate=True)
        _, decrypted = win32crypt.CryptUnprotectData(
            encrypted,
            None,
            None,
            None,
            0,
        )
        return decrypted.decode("utf-8")
    except Exception as exc:
        raise RuntimeError("无法使用 Windows DPAPI 解密密码") from exc


def managed_source_public(source: dict[str, Any]) -> dict[str, Any]:
    """构造绝不包含密码或密文的 managed 数据源响应。"""
    test_script = str(source.get("testScript") or "")
    prod_script = str(source.get("prodScript") or "")
    return {
        "id": source["id"],
        "systemId": source["systemId"],
        "systemLabel": source["systemLabel"],
        "label": source["label"],
        "type": source["type"],
        "host": source["host"],
        "port": source["port"],
        "username": source["username"],
        "databases": list(source["databases"]),
        "oracleMode": source["oracleMode"],
        "oracleService": source["oracleService"],
        "aliases": list(source["aliases"]),
        "testScript": test_script,
        "prodScript": prod_script,
        "odsPrefix": source.get("odsPrefix", ""),
        "starrocksResource": source.get("starrocksResource", ""),
        "startupMode": source.get("startupMode", "latest"),
        "parallelism": source.get("parallelism", 1),
        "validationStatus": source.get("validationStatus", "stale"),
        "message": source.get("message", "尚未验证"),
        "checkedAt": source.get("checkedAt"),
        "hasPassword": bool(source.get("passwordEncrypted")),
        "pipelineReady": managed_pipeline_ready(source),
        "managed": True,
        "readOnly": False,
    }


def load_source_management() -> dict[str, list[dict[str, Any]]]:
    """合并 legacy 与 managed 数据源管理视图。"""
    legacy_systems, legacy_sources, _ = load_legacy_sources()
    registry = load_managed_source_registry()
    systems = [
        {
            "id": system["id"],
            "label": system["label"],
            "managed": False,
            "readOnly": True,
        }
        for system in legacy_systems
    ]
    legacy_system_ids = {system["id"] for system in systems}
    systems.extend(
        {
            "id": system["id"],
            "label": system["label"],
            "managed": True,
            "readOnly": False,
        }
        for system in registry["systems"]
        if system.get("id") not in legacy_system_ids
    )
    return {
        "systems": systems,
        "sources": legacy_sources
        + [managed_source_public(source) for source in registry["sources"]],
    }


def require_managed_id(value: Any, field_name: str) -> str:
    """校验 managed 系统和数据源标识符。"""
    if not isinstance(value, str) or not MANAGED_ID_PATTERN.fullmatch(value):
        raise ValueError(
            f"{field_name} 必须以小写字母开头，且为 2-40 位小写字母、数字或下划线"
        )
    return value


def require_nonempty_text(value: Any, field_name: str, max_length: int = 200) -> str:
    """校验并清理普通文本字段。"""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} 必须是非空字符串")
    normalized = value.strip()
    if len(normalized) > max_length:
        raise ValueError(f"{field_name} 不能超过 {max_length} 个字符")
    return normalized


def normalize_managed_databases(value: Any) -> list[str]:
    """校验数据库或 Schema 列表并保持输入顺序去重。"""
    if not isinstance(value, list) or not value:
        raise ValueError("databases 必须是非空数组")
    databases: list[str] = []
    for item in value:
        if not isinstance(item, str) or not DATABASE_NAME_PATTERN.fullmatch(item):
            raise ValueError("databases 仅允许常见数据库标识符字符")
        if item not in databases:
            databases.append(item)
    return databases


def normalize_managed_aliases(value: Any) -> list[str]:
    """校验非空别名列表并去重。"""
    if not isinstance(value, list) or not value:
        raise ValueError("aliases 必须是非空数组")
    aliases: list[str] = []
    for item in value:
        alias = require_nonempty_text(item, "aliases 中的别名", 200)
        if alias not in aliases:
            aliases.append(alias)
    return aliases


def managed_system_labels(
    registry: dict[str, list[dict[str, Any]]],
) -> dict[str, str]:
    """返回 legacy 与 managed 系统标签。"""
    legacy_systems, _, _ = load_legacy_sources()
    labels = {system["id"]: system["label"] for system in legacy_systems}
    labels.update(
        {
            system["id"]: system["label"]
            for system in registry["systems"]
        }
    )
    return labels


def normalize_managed_source(
    payload: dict[str, Any],
    registry: dict[str, list[dict[str, Any]]],
    existing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """校验创建或更新请求并生成内部数据源记录。"""
    values = dict(existing or {})
    values.update(
        {
            key: payload[key]
            for key in (
                "id",
                "systemId",
                "label",
                "type",
                "host",
                "port",
                "username",
                "databases",
                "oracleMode",
                "oracleService",
                "aliases",
                "testScript",
                "prodScript",
                "odsPrefix",
                "starrocksResource",
                "startupMode",
                "parallelism",
            )
            if key in payload
        }
    )
    source_id = require_managed_id(values.get("id"), "id")
    if existing is not None and source_id != existing["id"]:
        raise ValueError("managed 数据源 id 不允许修改")
    system_id = require_managed_id(values.get("systemId"), "systemId")
    system_labels = managed_system_labels(registry)
    if system_id not in system_labels:
        raise ValueError("systemId 对应的系统不存在")

    source_type = values.get("type")
    if source_type not in {"mysql", "oracle"}:
        raise ValueError("type 仅支持 mysql 或 oracle")
    host = require_nonempty_text(values.get("host"), "host", 255)
    if re.search(r"\s", host) or "://" in host or "/" in host or "\\" in host:
        raise ValueError("host 不允许包含协议、斜杠或空白")
    port = values.get("port")
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError("port 必须是 1 到 65535 的整数")

    oracle_mode = values.get("oracleMode", "serviceName")
    if oracle_mode not in {"serviceName", "sid"}:
        raise ValueError("oracleMode 仅支持 serviceName 或 sid")
    oracle_service_value = values.get("oracleService", "")
    if not isinstance(oracle_service_value, str):
        raise ValueError("oracleService 必须是字符串")
    oracle_service = oracle_service_value.strip()
    if source_type == "oracle" and not oracle_service:
        raise ValueError("Oracle 数据源的 oracleService 不能为空")

    test_script = values.get("testScript", "")
    prod_script = values.get("prodScript", "")
    if not isinstance(test_script, str) or not isinstance(prod_script, str):
        raise ValueError("testScript 和 prodScript 必须是字符串")
    if test_script and not SCRIPT_NAME_PATTERN.fullmatch(test_script):
        raise ValueError("testScript 只能填写不带 .py 的脚本名")
    if prod_script and not SCRIPT_NAME_PATTERN.fullmatch(prod_script):
        raise ValueError("prodScript 只能填写不带 .py 的脚本名")
    ods_prefix = str(values.get("odsPrefix") or "").strip()
    resource_name = str(values.get("starrocksResource") or "").strip()
    if ods_prefix and not ODS_PREFIX_PATTERN.fullmatch(ods_prefix):
        raise ValueError("odsPrefix 必须是 ods_ 开头、下划线结尾的小写前缀")
    if resource_name and not RESOURCE_NAME_PATTERN.fullmatch(resource_name):
        raise ValueError("starrocksResource 格式不合法")
    startup_mode = values.get("startupMode", "latest")
    if startup_mode not in {"latest", "initial"}:
        raise ValueError("startupMode 仅支持 latest 或 initial")
    parallelism = values.get("parallelism", 1)
    if isinstance(parallelism, bool) or not isinstance(parallelism, int) or not 1 <= parallelism <= 64:
        raise ValueError("parallelism 必须是 1 到 64 的整数")

    return {
        "id": source_id,
        "systemId": system_id,
        "systemLabel": system_labels[system_id],
        "label": require_nonempty_text(values.get("label"), "label"),
        "type": source_type,
        "host": host,
        "port": port,
        "username": require_nonempty_text(values.get("username"), "username", 200),
        "databases": normalize_managed_databases(values.get("databases")),
        "oracleMode": oracle_mode,
        "oracleService": oracle_service,
        "aliases": normalize_managed_aliases(values.get("aliases")),
        "testScript": test_script.strip(),
        "prodScript": prod_script.strip(),
        "odsPrefix": ods_prefix,
        "starrocksResource": resource_name,
        "startupMode": startup_mode,
        "parallelism": parallelism,
        "passwordEncrypted": values.get("passwordEncrypted", ""),
        "validationStatus": "stale",
        "message": "配置已变更，等待重新验证" if existing else "尚未验证",
        "checkedAt": None,
    }


def find_managed_source(
    registry: dict[str, list[dict[str, Any]]],
    source_id: str,
) -> dict[str, Any] | None:
    """按 ID 查找 managed 数据源。"""
    return next(
        (source for source in registry["sources"] if source.get("id") == source_id),
        None,
    )


def clear_catalog_cache() -> None:
    """清理表目录缓存，使 managed 配置修改立即生效。"""
    with CATALOG_CACHE_LOCK:
        CATALOG_CACHE.clear()


def open_managed_connection(config: dict[str, Any]) -> tuple[Any, Any]:
    """使用注册表中的 DPAPI 凭据打开 managed 数据源。"""
    password = decrypt_managed_password(config["passwordEncrypted"])
    if config["type"] == "mysql":
        import pymysql

        connection = pymysql.connect(
            host=config["host"],
            port=config["port"],
            user=config["username"],
            password=password,
            connect_timeout=5,
            read_timeout=5,
            write_timeout=5,
        )
    else:
        import oracledb

        dsn_kwargs = {
            "service_name": config["oracleService"]
        } if config["oracleMode"] == "serviceName" else {
            "sid": config["oracleService"]
        }
        dsn = oracledb.makedsn(
            config["host"],
            config["port"],
            **dsn_kwargs,
        )
        connection = oracledb.connect(
            user=config["username"],
            password=password,
            dsn=dsn,
        )
    return connection.cursor(), connection


def validate_managed_source_connection(source: dict[str, Any]) -> None:
    """验证连接、基本查询及每个配置数据库或 Schema。"""
    config = {
        "mode": "managed",
        "type": source["type"],
        "host": source["host"],
        "port": source["port"],
        "username": source["username"],
        "oracleMode": source["oracleMode"],
        "oracleService": source["oracleService"],
        "passwordEncrypted": source["passwordEncrypted"],
    }
    cursor = None
    connection = None
    try:
        cursor, connection = open_managed_connection(config)
        if source["type"] == "mysql":
            cursor.execute("SELECT 1")
            cursor.fetchone()
            for database in source["databases"]:
                cursor.execute(
                    "SELECT schema_name FROM information_schema.schemata "
                    "WHERE schema_name = %s",
                    (database,),
                )
                if cursor.fetchone() is None:
                    raise LookupError("配置的数据库不存在")
        else:
            cursor.execute("SELECT 1 FROM dual")
            cursor.fetchone()
            for database in source["databases"]:
                cursor.execute(
                    "SELECT username FROM all_users WHERE username = :username",
                    {"username": database.upper()},
                )
                if cursor.fetchone() is None:
                    raise LookupError("配置的 Schema 不存在")
    finally:
        try:
            if cursor is not None:
                cursor.close()
        finally:
            if connection is not None and connection is not cursor:
                connection.close()


def managed_validation_error(exc: Exception) -> str:
    """返回不包含连接串、主机、用户名或密码的验证错误。"""
    error_type = type(exc).__name__
    if isinstance(exc, ModuleNotFoundError):
        return f"{error_type}: 数据库驱动不可用"
    if isinstance(exc, LookupError):
        return f"{error_type}: 配置的数据库或 Schema 不存在"
    return f"{error_type}: 连接或权限验证失败"


def resource_references_source(source_id: str) -> bool:
    """检查正式任务文件是否以独立标识符引用数据源。"""
    pattern = re.compile(
        rf"(?<![A-Za-z0-9_]){re.escape(source_id)}(?![A-Za-z0-9_])"
    )
    return bool(pattern.search(read_text(RESOURCE_FILE)))


def load_pipeline_history_index() -> list[dict[str, Any]]:
    """读取历史索引；损坏或不存在时返回空列表。"""
    if not HISTORY_INDEX_FILE.exists():
        return []
    try:
        payload = json.loads(HISTORY_INDEX_FILE.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []
    return [
        item
        for item in payload
        if isinstance(item, dict)
        and isinstance(item.get("runId"), str)
        and RUN_ID_PATTERN.fullmatch(item["runId"])
    ]


def summarize_pipeline_history(record: dict[str, Any]) -> dict[str, Any]:
    """从完整运行记录生成列表页所需的轻量摘要。"""
    tasks = record.get("tasks")
    task_list = tasks if isinstance(tasks, list) else []
    results = record.get("results")
    task_statuses: list[str] = []
    if isinstance(results, list):
        for result in results:
            final = result.get("final") if isinstance(result, dict) else None
            if isinstance(final, str) and final and final not in task_statuses:
                task_statuses.append(final)
    return {
        "runId": record.get("runId"),
        "status": record.get("status"),
        "taskStatus": "；".join(task_statuses),
        "startedAt": record.get("startedAt"),
        "finishedAt": record.get("finishedAt"),
        "returnCode": record.get("returnCode"),
        "error": record.get("error"),
        "taskCount": len(task_list),
        "tables": [
            task.get("table", "")
            for task in task_list
            if isinstance(task, dict) and task.get("table")
        ],
    }


def save_pipeline_history(record: dict[str, Any]) -> None:
    """保存单次运行详情并更新最近运行索引。"""
    run_id = record.get("runId")
    if not isinstance(run_id, str) or not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError("运行记录缺少合法的 runId")

    detail_file = HISTORY_ROOT / f"{run_id}.json"
    write_json_atomic(detail_file, record)

    summary = summarize_pipeline_history(record)
    index = [
        item
        for item in load_pipeline_history_index()
        if item.get("runId") != run_id
    ]
    index.insert(0, summary)
    expired = index[MAX_HISTORY_RECORDS:]
    write_json_atomic(HISTORY_INDEX_FILE, index[:MAX_HISTORY_RECORDS])

    for item in expired:
        expired_run_id = item.get("runId")
        if isinstance(expired_run_id, str) and RUN_ID_PATTERN.fullmatch(expired_run_id):
            try:
                (HISTORY_ROOT / f"{expired_run_id}.json").unlink(missing_ok=True)
            except OSError:
                pass


def load_pipeline_history(run_id: str) -> dict[str, Any] | None:
    """按运行 ID 读取详情，非法 ID 不访问文件系统。"""
    if not RUN_ID_PATTERN.fullmatch(run_id):
        return None
    detail_file = HISTORY_ROOT / f"{run_id}.json"
    if not detail_file.exists():
        return None
    try:
        payload = json.loads(detail_file.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def mark_pipeline_history_unknown(record: dict[str, Any]) -> dict[str, Any]:
    """将当前实例无法监控的活动记录以终态未知返回，不修改磁盘。"""
    unknown_record = dict(record)
    unknown_record["status"] = "interrupted"
    existing_log = str(unknown_record.get("log") or "")
    unknown_record["log"] = (
        existing_log
        + "\n[控制台] 当前控制台实例无法确认该流水线进程终态。\n"
    )
    tasks = unknown_record.get("tasks")
    if isinstance(tasks, list):
        unknown_record["results"] = parse_pipeline_results(
            unknown_record["log"],
            [task for task in tasks if isinstance(task, dict)],
            "interrupted",
        )
    return unknown_record


def validate_clob_whitelist(text: str) -> list[str]:
    """校验 CLOB 白名单的“系统.Schema.表名”格式。"""
    errors: list[str] = []
    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = [part.strip() for part in line.split(".")]
        if len(parts) != 3 or not all(parts):
            errors.append(
                f"CLOB 白名单第 {line_number} 行格式错误，应为：系统.Schema.表名"
            )
    return errors


def parse_task_lines(text: str) -> tuple[list[dict[str, str]], list[str]]:
    """校验任务文本，返回结构化任务和错误列表。"""
    tasks: list[dict[str, str]] = []
    errors: list[str] = []

    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue

        parts = line.split()
        if len(parts) < 3:
            errors.append(f"第 {line_number} 行格式错误，应为：别名 源表名 操作描述")
            continue

        alias, table_name = parts[0], parts[1]
        description = " ".join(parts[2:])
        if "字段" in description:
            operation = "add_field"
            operation_label = "新增字段"
        elif "表" in description:
            operation = "new_table"
            operation_label = "新建表"
        else:
            errors.append(f"第 {line_number} 行操作描述必须包含“表”或“字段”")
            continue

        tasks.append(
            {
                "alias": alias,
                "table": table_name,
                "description": description,
                "operation": operation,
                "operationLabel": operation_label,
                "raw": line,
            }
        )

    if not tasks and not errors:
        errors.append("任务列表不能为空")
    return tasks, errors


def normalize_status(status: str) -> str:
    """移除日志表格中可能出现的 ANSI 控制符和首尾空格。"""
    ansi_pattern = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
    return ansi_pattern.sub("", status).strip()


PIPELINE_PROGRESS_PREFIX = "[PIPELINE_PROGRESS] "
REPORT_STEP_TO_STAGE = {
    "测试环境": "test",
    "停止任务": "stop",
    "生产环境": "prod",
    "生成配置": "conf",
    "启动任务": "start",
}
PIPELINE_STAGE_ORDER = ["check", "test", "stop", "prod", "conf", "start"]


def parse_pipeline_progress_events(log_text: str) -> dict[str, dict[str, str]]:
    """读取正式流水线输出的逐表 JSON 进度事件。"""
    events: dict[str, dict[str, str]] = {}
    for line in log_text.splitlines():
        marker_index = line.find(PIPELINE_PROGRESS_PREFIX)
        if marker_index < 0:
            continue
        payload_text = line[marker_index + len(PIPELINE_PROGRESS_PREFIX):].strip()
        try:
            payload = json.loads(payload_text)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(payload, dict):
            continue
        task_line = str(payload.get("task") or "").strip()
        step = str(payload.get("step") or "").strip()
        status = normalize_status(str(payload.get("status") or ""))
        if not task_line or not step or not status:
            continue
        event = events.setdefault(task_line, {})
        event[step] = status
        event["job"] = str(payload.get("job") or "").strip()
        event["table"] = str(payload.get("table") or "").strip()
    return events


def parse_legacy_job_progress(
    log_text: str,
) -> tuple[dict[str, str], dict[tuple[str, str], str], str]:
    """按目标任务切分旧日志，避免把一个任务的阶段套用到全部表。"""
    task_jobs: dict[tuple[str, str], str] = {}
    mapping_pattern = re.compile(
        r"别名:\s*(?P<alias>[^|\r\n]+?)\s*\|\s*"
        r"映射任务:\s*(?P<job>[^|\r\n]+?)\s*\|\s*"
        r"表名:\s*(?P<table>.*?)\s+\.\.\."
    )
    for match in mapping_pattern.finditer(log_text):
        identity = (
            match.group("alias").strip().lower(),
            match.group("table").strip().lower(),
        )
        task_jobs[identity] = match.group("job").strip().lower()

    section_pattern = re.compile(
        r"^=+\s*开始全系流水线主流程，目标体系包:\s*"
        r"(?P<job>[^\s=]+)\s*=+\s*$",
        re.MULTILINE,
    )
    matches = list(section_pattern.finditer(log_text))
    sections: dict[str, str] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(log_text)
        sections[match.group("job").strip().lower()] = log_text[match.start():end]
    current_job = matches[-1].group("job").strip().lower() if matches else ""
    return sections, task_jobs, current_job


def parse_legacy_table_stage_status(
    section: str,
    table_name: str,
    stage_number: int,
) -> str | None:
    """从旧初始化脚本的成功/失败表清单中提取单表状态。"""
    header_pattern = re.compile(rf"^--- {stage_number}\.[^\r\n]*$", re.MULTILINE)
    boundary_pattern = re.compile(
        r"^(?:--- [1-5]\.|\[阶段 [1-5](?:\.\d+)?:|\[任务策略控制\])",
        re.MULTILINE,
    )
    expected_table = table_name.strip().lower()
    for header in header_pattern.finditer(section):
        boundary = boundary_pattern.search(section, header.end())
        segment_end = boundary.start() if boundary else len(section)
        segment = section[header.end():segment_end]
        for label, status in (
            ("以下任务初始化成功：", "成功"),
            ("以下任务初始化失败/超时：", "失败(任务反馈)"),
        ):
            match = re.search(re.escape(label) + r"\s*\r?\n([^\r\n]+)", segment)
            if not match:
                continue
            tables = {
                item.strip().lower()
                for item in match.group(1).split(",")
                if item.strip()
            }
            if expected_table in tables:
                return status
    return None


def infer_legacy_task_progress(
    section: str,
    job_name: str,
    table_name: str,
    process_status: str,
    is_current_job: bool,
) -> tuple[dict[str, str], str]:
    """为尚未输出逐表事件的旧流水线推断单个任务的阶段。"""
    stage_patterns = [
        ("test", r"\[阶段 1:"),
        ("stop", r"\[阶段 2:"),
        ("prod", r"\[阶段 3:"),
        ("conf", r"\[阶段 4:"),
        ("start", r"\[阶段 5:"),
    ]
    reached_stage = "check"
    for stage_key, pattern in stage_patterns:
        if re.search(pattern, section):
            reached_stage = stage_key
    reached_index = PIPELINE_STAGE_ORDER.index(reached_stage)
    completed = bool(
        re.search(
            rf"【\s*{re.escape(job_name)}\s*】[^\r\n]*(?:完成|成功)",
            section,
        )
    )
    strategy_completed = "初始化流程完成，按策略跳过后续配置生成、上传及启动任务步骤" in section

    stages: dict[str, str] = {}
    for stage_index, stage_key in enumerate(PIPELINE_STAGE_ORDER):
        if stage_key == "check":
            stages[stage_key] = "成功"
        elif stage_index < reached_index:
            stages[stage_key] = "成功" if completed else "已执行"
        elif stage_index == reached_index:
            if completed:
                stages[stage_key] = "成功"
            elif is_current_job and process_status == "running":
                stages[stage_key] = "进行中"
            elif is_current_job and process_status == "stopping":
                stages[stage_key] = "停止中"
            else:
                stages[stage_key] = "已执行"
        else:
            stages[stage_key] = "待执行"

    test_status = parse_legacy_table_stage_status(section, table_name, 1)
    prod_status = parse_legacy_table_stage_status(section, table_name, 3)
    if test_status:
        stages["test"] = test_status
    if prod_status:
        stages["prod"] = prod_status

    if strategy_completed:
        stages["stop"] = "按策略跳过"
        stages["conf"] = "策略跳过"
        stages["start"] = "策略跳过"
        final = "初始化完成"
    elif completed:
        final = "完美完成"
    elif is_current_job and process_status == "failed":
        final = "失败（查看日志）"
    elif is_current_job and process_status == "stopping":
        final = "正在停止"
    elif is_current_job and process_status == "stopped":
        final = "已手动停止"
    elif is_current_job and process_status == "interrupted":
        final = "终态未知（控制台中断）"
    else:
        final = "进行中" if is_current_job else "已结束（等待最终总览）"
    return stages, final


def parse_pipeline_results(
    log_text: str,
    submitted_tasks: list[dict[str, str]],
    process_status: str,
) -> list[dict[str, Any]]:
    """按最终报告、逐表事件、任务区块的优先级解析真实进度。"""
    report_blocks: dict[tuple[str, str], dict[str, str]] = {}
    report_tables: dict[str, list[dict[str, str]]] = {}
    block_pattern = re.compile(
        r"\[(?P<job>[^\]\r\n]+)\]\s*表名:\s*(?P<table>[^\r\n]+)"
        r"(?P<body>.*?)(?=\r?\n\[[^\]\r\n]+\]\s*表名:|\Z)",
        re.DOTALL,
    )
    field_patterns = {
        "test": r"测试初始化\s*:\s*([^\r\n]+)",
        "stop": r"停SeaTunnel\s*:\s*([^\r\n]+)",
        "prod": r"生产初始化\s*:\s*([^\r\n]+)",
        "conf": r"生成 Conf\s*:\s*([^\r\n]+)",
        "start": r"启SeaTunnel\s*:\s*([^\r\n]+)",
        "final": r"综合终态\s*:\s*([^\r\n]+)",
    }
    for block in block_pattern.finditer(log_text):
        job_name = block.group("job").strip().lower()
        table_name = block.group("table").strip().lower()
        body = block.group("body")
        values: dict[str, str] = {"job": job_name}
        for key, pattern in field_patterns.items():
            match = re.search(pattern, body)
            if match:
                values[key] = normalize_status(match.group(1))
        report_blocks[(job_name, table_name)] = values
        report_tables.setdefault(table_name, []).append(values)

    progress_events = parse_pipeline_progress_events(log_text)
    job_sections, task_jobs, current_job = parse_legacy_job_progress(log_text)
    results: list[dict[str, Any]] = []
    for index, task in enumerate(submitted_tasks, start=1):
        identity = (task["alias"].lower(), task["table"].lower())
        target_job = task_jobs.get(identity, task["alias"].lower())
        parsed = report_blocks.get((target_job, task["table"].lower()))
        if parsed is None:
            table_reports = report_tables.get(task["table"].lower(), [])
            parsed = table_reports[0] if len(table_reports) == 1 else None

        if parsed:
            stages = {
                "check": "成功",
                "test": parsed.get("test", "-"),
                "stop": parsed.get("stop", "-"),
                "prod": parsed.get("prod", "-"),
                "conf": parsed.get("conf", "-"),
                "start": parsed.get("start", "-"),
            }
            final = parsed.get("final", "已完成")
        elif task["raw"] in progress_events:
            event = progress_events[task["raw"]]
            stages = {stage: "待执行" for stage in PIPELINE_STAGE_ORDER}
            stages["check"] = "成功"
            for report_step, stage_key in REPORT_STEP_TO_STAGE.items():
                if report_step in event:
                    stages[stage_key] = event[report_step]
            final = event.get("最终结果", "")
            if not final or final == "等待":
                final = "进行中" if process_status == "running" else "已结束（等待最终总览）"
        elif target_job in job_sections:
            stages, final = infer_legacy_task_progress(
                job_sections[target_job],
                target_job,
                task["table"],
                process_status,
                target_job == current_job,
            )
        else:
            stages = {stage: "待执行" for stage in PIPELINE_STAGE_ORDER}
            stages["check"] = "成功" if identity in task_jobs else "进行中"
            if process_status == "failed":
                final = "失败（查看日志）"
            elif process_status == "succeeded":
                final = "已结束（查看日志）"
            elif process_status == "stopping":
                final = "正在停止"
            elif process_status == "stopped":
                final = "已手动停止"
            elif process_status == "interrupted":
                final = "终态未知（控制台中断）"
            else:
                final = "等待执行" if identity in task_jobs else "进行中"

        if process_status == "stopping":
            final = "正在停止"
        elif process_status == "stopped":
            final = "已手动停止"
        elif process_status == "interrupted":
            final = "终态未知（控制台中断）"

        results.append(
            {
                "id": index,
                "alias": task["alias"],
                "table": task["table"],
                "opLabel": task["operationLabel"],
                "stages": stages,
                "final": final,
            }
        )
    return results


class PipelineState:
    """真实流水线的线程安全单实例状态。"""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.run_id: str | None = None
        self.status = "idle"
        self.started_at: str | None = None
        self.finished_at: str | None = None
        self.return_code: int | None = None
        self.log_text = ""
        self.error: str | None = None
        self.tasks: list[dict[str, str]] = []
        self.process: subprocess.Popen[str] | None = None
        self.stop_requested = False

    def snapshot(self) -> dict[str, Any]:
        """生成前端可直接消费的状态快照。"""
        with self.lock:
            results = (
                parse_pipeline_results(self.log_text, self.tasks, self.status)
                if self.tasks
                else []
            )
            return {
                "runId": self.run_id,
                "status": self.status,
                "startedAt": self.started_at,
                "finishedAt": self.finished_at,
                "returnCode": self.return_code,
                "log": self.log_text,
                "error": self.error,
                "tasks": self.tasks,
                "results": results,
                "resourceText": read_text(RESOURCE_FILE),
            }

    def history_record(self) -> dict[str, Any]:
        """生成可持久化的运行详情。"""
        with self.lock:
            return self._history_record_locked()

    def history_summary(self) -> dict[str, Any]:
        """生成当前运行的历史列表摘要。"""
        return summarize_pipeline_history(self.history_record())

    def _history_record_locked(self) -> dict[str, Any]:
        """在持有状态锁时生成历史记录。"""
        history_log = self.log_text
        if len(history_log) > MAX_HISTORY_LOG_CHARS:
            history_log = (
                "[历史记录仅保留末尾日志，完整日志请查看正式流水线日志文件]\n"
                + history_log[-MAX_HISTORY_LOG_CHARS:]
            )
        results = (
            parse_pipeline_results(self.log_text, self.tasks, self.status)
            if self.tasks
            else []
        )
        return {
            "runId": self.run_id,
            "status": self.status,
            "startedAt": self.started_at,
            "finishedAt": self.finished_at,
            "returnCode": self.return_code,
            "log": history_log,
            "error": self.error,
            "tasks": self.tasks,
            "results": results,
        }

    def _persist_history_locked(self) -> None:
        """持久化当前状态；记录失败不影响正式流水线执行。"""
        if self.run_id is None:
            return
        try:
            save_pipeline_history(self._history_record_locked())
        except (OSError, TypeError, ValueError) as exc:
            self.log_text += (
                f"[控制台] 保存运行记录失败：{type(exc).__name__}: {exc}\n"
            )

    def append_log(self, line: str) -> None:
        """追加实时日志，并限制内存占用。"""
        with self.lock:
            self.log_text += line
            if len(self.log_text) > MAX_LOG_CHARS:
                self.log_text = (
                    "[较早日志已截断，完整日志请查看正式流水线日志文件]\n"
                    + self.log_text[-MAX_LOG_CHARS:]
                )

    def start(self, tasks: list[dict[str, str]]) -> str:
        """登记运行状态并启动后台线程。"""
        with self.lock:
            if self.status in ACTIVE_PIPELINE_STATUSES:
                raise RuntimeError("已有流水线正在运行或停止中，请等待当前任务结束")

            self.run_id = uuid.uuid4().hex
            self.status = "running"
            self.started_at = datetime.now().isoformat(timespec="seconds")
            self.finished_at = None
            self.return_code = None
            self.log_text = ""
            self.error = None
            self.tasks = tasks
            self.process = None
            self.stop_requested = False
            run_id = self.run_id
            self._persist_history_locked()

        worker = threading.Thread(
            target=self._run_process,
            args=(run_id,),
            daemon=True,
            name=f"lake-pipeline-{run_id[:8]}",
        )
        worker.start()
        return run_id

    def stop(self, run_id: str) -> str:
        """请求停止指定运行，并异步终止其完整进程树。"""
        with self.lock:
            if self.run_id != run_id:
                raise RuntimeError("运行 ID 已失效，请刷新状态后重试")
            if self.status not in ACTIVE_PIPELINE_STATUSES:
                raise RuntimeError("当前没有正在运行的流水线")
            if self.status == "stopping":
                return run_id

            self.status = "stopping"
            self.stop_requested = True
            self.log_text += "\n[控制台] 已收到手动停止请求，正在终止流水线进程树…\n"
            self._persist_history_locked()
            process = self.process

        # 进程可能仍处于创建阶段；这种情况下 _run_process 会在登记进程后执行终止。
        if process is not None:
            worker = threading.Thread(
                target=self._terminate_process_tree,
                args=(process,),
                daemon=True,
                name=f"lake-pipeline-stop-{run_id[:8]}",
            )
            worker.start()
        return run_id

    def _terminate_process_tree(self, process: subprocess.Popen[str]) -> None:
        """终止流水线及其派生进程，避免残留初始化或 SeaTunnel 操作。"""
        if process.poll() is not None:
            return

        try:
            if os.name == "nt":
                result = subprocess.run(
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                if result.returncode != 0 and process.poll() is None:
                    process.terminate()
            else:
                os.killpg(process.pid, signal.SIGTERM)

            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                if os.name == "nt":
                    process.kill()
                else:
                    os.killpg(process.pid, signal.SIGKILL)
        except (OSError, ProcessLookupError) as exc:
            self.append_log(f"[控制台] 终止进程时发生异常：{type(exc).__name__}: {exc}\n")
            if process.poll() is None:
                try:
                    process.kill()
                except OSError:
                    pass

    def _run_process(self, run_id: str) -> None:
        """执行正式 run_pipeline.py 并持续收集标准输出。"""
        try:
            command = [sys.executable, "-u", str(PIPELINE_SCRIPT)]
            creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
            if os.name == "nt":
                creation_flags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            # 强制子进程输出 UTF-8，确保中文阶段日志能被前端稳定解析。
            process_environment = os.environ.copy()
            process_environment["PYTHONIOENCODING"] = "utf-8"
            process_environment["PYTHONUTF8"] = "1"
            process = subprocess.Popen(
                command,
                cwd=str(PIPELINE_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                creationflags=creation_flags,
                env=process_environment,
                start_new_session=os.name != "nt",
            )
            with self.lock:
                self.process = process
                should_stop = self.run_id == run_id and self.stop_requested

            if should_stop:
                self._terminate_process_tree(process)

            assert process.stdout is not None
            for line in process.stdout:
                self.append_log(line)

            return_code = process.wait()
            with self.lock:
                if self.run_id != run_id:
                    return
                self.return_code = return_code
                if self.stop_requested:
                    self.status = "stopped"
                    self.log_text += "[控制台] 流水线进程树已手动停止。\n"
                else:
                    self.status = "succeeded" if return_code == 0 else "failed"
                self.finished_at = datetime.now().isoformat(timespec="seconds")
                self.process = None
                self._persist_history_locked()
        except Exception as exc:
            with self.lock:
                manually_stopped = self.run_id == run_id and self.stop_requested
                if manually_stopped:
                    self.log_text += "[控制台] 流水线已手动停止。\n"
                    self.status = "stopped"
                else:
                    self.error = f"{type(exc).__name__}: {exc}"
                    self.log_text += "\n[控制台后端异常]\n" + traceback.format_exc()
                    self.status = "failed"
                self.return_code = -1
                self.finished_at = datetime.now().isoformat(timespec="seconds")
                self.process = None
                self._persist_history_locked()


PIPELINE_STATE = PipelineState()


class LakeConsoleHandler(SimpleHTTPRequestHandler):
    """统一提供静态页面和本地 JSON API。"""

    server_version = "LakeIngestionConsole/1.0"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, directory=str(UI_ROOT), **kwargs)

    def log_message(self, format: str, *args: Any) -> None:
        """输出简洁访问日志。"""
        sys.stdout.write(
            f"[{self.log_date_time_string()}] "
            f"{self.address_string()} {format % args}\n"
        )

    def send_json(
        self,
        payload: Any,
        status: HTTPStatus = HTTPStatus.OK,
    ) -> None:
        """发送 UTF-8 JSON 响应。"""
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status.value)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def read_json(self) -> dict[str, Any]:
        """读取并校验 JSON 请求体。"""
        raw_length = self.headers.get("Content-Length", "0")
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise ValueError("Content-Length 不合法") from exc

        if length <= 0:
            return {}
        if length > MAX_BODY_BYTES:
            raise ValueError("请求体超过 2 MiB 限制")

        raw_body = self.rfile.read(length)
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("请求体必须是 UTF-8 JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("JSON 根节点必须是对象")
        return payload

    def ensure_local_agency_request(self, require_json: bool = False) -> bool:
        """限制 Agent API 为本机同源请求，避免局域网匿名触发 Codex 执行。"""
        try:
            if not ipaddress.ip_address(self.client_address[0]).is_loopback:
                self.send_json({"error": "Agent 编排接口仅允许本机访问"}, HTTPStatus.FORBIDDEN)
                return False
        except ValueError:
            self.send_json({"error": "无法识别客户端地址"}, HTTPStatus.FORBIDDEN)
            return False

        host_header = self.headers.get("Host", "")
        host_name = urlparse("//" + host_header).hostname
        if host_name not in {"127.0.0.1", "localhost", "::1"}:
            self.send_json({"error": "Agent 编排接口拒绝非本机 Host"}, HTTPStatus.FORBIDDEN)
            return False

        origin = self.headers.get("Origin")
        if origin and urlparse(origin).netloc.lower() != host_header.lower():
            self.send_json({"error": "Agent 编排接口拒绝跨站请求"}, HTTPStatus.FORBIDDEN)
            return False

        if require_json and not self.headers.get("Content-Type", "").lower().startswith("application/json"):
            self.send_json({"error": "请求必须使用 application/json"}, HTTPStatus.UNSUPPORTED_MEDIA_TYPE)
            return False
        return True

    def do_GET(self) -> None:
        """处理健康检查、配置读取和状态查询。"""
        path = urlparse(self.path).path
        if path == "/api/agency/providers":
            if self.ensure_local_agency_request():
                self.send_json(agency_providers.list_providers())
            return
        if path == "/api/agency/experts":
            if self.ensure_local_agency_request():
                experts = agency_orchestrator.list_public_experts()
                self.send_json({"experts": experts, "count": len(experts)})
            return
        agency_expert_prefix = "/api/agency/experts/"
        agency_prompt_suffix = "/prompt"
        if path.startswith(agency_expert_prefix) and path.endswith(agency_prompt_suffix):
            if not self.ensure_local_agency_request():
                return
            expert_id = path[len(agency_expert_prefix):-len(agency_prompt_suffix)]
            try:
                self.send_json(
                    {
                        "id": expert_id,
                        "prompt": agency_orchestrator.get_expert_prompt(expert_id),
                    }
                )
            except agency_orchestrator.AgencyOrchestratorError as exc:
                self.send_json({"error": str(exc)}, HTTPStatus.NOT_FOUND)
            return
        if path == "/api/agency/tasks":
            if self.ensure_local_agency_request():
                self.send_json({"tasks": agency_orchestrator.AGENCY_STATE.list()})
            return
        agency_task_prefix = "/api/agency/tasks/"
        if path.startswith(agency_task_prefix):
            if not self.ensure_local_agency_request():
                return
            task_id = path[len(agency_task_prefix):]
            task = agency_orchestrator.AGENCY_STATE.get(task_id)
            if task is None:
                self.send_json({"error": "Agent 任务不存在"}, HTTPStatus.NOT_FOUND)
            else:
                self.send_json(task)
            return
        if path == "/api/health":
            self.send_json(
                {
                    "ok": PIPELINE_SCRIPT.exists(),
                    "pipelineRoot": str(PIPELINE_ROOT),
                    "pipelineScript": str(PIPELINE_SCRIPT),
                    "python": sys.executable,
                }
            )
            return
        if path == "/api/tasks":
            self.send_json({"text": read_text(RESOURCE_FILE)})
            return
        if path == "/api/catalog":
            try:
                systems, _ = load_catalog()
                self.send_json({"systems": systems})
            except RuntimeError as exc:
                self.send_json(
                    {"error": str(exc)},
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                )
            return
        if path == "/api/source-management":
            try:
                self.send_json(load_source_management())
            except RuntimeError as exc:
                self.send_json(
                    {"error": str(exc)},
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                )
            return
        if path == "/api/mapping":
            self.send_json(
                {
                    "system": read_text(SYSTEM_MAPPING_FILE),
                    "script": read_text(SCRIPT_MAPPING_FILE),
                    "clob": read_text(CLOB_WHITELIST_FILE),
                }
            )
            return
        if path == "/api/pipeline/status":
            self.send_json(PIPELINE_STATE.snapshot())
            return
        if path == "/api/pipeline/history":
            current_run_id = PIPELINE_STATE.run_id
            records = [
                {**item, "status": "interrupted"}
                if item.get("status") in ACTIVE_PIPELINE_STATUSES
                and item.get("runId") != current_run_id
                else item
                for item in load_pipeline_history_index()
            ]
            if current_run_id is not None:
                current = PIPELINE_STATE.history_summary()
                records = [
                    item
                    for item in records
                    if item.get("runId") != current.get("runId")
                ]
                records.insert(0, current)
            self.send_json({"records": records[:MAX_HISTORY_RECORDS]})
            return
        history_prefix = "/api/pipeline/history/"
        if path.startswith(history_prefix):
            run_id = path[len(history_prefix):]
            if PIPELINE_STATE.run_id == run_id:
                record = PIPELINE_STATE.history_record()
            else:
                record = load_pipeline_history(run_id)
                if record is not None and record.get("status") in ACTIVE_PIPELINE_STATUSES:
                    record = mark_pipeline_history_unknown(record)
            if record is None:
                self.send_json({"error": "运行记录不存在"}, HTTPStatus.NOT_FOUND)
            else:
                self.send_json(record)
            return
        if path == "/api/rerun/environments":
            self.send_json(starrocks_rerun.list_environments())
            return
        if path == "/api/rerun/history":
            self.send_json(starrocks_rerun.list_history())
            return
        rerun_history_prefix = "/api/rerun/history/"
        if path.startswith(rerun_history_prefix):
            operation_id = path[len(rerun_history_prefix):]
            operation = starrocks_rerun.get_history(operation_id)
            if operation is None:
                self.send_json({"error": "数据重跑记录不存在"}, HTTPStatus.NOT_FOUND)
            else:
                self.send_json(operation)
            return
        rerun_status_prefix = "/api/rerun/status/"
        if path.startswith(rerun_status_prefix):
            operation_id = path[len(rerun_status_prefix):]
            operation = starrocks_rerun.get_operation(operation_id)
            if operation is None:
                self.send_json({"error": "数据重跑记录不存在"}, HTTPStatus.NOT_FOUND)
            else:
                self.send_json(operation)
            return
        if path == "/api/seatunnel/nodes":
            try:
                self.send_json(seatunnel_manager.list_nodes())
            except seatunnel_manager.SeaTunnelError as exc:
                self.send_json({"error": str(exc)}, HTTPStatus.BAD_GATEWAY)
            return
        if path == "/api/seatunnel/jobs":
            self.send_json(seatunnel_manager.list_jobs())
            return
        seatunnel_logs_prefix = "/api/seatunnel/logs/"
        if path.startswith(seatunnel_logs_prefix):
            operation_id = path[len(seatunnel_logs_prefix):]
            operation = seatunnel_manager.get_operation(operation_id)
            if operation is None:
                self.send_json({"error": "操作记录不存在"}, HTTPStatus.NOT_FOUND)
            else:
                self.send_json(operation)
            return
        seatunnel_jobs_prefix = "/api/seatunnel/jobs/"
        if path.startswith(seatunnel_jobs_prefix):
            rest = path[len(seatunnel_jobs_prefix):]
            if rest.endswith("/config"):
                name = rest[: -len("/config")]
                try:
                    content = seatunnel_manager.read_running_conf(name)
                    self.send_json(
                        {
                            "name": name,
                            "configFile": f"{name}.conf",
                            "mode": seatunnel_manager.extract_job_mode(content),
                            "content": content,
                        }
                    )
                except ValueError as exc:
                    self.send_json({"error": str(exc)}, HTTPStatus.NOT_FOUND)
                except seatunnel_manager.SeaTunnelError as exc:
                    self.send_json({"error": str(exc)}, HTTPStatus.BAD_GATEWAY)
                return
        super().do_GET()

    def do_POST(self) -> None:
        """处理配置保存和真实流水线启动。"""
        path = urlparse(self.path).path
        try:
            if path.startswith("/api/agency/") and not self.ensure_local_agency_request(require_json=True):
                return
            payload = self.read_json()
            if path == "/api/agency/providers/configure":
                self.send_json(
                    agency_providers.configure_cloud_provider(
                        payload.get("providerId"),
                        payload.get("apiKey", ""),
                        payload.get("model"),
                    )
                )
                return
            if path == "/api/agency/providers/current":
                self.send_json(agency_providers.set_current_provider(payload.get("providerId")))
                return
            if path == "/api/agency/providers/clear":
                self.send_json(agency_providers.clear_cloud_key(payload.get("providerId")))
                return
            if path == "/api/agency/tasks":
                task = agency_orchestrator.AGENCY_STATE.create(
                    payload.get("description"),
                    payload.get("expertIds"),
                )
                self.send_json(task, HTTPStatus.ACCEPTED)
                return
            agency_cancel_suffix = "/cancel"
            if path.startswith("/api/agency/tasks/") and path.endswith(agency_cancel_suffix):
                task_id = path[len("/api/agency/tasks/"):-len(agency_cancel_suffix)]
                task = agency_orchestrator.AGENCY_STATE.cancel(task_id)
                self.send_json(task, HTTPStatus.ACCEPTED)
                return
            if path == "/api/tasks":
                self.handle_save_tasks(payload)
                return
            if path == "/api/mapping":
                self.handle_save_mapping(payload)
                return
            if path == "/api/catalog/tables":
                self.handle_catalog_tables(payload)
                return
            if path == "/api/rerun/tables":
                self.handle_rerun_tables(payload)
                return
            if path == "/api/rerun/run":
                self.handle_rerun_run(payload)
                return
            if path == "/api/source-management/systems":
                self.handle_create_managed_system(payload)
                return
            if path == "/api/source-management/systems/delete":
                self.handle_delete_managed_system(payload)
                return
            if path == "/api/source-management/sources":
                self.handle_create_managed_source(payload)
                return
            if path == "/api/source-management/sources/update":
                self.handle_update_managed_source(payload)
                return
            if path == "/api/source-management/sources/delete":
                self.handle_delete_managed_source(payload)
                return
            if path == "/api/source-management/sources/validate":
                self.handle_validate_managed_source(payload)
                return
            if path == "/api/pipeline/run":
                self.handle_run_pipeline(payload)
                return
            if path == "/api/pipeline/stop":
                self.handle_stop_pipeline(payload)
                return
            if path == "/api/seatunnel/jobs/start":
                self.handle_seatunnel_start(payload)
                return
            if path == "/api/seatunnel/jobs/stop":
                self.handle_seatunnel_stop(payload)
                return
            if path == "/api/seatunnel/jobs/restart":
                self.handle_seatunnel_restart(payload)
                return
            self.send_json({"error": "接口不存在"}, HTTPStatus.NOT_FOUND)
        except seatunnel_manager.SeaTunnelError as exc:
            self.send_json({"error": str(exc)}, HTTPStatus.BAD_GATEWAY)
        except starrocks_rerun.StarRocksRerunError as exc:
            self.send_json({"error": str(exc)}, HTTPStatus.CONFLICT)
        except agency_orchestrator.AgencyOrchestratorError as exc:
            self.send_json({"error": str(exc)}, HTTPStatus.CONFLICT)
        except agency_providers.AgencyProviderError as exc:
            self.send_json({"error": str(exc)}, HTTPStatus.CONFLICT)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
        except RuntimeError as exc:
            self.send_json({"error": str(exc)}, HTTPStatus.CONFLICT)
        except Exception as exc:
            self.send_json(
                {"error": f"服务器异常：{type(exc).__name__}: {exc}"},
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    def ensure_source_management_writable(self) -> None:
        """流水线活动期间阻止 managed 注册表写入。"""
        if PIPELINE_STATE.status in ACTIVE_PIPELINE_STATUSES:
            raise RuntimeError("流水线运行或停止期间禁止修改数据源")

    def handle_create_managed_system(self, payload: dict[str, Any]) -> None:
        """新增 managed 业务系统。"""
        system_id = require_managed_id(payload.get("id"), "id")
        label = require_nonempty_text(payload.get("label"), "label")
        with PIPELINE_STATE.lock:
            self.ensure_source_management_writable()
            with MANAGED_SOURCE_LOCK:
                registry = load_managed_source_registry()
                legacy_systems, _, _ = load_legacy_sources()
                existing_ids = {
                    system["id"]
                    for system in legacy_systems + registry["systems"]
                }
                if system_id in existing_ids:
                    raise ValueError("系统 id 已存在")
                system = {"id": system_id, "label": label}
                registry["systems"].append(system)
                save_managed_source_registry(registry)
        clear_catalog_cache()
        self.send_json(
            {
                "ok": True,
                "system": {
                    **system,
                    "managed": True,
                    "readOnly": False,
                },
            },
            HTTPStatus.CREATED,
        )

    def handle_delete_managed_system(self, payload: dict[str, Any]) -> None:
        """删除没有 managed 数据源的 managed 系统。"""
        system_id = require_managed_id(payload.get("systemId"), "systemId")
        with PIPELINE_STATE.lock:
            self.ensure_source_management_writable()
            with MANAGED_SOURCE_LOCK:
                registry = load_managed_source_registry()
                system = next(
                    (
                        item
                        for item in registry["systems"]
                        if item.get("id") == system_id
                    ),
                    None,
                )
                if system is None:
                    raise ValueError("managed 系统不存在")
                if any(
                    source.get("systemId") == system_id
                    for source in registry["sources"]
                ):
                    raise RuntimeError("系统仍包含 managed 数据源，不能删除")
                registry["systems"].remove(system)
                save_managed_source_registry(registry)
        clear_catalog_cache()
        self.send_json({"ok": True, "systemId": system_id})

    def handle_create_managed_source(self, payload: dict[str, Any]) -> None:
        """新增带 DPAPI 凭据的 managed 数据源。"""
        password = payload.get("password")
        if not isinstance(password, str) or not password:
            raise ValueError("创建 managed 数据源时 password 必填")
        with PIPELINE_STATE.lock:
            self.ensure_source_management_writable()
            with MANAGED_SOURCE_LOCK:
                registry = load_managed_source_registry()
                source = normalize_managed_source(payload, registry)
                _, legacy_sources, _ = load_legacy_sources()
                existing_ids = {
                    item["id"]
                    for item in legacy_sources + registry["sources"]
                }
                if source["id"] in existing_ids:
                    raise ValueError("数据源 id 已存在")
                source["passwordEncrypted"] = encrypt_managed_password(password)
                registry["sources"].append(source)
                save_managed_source_registry(registry)
        clear_catalog_cache()
        self.send_json(
            {"ok": True, "source": managed_source_public(source)},
            HTTPStatus.CREATED,
        )

    def handle_update_managed_source(self, payload: dict[str, Any]) -> None:
        """更新 managed 数据源；未提交密码时保留原凭据。"""
        source_id = require_managed_id(
            payload.get("sourceId", payload.get("id")),
            "sourceId",
        )
        with PIPELINE_STATE.lock:
            self.ensure_source_management_writable()
            with MANAGED_SOURCE_LOCK:
                registry = load_managed_source_registry()
                existing = find_managed_source(registry, source_id)
                if existing is None:
                    raise ValueError("managed 数据源不存在")
                source = normalize_managed_source(payload, registry, existing)
                if "password" in payload:
                    password = payload["password"]
                    if not isinstance(password, str) or not password:
                        raise ValueError("password 提交时必须是非空字符串")
                    source["passwordEncrypted"] = encrypt_managed_password(password)
                index = registry["sources"].index(existing)
                registry["sources"][index] = source
                save_managed_source_registry(registry)
        clear_catalog_cache()
        self.send_json({"ok": True, "source": managed_source_public(source)})

    def handle_delete_managed_source(self, payload: dict[str, Any]) -> None:
        """确认 ID、引用和运行状态后删除 managed 数据源。"""
        source_id = require_managed_id(payload.get("sourceId"), "sourceId")
        if payload.get("confirmSourceId") != source_id:
            raise ValueError("confirmSourceId 必须与 sourceId 完全一致")
        with PIPELINE_STATE.lock:
            self.ensure_source_management_writable()
            if resource_references_source(source_id):
                raise RuntimeError("resource.text 仍引用该数据源，不能删除")
            with MANAGED_SOURCE_LOCK:
                registry = load_managed_source_registry()
                source = find_managed_source(registry, source_id)
                if source is None:
                    raise ValueError("managed 数据源不存在")
                registry["sources"].remove(source)
                save_managed_source_registry(registry)
        clear_catalog_cache()
        self.send_json({"ok": True, "sourceId": source_id})

    def handle_validate_managed_source(self, payload: dict[str, Any]) -> None:
        """使用保存的凭据验证数据源，并持久化脱敏结果。"""
        source_id = require_managed_id(payload.get("sourceId"), "sourceId")
        with PIPELINE_STATE.lock:
            self.ensure_source_management_writable()
            with MANAGED_SOURCE_LOCK:
                registry = load_managed_source_registry()
                current = find_managed_source(registry, source_id)
                if current is None:
                    raise ValueError("managed 数据源不存在")
                source = dict(current)

        try:
            validate_managed_source_connection(source)
            status = "valid"
            message = "连接及数据库或 Schema 验证通过"
        except Exception as exc:
            status = "invalid"
            message = managed_validation_error(exc)
        checked_at = datetime.now().isoformat(timespec="seconds")

        with PIPELINE_STATE.lock:
            self.ensure_source_management_writable()
            with MANAGED_SOURCE_LOCK:
                registry = load_managed_source_registry()
                current = find_managed_source(registry, source_id)
                if current is None:
                    raise RuntimeError("验证期间数据源已被删除")
                if current != source:
                    raise RuntimeError("验证期间数据源配置已变化，请重新验证")
                current["validationStatus"] = status
                current["message"] = message
                current["checkedAt"] = checked_at
                save_managed_source_registry(registry)
                public_source = managed_source_public(current)
        clear_catalog_cache()
        self.send_json(
            {
                "ok": status == "valid",
                "validationStatus": status,
                "message": message,
                "checkedAt": checked_at,
                "source": public_source,
            }
        )

    def handle_save_tasks(self, payload: dict[str, Any]) -> None:
        """校验并写入正式 resource.text。"""
        text = payload.get("text")
        if not isinstance(text, str):
            raise ValueError("text 必须是字符串")

        _, errors = parse_task_lines(text)
        # 允许用户主动清空文件；非空任务必须全部通过格式校验。
        if text.strip() and errors:
            raise ValueError("；".join(errors))
        if PIPELINE_STATE.status in ACTIVE_PIPELINE_STATUSES:
            raise RuntimeError("流水线运行或停止期间禁止修改任务列表")

        write_text_atomic(RESOURCE_FILE, text)
        self.send_json({"ok": True, "text": read_text(RESOURCE_FILE)})

    def handle_save_mapping(self, payload: dict[str, Any]) -> None:
        """写入正式系统映射、脚本映射和 CLOB 白名单。"""
        system = payload.get("system")
        script = payload.get("script")
        clob = payload.get("clob")
        if (
            not isinstance(system, str)
            or not isinstance(script, str)
            or not isinstance(clob, str)
        ):
            raise ValueError("system、script、clob 必须都是字符串")
        if not system.strip() or not script.strip():
            raise ValueError("系统映射和脚本映射不能为空")
        clob_errors = validate_clob_whitelist(clob)
        if clob_errors:
            raise ValueError("；".join(clob_errors))
        if PIPELINE_STATE.status in ACTIVE_PIPELINE_STATUSES:
            raise RuntimeError("流水线运行或停止期间禁止修改映射配置")

        write_text_atomic(SYSTEM_MAPPING_FILE, system)
        write_text_atomic(SCRIPT_MAPPING_FILE, script)
        write_text_atomic(CLOB_WHITELIST_FILE, clob)
        self.send_json({"ok": True})

    def handle_catalog_tables(self, payload: dict[str, Any]) -> None:
        """查询白名单数据源的表目录，单库失败不影响其他目标。"""
        _, source_configs = load_catalog()
        selected, terms, limit = parse_catalog_request(payload, source_configs)
        rows: list[tuple[str, str, str]] = []
        errors: list[dict[str, str]] = []
        for source_id, database, config in selected:
            try:
                tables = query_catalog_tables(source_id, database, config)
                rows.extend((source_id, database, table) for table in tables)
            except Exception as exc:
                errors.append(
                    {
                        "sourceId": source_id,
                        "database": database,
                        "error": f"{type(exc).__name__}: 表目录查询失败",
                    }
                )
        self.send_json(
            {
                "items": filter_catalog_items(rows, terms, limit),
                "errors": errors,
            }
        )

    def handle_rerun_tables(self, payload: dict[str, Any]) -> None:
        """查询测试或生产 StarRocks 中的真实 ODS 表。"""
        result = starrocks_rerun.search_tables(
            payload.get("environment"),
            payload.get("query", ""),
            payload.get("limit", 300),
        )
        self.send_json(result)

    def handle_rerun_run(self, payload: dict[str, Any]) -> None:
        """启动独立的 StarRocks 数据重跑，不调用 run_pipeline.py。"""
        result = starrocks_rerun.start_rerun(payload)
        self.send_json(result, HTTPStatus.ACCEPTED)

    def handle_run_pipeline(self, payload: dict[str, Any]) -> None:
        """写入任务并启动真实流水线。

        confirmed 必须严格为 true，防止误调用接口操作生产环境。
        """
        if payload.get("confirmed") is not True:
            raise ValueError("启动真实流水线必须显式确认")

        text = payload.get("text")
        if not isinstance(text, str):
            raise ValueError("text 必须是字符串")

        tasks, errors = parse_task_lines(text)
        if errors:
            raise ValueError("；".join(errors))
        if not PIPELINE_SCRIPT.exists():
            raise RuntimeError(f"正式流水线脚本不存在：{PIPELINE_SCRIPT}")

        # 持锁完成“运行检查、任务写入、启动登记”，避免并发请求相互覆盖。
        with PIPELINE_STATE.lock:
            if PIPELINE_STATE.status in ACTIVE_PIPELINE_STATUSES:
                raise RuntimeError("已有流水线正在运行或停止中，请等待当前任务结束")
            write_text_atomic(RESOURCE_FILE, text)
            run_id = PIPELINE_STATE.start(tasks)

        self.send_json(
            {
                "ok": True,
                "runId": run_id,
                "status": "running",
                "message": "真实流水线已启动",
            },
            HTTPStatus.ACCEPTED,
        )

    def handle_stop_pipeline(self, payload: dict[str, Any]) -> None:
        """校验运行 ID，并请求停止当前真实流水线。"""
        if payload.get("confirmed") is not True:
            raise ValueError("停止真实流水线必须显式确认")

        run_id = payload.get("runId")
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("runId 必须是非空字符串")

        stopped_run_id = PIPELINE_STATE.stop(run_id)
        self.send_json(
            {
                "ok": True,
                "runId": stopped_run_id,
                "status": "stopping",
                "message": "已提交手动停止请求",
            },
            HTTPStatus.ACCEPTED,
        )

    def _require_seatunnel_confirmation(self, payload: dict[str, Any], action: str) -> None:
        """生产环境 SeaTunnel 启停操作必须显式确认。"""
        if payload.get("confirmed") is not True:
            raise ValueError(f"{action} SeaTunnel 任务必须显式确认")

    def handle_seatunnel_start(self, payload: dict[str, Any]) -> None:
        """全新启动一个 SeaTunnel 任务。"""
        self._require_seatunnel_confirmation(payload, "启动")
        result = seatunnel_manager.start_job(payload.get("name"))
        self.send_json(result, HTTPStatus.ACCEPTED)

    def handle_seatunnel_stop(self, payload: dict[str, Any]) -> None:
        """通过 Savepoint 停止一个 SeaTunnel 任务。"""
        self._require_seatunnel_confirmation(payload, "停止")
        result = seatunnel_manager.stop_job(payload.get("name"))
        self.send_json(result, HTTPStatus.ACCEPTED)

    def handle_seatunnel_restart(self, payload: dict[str, Any]) -> None:
        """重启一个 SeaTunnel 任务（运行中则先停止再启动）。"""
        self._require_seatunnel_confirmation(payload, "重启")
        result = seatunnel_manager.restart_job(payload.get("name"))
        self.send_json(result, HTTPStatus.ACCEPTED)


def verify_environment() -> list[str]:
    """检查真实运行所需的正式路径。"""
    required_paths = [
        PIPELINE_ROOT,
        PIPELINE_SCRIPT,
        RESOURCE_ROOT,
        SYSTEM_MAPPING_FILE,
        SCRIPT_MAPPING_FILE,
    ]
    return [f"缺少路径：{path}" for path in required_paths if not path.exists()]


def main() -> int:
    """启动本地控制台服务或执行只读环境检查。"""
    parser = argparse.ArgumentParser(description="入湖流水线本地控制台")
    parser.add_argument("--host", default="0.0.0.0", help="监听地址，默认允许局域网访问")
    parser.add_argument("--port", type=int, default=8080, help="监听端口")
    parser.add_argument("--check", action="store_true", help="只检查环境")
    args = parser.parse_args()

    issues = verify_environment()
    if issues:
        for issue in issues:
            print(issue)
        return 1

    print(f"正式流水线目录：{PIPELINE_ROOT}")
    print(f"正式流水线脚本：{PIPELINE_SCRIPT}")
    print(f"Python 解释器：{sys.executable}")
    if args.check:
        print("环境检查通过。")
        return 0

    server = ThreadingHTTPServer((args.host, args.port), LakeConsoleHandler)
    print(f"入湖控制台已启动：http://{args.host}:{args.port}")
    print("关闭窗口或按 Ctrl+C 可停止控制台服务。")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n正在停止控制台服务…")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


