"""SeaTunnel 任务管理后端模块。

为入湖控制台提供 SeaTunnel 物理任务的列表、状态、延迟查询，以及启动 /
停止 / 重启操作。任务定义来自正式入湖项目 ``seatunnel`` 目录下的 ``.conf``
文件，运行状态与延迟来自正式 SeaTunnel 引擎（SSH 执行 ``seatunnel.sh``）
和状态库 ``seatunnel.job_state``（MySQL）。

连接信息与正式项目 ``auto_alarm/seatunnel`` 目录下的脚本保持一致，属于可信
内网运维配置。
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

# 正式入湖项目目录中的 SeaTunnel 配置文件目录，需与 server.py 的 PIPELINE_ROOT
# 指向同一个正式项目。
SEATUNNEL_ROOT = Path(r"D:\code\python\pythonProj\get_ddl\seatunnel").resolve()

# 生产连接信息从环境变量或本地忽略文件读取，禁止提交真实凭据。
LOCAL_SECRETS_FILE = Path(__file__).resolve().parent / ".runtime" / "seatunnel-secrets.json"


def _load_local_secrets() -> dict[str, Any]:
    if not LOCAL_SECRETS_FILE.exists():
        return {}
    try:
        payload = json.loads(LOCAL_SECRETS_FILE.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"无法读取本地 SeaTunnel 凭据：{LOCAL_SECRETS_FILE}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("本地 SeaTunnel 凭据必须是 JSON 对象")
    return payload


def _config_value(name: str, default: Any = None) -> Any:
    value = os.environ.get(name)
    if value is not None:
        return value
    return LOCAL_SECRETS.get(name, default)


LOCAL_SECRETS = _load_local_secrets()
SEATUNNEL_HOST = str(_config_value("SEATUNNEL_HOST", "10.50.56.234"))
SEATUNNEL_PORT = int(_config_value("SEATUNNEL_PORT", 22))
SEATUNNEL_USERNAME = str(_config_value("SEATUNNEL_USERNAME", "root"))
SEATUNNEL_PASSWORD = str(_config_value("SEATUNNEL_PASSWORD", ""))
# 注意：必须使用 apache-seatunnel-current（软链接指向当前生产版本），
# 与引擎服务端保持一致。之前硬编码 2.3.11 会导致客户端与 2.3.13 服务端
# 的 serialVersionUID 不一致，任务提交时抛 InvalidClassException。
SEATUNNEL_BIN = "/data/seatunnel/backend/apache-seatunnel-current/bin/seatunnel.sh"
SEATUNNEL_REMOTE_CONF_DIR = "/data/script"
# 引擎 Hazelcast REST 接口（用于读取正在运行的任务）。
SEATUNNEL_HAZELCAST_PORT = 5801

# SeaTunnel 任务状态数据库（MySQL）。
SEATUNNEL_MYSQL_HOST = str(_config_value("SEATUNNEL_MYSQL_HOST", "10.30.250.27"))
SEATUNNEL_MYSQL_PORT = int(_config_value("SEATUNNEL_MYSQL_PORT", 3306))
SEATUNNEL_MYSQL_USERNAME = str(_config_value("SEATUNNEL_MYSQL_USERNAME", "root"))
SEATUNNEL_MYSQL_PASSWORD = str(_config_value("SEATUNNEL_MYSQL_PASSWORD", ""))
SEATUNNEL_MYSQL_DATABASE = str(_config_value("SEATUNNEL_MYSQL_DATABASE", "aaaa"))
SEATUNNEL_JOB_STATE_TABLE = "seatunnel.job_state"

# 合法的 SeaTunnel 任务名：与 .conf 文件名一致，避免路径穿越。
JOB_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,79}$")
JOB_MODE_PATTERN = re.compile(r"job\.mode\s*=\s*\"?([A-Za-z]+)\"?")
MEMBER_ADDRESS_PATTERN = re.compile(r"^\[([^\]]+)\]:(\d+)$")

# SeaTunnel 引擎任务状态全集（来自正式 run_pipeline.py 的解析逻辑）。
SEATUNNEL_STATUSES = {
    "INITIALIZING",
    "RUNNING",
    "FAILING",
    "FAILED",
    "CANCELLING",
    "CANCELLED",
    "FINISHED",
    "SUSPENDING",
    "SUSPENDED",
    "RECONCILING",
    "DOING_SAVEPOINT",
    "SAVEPOINT_DONE",
}
# 处于这些状态的任务视为“正在运行”，可被停止或重启。
SEATUNNEL_ACTIVE_STATUSES = {
    "RUNNING",
    "INITIALIZING",
    "RECONCILING",
    "DOING_SAVEPOINT",
    "SUSPENDING",
}

# 串行化所有会操作 SeaTunnel 引擎的请求，避免并发 SSH 相互干扰。
SEATUNNEL_ACTION_LOCK = threading.RLock()

# 异步启停操作注册表：operation_id -> 操作状态（含实时日志）。
SEATUNNEL_OPERATIONS: dict[str, dict[str, Any]] = {}
SEATUNNEL_OPERATIONS_LOCK = threading.RLock()
# 操作日志最大保留字符数，避免内存无限增长。
MAX_OPERATION_LOG_CHARS = 2 * 1024 * 1024


class SeaTunnelError(RuntimeError):
    """SeaTunnel 任务管理操作失败。"""


def read_local_conf(name: str) -> str:
    """读取本地 SeaTunnel 目录中的配置文件内容（只读）。"""
    if not JOB_NAME_PATTERN.fullmatch(name):
        raise ValueError("任务名不合法")
    path = SEATUNNEL_ROOT / f"{name}.conf"
    if not path.exists():
        raise ValueError(f"本地配置文件不存在：{name}.conf")
    return path.read_text(encoding="utf-8", errors="replace")


def read_remote_conf(name: str) -> str:
    """通过 SSH 读取集群服务器上该任务实际运行的配置文件。"""
    if not JOB_NAME_PATTERN.fullmatch(name):
        raise ValueError("任务名不合法")
    try:
        ssh = _open_ssh_connection()
    except Exception as exc:
        raise SeaTunnelError(f"无法连接 SeaTunnel 引擎读取配置：{exc}") from exc
    try:
        command = f"source /etc/profile && cat {SEATUNNEL_REMOTE_CONF_DIR}/{name}.conf"
        _, stdout, _ = ssh.exec_command(command, timeout=25)
        output = stdout.read().decode("utf-8", errors="replace")
        exit_status = stdout.channel.recv_exit_status()
    except Exception as exc:
        raise SeaTunnelError(f"读取集群配置失败：{exc}") from exc
    finally:
        ssh.close()
    if exit_status != 0 or not output.strip():
        raise SeaTunnelError(f"集群配置文件不存在或读取失败：{name}.conf")
    return output


def read_running_conf(name: str) -> str:
    """读取任务当前运行的配置：优先集群远程配置，失败时回退本地配置。"""
    name = _require_job_name(name)
    try:
        return read_remote_conf(name)
    except SeaTunnelError:
        return read_local_conf(name)


def extract_job_mode(content: str) -> str | None:
    """从配置文件内容中提取 job.mode。"""
    match = JOB_MODE_PATTERN.search(content)
    return match.group(1).upper() if match else None


def _open_mysql_connection() -> Any:
    """打开 SeaTunnel 状态库连接（惰性导入，避免缺失驱动影响页面启动）。"""
    import pymysql

    return pymysql.connect(
        host=SEATUNNEL_MYSQL_HOST,
        port=SEATUNNEL_MYSQL_PORT,
        user=SEATUNNEL_MYSQL_USERNAME,
        password=SEATUNNEL_MYSQL_PASSWORD,
        database=SEATUNNEL_MYSQL_DATABASE,
        charset="utf8",
        connect_timeout=8,
        read_timeout=15,
        write_timeout=15,
    )


def query_job_state() -> dict[str, dict[str, Any]]:
    """从 MySQL ``seatunnel.job_state`` 读取任务状态、延迟与吞吐量。"""
    connection = _open_mysql_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT job_id, job_name, job_status, delay_time, "
                "source_received_count, sink_write_count, create_time, "
                "is_start_with_save_point "
                f"FROM {SEATUNNEL_JOB_STATE_TABLE}"
            )
            columns = [column[0] for column in cursor.description]
            rows: dict[str, dict[str, Any]] = {}
            for row in cursor.fetchall():
                record = dict(zip(columns, row))
                job_name = str(record.get("job_name") or "").strip()
                if job_name:
                    rows[job_name] = record
            return rows
    finally:
        connection.close()


def _extract_mode_from_env(env_options: Any) -> str | None:
    """从 Hazelcast 返回的 envOptions 中提取 job.mode。"""
    if not isinstance(env_options, dict):
        return None
    mode = env_options.get("job.mode")
    if isinstance(mode, str) and mode.strip():
        return mode.strip().upper()
    return None


def _open_ssh_connection() -> Any:
    """打开 SeaTunnel 引擎服务器 SSH 连接（惰性导入 paramiko）。"""
    import paramiko

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        SEATUNNEL_HOST,
        port=SEATUNNEL_PORT,
        username=SEATUNNEL_USERNAME,
        password=SEATUNNEL_PASSWORD,
        timeout=15,
    )
    return ssh


def _request_hazelcast_json(host: str, port: int, path: str, timeout: int = 10) -> Any:
    """请求 Hazelcast 只读 JSON 接口。"""
    import http.client

    connection = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        connection.request("GET", path, "", {"Accept": "application/json"})
        response = connection.getresponse()
        data = response.read().decode("utf-8", errors="replace")
    finally:
        connection.close()
    if response.status != 200:
        raise SeaTunnelError(f"Hazelcast 接口 {path} 返回状态码 {response.status}")
    try:
        return json.loads(data) if data.strip() else {}
    except json.JSONDecodeError as exc:
        raise SeaTunnelError(f"Hazelcast 接口 {path} 返回格式异常") from exc


def _probe_cluster_member(
    member: dict[str, Any],
    cluster_health: dict[str, Any],
) -> dict[str, Any]:
    """读取单个 Hazelcast 成员健康状态和接口响应耗时。"""
    address = str(member.get("address") or "")
    match = MEMBER_ADDRESS_PATTERN.fullmatch(address)
    base = {
        "address": address,
        "host": None,
        "port": None,
        "uuid": member.get("uuid"),
        "version": member.get("memberVersion"),
        "liteMember": bool(member.get("liteMember")),
        "localMember": bool(member.get("localMember")),
        "role": "轻量节点" if member.get("liteMember") else "数据节点",
        "status": "ONLINE",
        "responseMs": None,
        "nodeState": "MEMBER",
        "clusterState": cluster_health.get("clusterState"),
        "clusterSafe": cluster_health.get("clusterSafe"),
        "clusterSize": cluster_health.get("clusterSize"),
        "migrationQueueSize": cluster_health.get("migrationQueueSize"),
        "error": None,
    }
    if match is None:
        base["error"] = "节点地址格式异常"
        return base
    host, raw_port = match.groups()
    port = int(raw_port)
    base["host"] = host
    base["port"] = port
    # 轻量成员默认不开放健康端点，但已出现在集群成员列表中，因此视为在线。
    if base["liteMember"]:
        return base
    started = time.monotonic()
    try:
        health = _request_hazelcast_json(host, port, "/hazelcast/health", timeout=5)
        base["responseMs"] = round((time.monotonic() - started) * 1000)
        base["nodeState"] = health.get("nodeState")
        base["clusterState"] = health.get("clusterState")
        base["clusterSafe"] = health.get("clusterSafe")
        base["clusterSize"] = health.get("clusterSize")
        base["migrationQueueSize"] = health.get("migrationQueueSize")
        base["status"] = "ONLINE" if health.get("nodeState") == "ACTIVE" else "DEGRADED"
    except Exception as exc:
        base["responseMs"] = round((time.monotonic() - started) * 1000)
        base["status"] = "DEGRADED"
        base["error"] = f"健康接口不可用：{type(exc).__name__}: {exc}"
    return base


def list_nodes() -> dict[str, Any]:
    """读取集群真实成员，并并发探测每个节点健康状态。"""
    cluster = _request_hazelcast_json(
        SEATUNNEL_HOST,
        SEATUNNEL_HAZELCAST_PORT,
        "/hazelcast/rest/cluster",
    )
    members = cluster.get("members") if isinstance(cluster, dict) else None
    if not isinstance(members, list):
        raise SeaTunnelError("Hazelcast 集群成员返回格式异常")

    cluster_health = _request_hazelcast_json(
        SEATUNNEL_HOST,
        SEATUNNEL_HAZELCAST_PORT,
        "/hazelcast/health",
    )
    nodes: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(8, max(1, len(members)))) as executor:
        futures = [
            executor.submit(_probe_cluster_member, member, cluster_health)
            for member in members
        ]
        for future in as_completed(futures):
            nodes.append(future.result())
    nodes.sort(key=lambda item: (str(item.get("host") or ""), int(item.get("port") or 0)))
    online_count = sum(1 for node in nodes if node["status"] != "OFFLINE")
    return {
        "nodes": nodes,
        "summary": {
            "total": len(nodes),
            "online": online_count,
            "offline": len(nodes) - online_count,
            "degraded": sum(1 for node in nodes if node["status"] == "DEGRADED"),
            "dataMembers": sum(1 for node in nodes if not node["liteMember"]),
            "liteMembers": sum(1 for node in nodes if node["liteMember"]),
            "connectionCount": cluster.get("connectionCount"),
            "allConnectionCount": cluster.get("allConnectionCount"),
        },
        "checkedAt": datetime.now().isoformat(timespec="seconds"),
    }


def list_live_jobs() -> dict[str, dict[str, Any]]:
    """通过 Hazelcast REST 接口读取引擎中正在运行的任务。

    相比 ``seatunnel.sh -l``（当前版本存在 JSON 反序列化兼容问题），该接口
    直接返回结构化的运行任务 JSON，与正式监控脚本保持一致。
    """
    import http.client

    connection = http.client.HTTPConnection(
        f"{SEATUNNEL_HOST}:{SEATUNNEL_HAZELCAST_PORT}",
        timeout=10,
    )
    try:
        connection.request("GET", "/hazelcast/rest/maps/running-jobs", "", {"Accept": "*/*"})
        response = connection.getresponse()
        data = response.read().decode("utf-8", errors="ignore")
    finally:
        connection.close()

    if response.status != 200:
        raise SeaTunnelError(f"Hazelcast 接口返回异常状态码 {response.status}")

    items = json.loads(data)
    if not isinstance(items, list):
        raise SeaTunnelError("Hazelcast 接口返回格式异常")

    jobs: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        name = str(item.get("jobName") or "").strip()
        if not name:
            continue
        jobs[name] = {
            "jobId": item.get("jobId"),
            "status": str(item.get("jobStatus") or "").upper(),
            "mode": _extract_mode_from_env(item.get("envOptions")),
        }
    return jobs


def _stream_remote_command(command: str, on_line, timeout_seconds: int) -> None:
    """流式执行远程 SeaTunnel 命令，逐行回调 on_line（用于实时日志）。

    ``exec_command`` 返回的 stdout/stderr 以文本模式打开（makefile("r")），
    迭代时已经解码为 str，因此这里直接按 str 处理，不再调用 decode。
    """
    ssh = _open_ssh_connection()
    try:
        _, stdout, stderr = ssh.exec_command(command, timeout=timeout_seconds)
        for line in stdout:
            on_line(line)
        for line in stderr:
            on_line(line)
    finally:
        ssh.close()


def _new_operation(job_name: str, action: str) -> str:
    """登记一个异步启停操作，返回 operation_id。"""
    operation_id = uuid.uuid4().hex
    with SEATUNNEL_OPERATIONS_LOCK:
        SEATUNNEL_OPERATIONS[operation_id] = {
            "operationId": operation_id,
            "jobName": job_name,
            "action": action,
            "status": "running",
            "startedAt": datetime.now().isoformat(timespec="seconds"),
            "finishedAt": None,
            "log": "",
            "error": None,
        }
    return operation_id


def _append_operation_log(operation_id: str, text: str) -> None:
    """向操作日志缓冲区追加文本，并限制内存占用。"""
    with SEATUNNEL_OPERATIONS_LOCK:
        operation = SEATUNNEL_OPERATIONS.get(operation_id)
        if operation is None:
            return
        operation["log"] += text
        if len(operation["log"]) > MAX_OPERATION_LOG_CHARS:
            operation["log"] = (
                "[较早日志已截断]\n" + operation["log"][-MAX_OPERATION_LOG_CHARS:]
            )


def _finish_operation(operation_id: str, status: str, error: str | None = None) -> None:
    """标记操作完成或失败。"""
    with SEATUNNEL_OPERATIONS_LOCK:
        operation = SEATUNNEL_OPERATIONS.get(operation_id)
        if operation is None:
            return
        operation["status"] = status
        operation["finishedAt"] = datetime.now().isoformat(timespec="seconds")
        operation["error"] = error


def get_operation(operation_id: str) -> dict[str, Any] | None:
    """返回单个操作的状态与实时日志。"""
    with SEATUNNEL_OPERATIONS_LOCK:
        operation = SEATUNNEL_OPERATIONS.get(operation_id)
        return dict(operation) if operation is not None else None


def _ensure_no_running_operation() -> None:
    """同一时间只允许一个 SeaTunnel 启停操作在运行。"""
    with SEATUNNEL_OPERATIONS_LOCK:
        running = [
            operation
            for operation in SEATUNNEL_OPERATIONS.values()
            if operation.get("status") == "running"
        ]
    if running:
        raise SeaTunnelError("已有 SeaTunnel 启停操作正在进行，请稍后再试")


def _run_operation(operation_id: str, name: str, action: str, job_id: Any) -> None:
    """后台线程执行启停操作，并把输出流式写入操作日志。"""
    commands: list[tuple[str, str, int]] = []
    if action == "start":
        commands.append((
            f"source /etc/profile && {SEATUNNEL_BIN} -c {SEATUNNEL_REMOTE_CONF_DIR}/{name}.conf --async true",
            f"启动任务 {name}",
            180,
        ))
    elif action == "stop":
        commands.append((
            f"source /etc/profile && {SEATUNNEL_BIN} -s {job_id}",
            f"停止任务 {name}",
            300,
        ))
    elif action == "restart":
        if job_id is not None:
            commands.append((
                f"source /etc/profile && {SEATUNNEL_BIN} -s {job_id}",
                f"停止任务 {name}",
                300,
            ))
        commands.append((
            f"source /etc/profile && {SEATUNNEL_BIN} -c {SEATUNNEL_REMOTE_CONF_DIR}/{name}.conf --async true",
            f"启动任务 {name}",
            180,
        ))

    try:
        for command, label, timeout_seconds in commands:
            _append_operation_log(operation_id, f"[{label}]\n")
            _stream_remote_command(
                command,
                lambda line: _append_operation_log(operation_id, line),
                timeout_seconds,
            )
            _append_operation_log(operation_id, "\n")
    except Exception as exc:
        _append_operation_log(
            operation_id, f"\n[错误] {type(exc).__name__}: {exc}\n"
        )
        _finish_operation(operation_id, "failed", str(exc))
        return
    _finish_operation(operation_id, "succeeded")


def _resolve_running_job_id(name: str, live_jobs: dict[str, dict[str, Any]]) -> Any:
    """查找某个任务正在运行的 jobId；未运行则返回 None。"""
    live = live_jobs.get(name)
    if live and str(live.get("status") or "").upper() in SEATUNNEL_ACTIVE_STATUSES:
        return live.get("jobId")
    return None


def list_jobs() -> dict[str, Any]:
    """返回集群任务列表。

    任务名以状态库（seatunnel.job_state）和引擎（seatunnel.sh -l）的并集为准，
    而非本地 seatunnel 目录里的 .conf 文件；本地 .conf 仅用于补充 mode。
    """
    # 状态库与引擎均为可选数据源：单一来源失败不应让整个列表不可用。
    db_rows: dict[str, dict[str, Any]] = {}
    try:
        db_rows = query_job_state()
    except Exception:
        db_rows = {}

    live_jobs: dict[str, dict[str, Any]] = {}
    try:
        live_jobs = list_live_jobs()
    except Exception:
        live_jobs = {}

    names = sorted(set(db_rows.keys()) | set(live_jobs.keys()))

    jobs: list[dict[str, Any]] = []
    for name in names:
        db_record = db_rows.get(name) or {}
        live_record = live_jobs.get(name) or {}

        content = ""
        try:
            content = read_local_conf(name)
        except (ValueError, OSError, UnicodeDecodeError):
            content = ""

        status = _compose_status(name, db_record, live_record)
        jobs.append(
            {
                "name": name,
                "configFile": f"{name}.conf",
                "mode": live_record.get("mode") or extract_job_mode(content),
                "status": status,
                "statusSource": _status_source(db_record, live_record),
                "jobId": db_record.get("job_id") or live_record.get("jobId"),
                "delayMs": _coerce_int(db_record.get("delay_time")),
                "sourceReceivedCount": _coerce_int(
                    db_record.get("source_received_count")
                ),
                "sinkWriteCount": _coerce_int(db_record.get("sink_write_count")),
                "createdAt": _coerce_text(db_record.get("create_time")),
            }
        )

    return {
        "jobs": jobs,
        "total": len(jobs),
        "sources": {
            "database": bool(db_rows),
            "engine": bool(live_jobs),
        },
    }


def _status_source(
    db_record: dict[str, Any],
    live_record: dict[str, Any],
) -> str:
    """标记状态来源，便于排查状态库与引擎不一致。"""
    if live_record:
        return "engine"
    if db_record:
        return "database"
    return "none"


def _compose_status(
    name: str,
    db_record: dict[str, Any],
    live_record: dict[str, Any],
) -> str:
    """优先取引擎活跃状态，其次取状态库状态，否则标记为未运行。"""
    live_status = str(live_record.get("status") or "").upper()
    if live_status in SEATUNNEL_STATUSES:
        return live_status
    db_status = str(db_record.get("job_status") or "").upper()
    if db_status:
        return db_status
    return "NOT_RUNNING"


def _coerce_int(value: Any) -> int | None:
    """把数据库可能返回的字符串数值安全转换为整数。"""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_text(value: Any) -> str | None:
    """把数据库可能返回的非字符串值安全转换为字符串。"""
    if value is None:
        return None
    return str(value)


def _require_job_name(name: Any) -> str:
    """校验任务名并返回规范化结果。"""
    if not isinstance(name, str) or not JOB_NAME_PATTERN.fullmatch(name):
        raise ValueError("任务名必须以字母开头，且为 1-80 位字母、数字或下划线")
    return name


def start_job(name: Any) -> dict[str, Any]:
    """异步提交一个 SeaTunnel 任务（``--async true``），返回 operationId 供实时查看日志。"""
    name = _require_job_name(name)
    with SEATUNNEL_ACTION_LOCK:
        _ensure_no_running_operation()
        try:
            live_jobs = list_live_jobs()
        except Exception as exc:
            raise SeaTunnelError(f"无法连接 SeaTunnel 引擎：{exc}") from exc

        if _resolve_running_job_id(name, live_jobs) is not None:
            raise SeaTunnelError(f"任务 {name} 已在运行，请先停止后再启动")

        operation_id = _new_operation(name, "start")

    worker = threading.Thread(
        target=_run_operation,
        args=(operation_id, name, "start", None),
        daemon=True,
        name=f"st-start-{operation_id[:8]}",
    )
    worker.start()

    return {
        "ok": True,
        "operationId": operation_id,
        "jobName": name,
        "action": "start",
        "status": "running",
        "message": "任务已开始启动",
    }


def stop_job(name: Any) -> dict[str, Any]:
    """异步停止一个正在运行的 SeaTunnel 任务（Savepoint）。"""
    name = _require_job_name(name)
    with SEATUNNEL_ACTION_LOCK:
        _ensure_no_running_operation()
        try:
            live_jobs = list_live_jobs()
        except Exception as exc:
            raise SeaTunnelError(f"无法连接 SeaTunnel 引擎：{exc}") from exc

        job_id = _resolve_running_job_id(name, live_jobs)
        if job_id is None:
            raise SeaTunnelError(f"任务 {name} 当前未在运行，无需停止")

        operation_id = _new_operation(name, "stop")

    worker = threading.Thread(
        target=_run_operation,
        args=(operation_id, name, "stop", job_id),
        daemon=True,
        name=f"st-stop-{operation_id[:8]}",
    )
    worker.start()

    return {
        "ok": True,
        "operationId": operation_id,
        "jobName": name,
        "action": "stop",
        "jobId": job_id,
        "status": "running",
        "message": "任务已开始停止（Savepoint）",
    }


def restart_job(name: Any) -> dict[str, Any]:
    """异步重启一个 SeaTunnel 任务（运行中则先停再启）。"""
    name = _require_job_name(name)
    with SEATUNNEL_ACTION_LOCK:
        _ensure_no_running_operation()
        try:
            live_jobs = list_live_jobs()
        except Exception as exc:
            raise SeaTunnelError(f"无法连接 SeaTunnel 引擎：{exc}") from exc

        job_id = _resolve_running_job_id(name, live_jobs)
        operation_id = _new_operation(name, "restart")

    worker = threading.Thread(
        target=_run_operation,
        args=(operation_id, name, "restart", job_id),
        daemon=True,
        name=f"st-restart-{operation_id[:8]}",
    )
    worker.start()

    return {
        "ok": True,
        "operationId": operation_id,
        "jobName": name,
        "action": "restart",
        "status": "running",
        "message": "任务已开始重启" if job_id is not None else "任务未运行，已开始全新启动",
    }
