"""入湖 UI 前后端一键启动脚本。

运行方式：
    python start_lake_ui.py

可选参数：
    python start_lake_ui.py --no-browser
    python start_lake_ui.py --port 8080

Python 后端 server.py 同时托管前端静态页面，因此只需启动一个后端进程，
即可同时完成前端和后端启动。
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path
from typing import Any


PROJECT_DIRECTORY = Path(__file__).resolve().parent
SERVER_SCRIPT = PROJECT_DIRECTORY / "server.py"
RUNTIME_DIRECTORY = PROJECT_DIRECTORY / ".runtime"
PID_FILE = RUNTIME_DIRECTORY / "lake-ui.pid"
STDOUT_LOG = RUNTIME_DIRECTORY / "lake-ui.stdout.log"
STDERR_LOG = RUNTIME_DIRECTORY / "lake-ui.stderr.log"


def get_lan_ipv4() -> str:
    """获取局域网 IPv4 地址，用于生成其他电脑可访问的前端 URL。"""
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # UDP connect 不会真正发送数据，只用于让系统选择当前出口网卡。
        udp_socket.connect(("8.8.8.8", 80))
        return str(udp_socket.getsockname()[0])
    except OSError:
        try:
            return socket.gethostbyname(socket.gethostname())
        except OSError:
            return "127.0.0.1"
    finally:
        udp_socket.close()


def request_health(port: int, timeout: float = 2.0) -> dict[str, Any] | None:
    """读取健康接口，确认端口上的服务确实是入湖控制台。"""
    health_url = f"http://127.0.0.1:{port}/api/health"
    try:
        with urllib.request.urlopen(health_url, timeout=timeout) as response:
            if response.status != 200:
                return None
            payload = json.loads(response.read().decode("utf-8"))
    except (
        OSError,
        urllib.error.URLError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ):
        return None

    if (
        isinstance(payload, dict)
        and payload.get("ok") is True
        and str(payload.get("pipelineScript", "")).endswith("run_pipeline.py")
    ):
        return payload
    return None


def verify_environment() -> None:
    """调用后端只读检查，启动失败时直接输出后端的具体原因。"""
    result = subprocess.run(
        [sys.executable, str(SERVER_SCRIPT), "--check"],
        cwd=str(PROJECT_DIRECTORY),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )
    if result.stdout:
        print(result.stdout, end="")
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr, file=sys.stderr, end="")
        raise RuntimeError("正式入湖环境检查失败，未启动服务。")


def start_backend(port: int) -> subprocess.Popen[bytes]:
    """以后台进程启动后端，并把输出写入项目内的运行日志。"""
    RUNTIME_DIRECTORY.mkdir(parents=True, exist_ok=True)

    creation_flags = 0
    if os.name == "nt":
        # 后端不弹出额外控制台窗口，并与启动脚本进程解耦。
        creation_flags |= getattr(subprocess, "CREATE_NO_WINDOW", 0)
        creation_flags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        creation_flags |= getattr(subprocess, "DETACHED_PROCESS", 0)

    stdout_handle = STDOUT_LOG.open("ab")
    stderr_handle = STDERR_LOG.open("ab")
    try:
        process = subprocess.Popen(
            [
                sys.executable,
                str(SERVER_SCRIPT),
                "--host",
                "0.0.0.0",
                "--port",
                str(port),
            ],
            cwd=str(PROJECT_DIRECTORY),
            stdin=subprocess.DEVNULL,
            stdout=stdout_handle,
            stderr=stderr_handle,
            creationflags=creation_flags,
            close_fds=True,
        )
    finally:
        # 子进程已经继承日志句柄，父进程无需继续持有。
        stdout_handle.close()
        stderr_handle.close()

    PID_FILE.write_text(str(process.pid), encoding="ascii")
    return process


def wait_until_ready(
    process: subprocess.Popen[bytes],
    port: int,
    timeout_seconds: float = 20.0,
) -> dict[str, Any]:
    """等待健康接口就绪，同时监测后端是否提前退出。"""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        health = request_health(port)
        if health:
            return health

        return_code = process.poll()
        if return_code is not None:
            error_text = ""
            if STDERR_LOG.exists():
                error_text = STDERR_LOG.read_text(
                    encoding="utf-8",
                    errors="replace",
                )
            raise RuntimeError(
                f"后端进程提前退出，退出码 {return_code}。\n{error_text}"
            )

        time.sleep(0.5)

    raise TimeoutError(f"等待入湖前后端启动超时，请检查日志：{STDERR_LOG}")


def main() -> int:
    """检查、启动并按需打开入湖前端页面。"""
    parser = argparse.ArgumentParser(description="启动入湖 UI 前后端")
    parser.add_argument("--port", type=int, default=8080, help="监听端口")
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="启动成功后不自动打开浏览器",
    )
    args = parser.parse_args()

    if not SERVER_SCRIPT.exists():
        print(f"未找到后端脚本：{SERVER_SCRIPT}", file=sys.stderr)
        return 1

    lan_ip = get_lan_ipv4()
    ui_url = f"http://{lan_ip}:{args.port}/"
    health = request_health(args.port)

    if health:
        print(f"入湖前后端已经运行：{ui_url}")
    else:
        print("正在检查正式入湖环境...")
        verify_environment()

        print("正在启动入湖前后端...")
        process = start_backend(args.port)
        health = wait_until_ready(process, args.port)
        print(f"入湖前后端启动成功：{ui_url}")

    print(f"正式流水线：{health['pipelineScript']}")
    if not args.no_browser:
        webbrowser.open(ui_url)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


