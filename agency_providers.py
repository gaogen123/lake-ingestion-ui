"""Agency 本地 CLI 与云端大模型提供商配置。

云端密钥只使用当前 Windows 用户的 DPAPI 加密，公开接口仅返回配置状态和掩码。
所有云端地址均为服务端固定值，客户端不能覆盖，避免将密钥发送到任意地址。
"""

from __future__ import annotations

import base64
import json
import os
import re
import shutil
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any


RUNTIME_ROOT = Path(os.environ.get("LOCALAPPDATA", Path.home())) / "LakeOps" / "agency-orchestrator"
CONFIG_FILE = RUNTIME_ROOT / "providers.json"
CONFIG_LOCK = threading.RLock()
MODEL_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,99}$")

LOCAL_PROVIDERS = [
    {"id": "codex-cli", "label": "Codex CLI", "commands": ["codex.cmd", "codex"], "description": "已登录的 OpenAI Codex，本地项目操作与多代理协作"},
    {"id": "claude-cli", "label": "Claude Code CLI", "commands": ["claude.cmd", "claude"], "description": "已登录的 Claude Code，本地项目读取与修改"},
    {"id": "gemini-cli", "label": "Gemini CLI", "commands": ["gemini.cmd", "gemini"], "description": "已登录的 Gemini CLI，本地项目协作"},
]

CLOUD_PROVIDERS = [
    {"id": "openai", "label": "OpenAI", "kind": "openai-responses", "endpoint": "https://api.openai.com/v1/responses", "model": "gpt-5", "description": "OpenAI Responses API"},
    {"id": "anthropic", "label": "Claude (Anthropic)", "kind": "anthropic", "endpoint": "https://api.anthropic.com/v1/messages", "model": "claude-sonnet-4-20250514", "description": "Anthropic Messages API"},
    {"id": "gemini", "label": "Gemini (Google)", "kind": "gemini", "endpoint": "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent", "model": "gemini-3.5-flash", "description": "Google Gemini API"},
    {"id": "deepseek", "label": "DeepSeek", "kind": "openai-compatible", "endpoint": "https://api.deepseek.com/chat/completions", "model": "deepseek-chat", "description": "DeepSeek 开放平台"},
    {"id": "xai", "label": "xAI Grok", "kind": "openai-compatible", "endpoint": "https://api.x.ai/v1/chat/completions", "model": "grok-4", "description": "xAI API"},
    {"id": "kimi", "label": "Moonshot Kimi", "kind": "openai-compatible", "endpoint": "https://api.moonshot.cn/v1/chat/completions", "model": "moonshot-v1-8k", "description": "Moonshot AI 开放平台"},
    {"id": "qwen", "label": "通义千问 Qwen", "kind": "openai-compatible", "endpoint": "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions", "model": "qwen-plus", "description": "阿里云百炼 OpenAI 兼容接口"},
    {"id": "glm", "label": "智谱 GLM", "kind": "openai-compatible", "endpoint": "https://open.bigmodel.cn/api/paas/v4/chat/completions", "model": "glm-4-flash", "description": "智谱开放平台"},
]


class AgencyProviderError(RuntimeError):
    """可安全返回前端的提供商配置或调用错误。"""


def _write_config(payload: dict[str, Any]) -> None:
    """原子写入仅包含 DPAPI 密文的配置文件。"""
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    temp_path = CONFIG_FILE.with_name(f".{CONFIG_FILE.name}.{uuid.uuid4().hex}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp_path, CONFIG_FILE)


def _load_config() -> dict[str, Any]:
    """读取内部配置；损坏文件安全回退为空配置。"""
    try:
        payload = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        payload = {}
    return {
        "currentProvider": payload.get("currentProvider") or "codex-cli",
        "cloud": payload.get("cloud") if isinstance(payload.get("cloud"), dict) else {},
    }


def _encrypt_secret(secret: str) -> str:
    """使用当前 Windows 用户 DPAPI 加密密钥，密文离开该用户后无法解密。"""
    if os.name != "nt":
        raise AgencyProviderError("云端密钥保存当前仅支持 Windows DPAPI")
    try:
        import win32crypt

        encrypted = win32crypt.CryptProtectData(
            secret.encode("utf-8"),
            "LakeOps Agency provider key",
            None,
            None,
            None,
            0,
        )
        return base64.b64encode(encrypted).decode("ascii")
    except Exception as exc:
        raise AgencyProviderError("无法使用 Windows DPAPI 加密密钥") from exc


def _decrypt_secret(encrypted_secret: str) -> str:
    """仅在服务端执行模型请求前解密，明文不会写入日志或返回浏览器。"""
    try:
        import win32crypt

        encrypted = base64.b64decode(encrypted_secret, validate=True)
        _, decrypted = win32crypt.CryptUnprotectData(encrypted, None, None, None, 0)
        return decrypted.decode("utf-8")
    except Exception as exc:
        raise AgencyProviderError("无法使用 Windows DPAPI 解密密钥") from exc


def _find_local_command(provider: dict[str, Any]) -> str | None:
    """按稳定顺序检测本地 CLI，不执行版本命令或读取登录凭据。"""
    for command in provider["commands"]:
        found = shutil.which(command)
        if found:
            return found
    return None


def list_providers() -> dict[str, Any]:
    """返回本地 CLI 检测结果、云端配置状态和当前提供商。"""
    with CONFIG_LOCK:
        config = _load_config()
    local = []
    for provider in LOCAL_PROVIDERS:
        command = _find_local_command(provider)
        local.append(
            {
                "id": provider["id"],
                "label": provider["label"],
                "type": "cli",
                "description": provider["description"],
                "available": bool(command),
                "configured": bool(command),
                "current": config["currentProvider"] == provider["id"],
            }
        )
    cloud = []
    for provider in CLOUD_PROVIDERS:
        saved = config["cloud"].get(provider["id"], {})
        cloud.append(
            {
                "id": provider["id"],
                "label": provider["label"],
                "type": "api",
                "description": provider["description"],
                "available": bool(saved.get("encryptedKey")),
                "configured": bool(saved.get("encryptedKey")),
                "current": config["currentProvider"] == provider["id"],
                "model": saved.get("model") or provider["model"],
                "keyHint": saved.get("keyHint") or "",
            }
        )
    return {"currentProvider": config["currentProvider"], "local": local, "cloud": cloud}


def configure_cloud_provider(provider_id: Any, api_key: Any, model: Any) -> dict[str, Any]:
    """保存云端模型配置；空密钥表示保留原密钥，只修改模型。"""
    provider = next((item for item in CLOUD_PROVIDERS if item["id"] == provider_id), None)
    if provider is None:
        raise AgencyProviderError("不支持该云端模型提供商")
    if not isinstance(model, str) or not MODEL_PATTERN.fullmatch(model.strip()):
        raise AgencyProviderError("模型名格式不合法")
    if not isinstance(api_key, str):
        raise AgencyProviderError("apiKey 必须是字符串")
    api_key = api_key.strip()
    if api_key and (len(api_key) < 8 or len(api_key) > 512 or "\x00" in api_key):
        raise AgencyProviderError("API Key 长度或格式不合法")

    with CONFIG_LOCK:
        config = _load_config()
        saved = dict(config["cloud"].get(provider_id, {}))
        if api_key:
            saved["encryptedKey"] = _encrypt_secret(api_key)
            saved["keyHint"] = "••••" + api_key[-4:]
        if not saved.get("encryptedKey"):
            raise AgencyProviderError("请填写 API Key")
        saved["model"] = model.strip()
        config["cloud"][provider_id] = saved
        _write_config(config)
    return list_providers()


def clear_cloud_key(provider_id: Any) -> dict[str, Any]:
    """删除指定云端提供商密钥；当前提供商被删除时回退到 Codex CLI。"""
    if not any(item["id"] == provider_id for item in CLOUD_PROVIDERS):
        raise AgencyProviderError("不支持该云端模型提供商")
    with CONFIG_LOCK:
        config = _load_config()
        config["cloud"].pop(provider_id, None)
        if config["currentProvider"] == provider_id:
            config["currentProvider"] = "codex-cli"
        _write_config(config)
    return list_providers()


def set_current_provider(provider_id: Any) -> dict[str, Any]:
    """设置执行提供商，拒绝未安装的 CLI 或尚未配置密钥的云端 API。"""
    providers = list_providers()
    available = [*providers["local"], *providers["cloud"]]
    provider = next((item for item in available if item["id"] == provider_id), None)
    if provider is None or not provider["available"]:
        raise AgencyProviderError("该提供商尚未安装或配置")
    with CONFIG_LOCK:
        config = _load_config()
        config["currentProvider"] = provider_id
        _write_config(config)
    return list_providers()


def get_current_provider() -> dict[str, Any]:
    """返回后端执行所需的当前提供商信息；云端结果包含仅限内存使用的明文密钥。"""
    with CONFIG_LOCK:
        config = _load_config()
    provider_id = config["currentProvider"]
    local_provider = next((item for item in LOCAL_PROVIDERS if item["id"] == provider_id), None)
    if local_provider:
        command = _find_local_command(local_provider)
        if not command:
            raise AgencyProviderError("当前本地 CLI 不可用")
        return {**local_provider, "type": "cli", "command": command}
    cloud_provider = next((item for item in CLOUD_PROVIDERS if item["id"] == provider_id), None)
    saved = config["cloud"].get(provider_id, {}) if cloud_provider else {}
    if not cloud_provider or not saved.get("encryptedKey"):
        raise AgencyProviderError("当前云端模型尚未配置密钥")
    return {
        **cloud_provider,
        "type": "api",
        "apiKey": _decrypt_secret(saved["encryptedKey"]),
        "model": saved.get("model") or cloud_provider["model"],
    }


def call_cloud_provider(provider: dict[str, Any], prompt: str) -> str:
    """调用固定官方端点并提取纯文本结果，错误消息不会包含密钥。"""
    headers = {"Content-Type": "application/json"}
    kind = provider["kind"]
    endpoint = provider["endpoint"]
    if kind == "openai-responses":
        headers["Authorization"] = "Bearer " + provider["apiKey"]
        payload = {"model": provider["model"], "input": prompt, "max_output_tokens": 4000}
    elif kind == "anthropic":
        headers.update({"x-api-key": provider["apiKey"], "anthropic-version": "2023-06-01"})
        payload = {"model": provider["model"], "max_tokens": 4000, "messages": [{"role": "user", "content": prompt}]}
    elif kind == "gemini":
        headers["x-goog-api-key"] = provider["apiKey"]
        endpoint = endpoint.format(model=urllib.parse.quote(provider["model"], safe="-._"))
        payload = {"contents": [{"role": "user", "parts": [{"text": prompt}]}], "generationConfig": {"maxOutputTokens": 4000}}
    else:
        headers["Authorization"] = "Bearer " + provider["apiKey"]
        payload = {"model": provider["model"], "messages": [{"role": "user", "content": prompt}], "max_tokens": 4000}

    request = urllib.request.Request(
        endpoint,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=180) as response:
            result = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise AgencyProviderError(f"云端模型请求失败，HTTP {exc.code}") from exc
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise AgencyProviderError("云端模型请求失败或响应无法解析") from exc

    if kind == "openai-responses":
        if isinstance(result.get("output_text"), str):
            return result["output_text"]
        return "\n".join(
            content.get("text", "")
            for item in result.get("output", [])
            for content in item.get("content", [])
            if content.get("type") == "output_text"
        )
    if kind == "anthropic":
        return "\n".join(item.get("text", "") for item in result.get("content", []) if item.get("type") == "text")
    if kind == "gemini":
        return "\n".join(
            part.get("text", "")
            for candidate in result.get("candidates", [])
            for part in candidate.get("content", {}).get("parts", [])
        )
    return str(((result.get("choices") or [{}])[0].get("message") or {}).get("content") or "")
