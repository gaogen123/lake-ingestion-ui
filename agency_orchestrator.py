"""Agency 专家发现与 Codex 多代理任务编排。

该模块只负责 Agency 功能，不修改现有入湖流水线状态。专家定义直接读取本机
``~/.agents/skills/agency-*``，任务通过已登录的 Codex CLI 在当前项目中执行。
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import threading
import time
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import agency_providers


WORKSPACE_ROOT = Path(__file__).resolve().parent
SKILLS_ROOT = Path.home() / ".agents" / "skills"
# 运行状态放到 Agent 工作区之外，避免执行中的 Agent 修改自己的后端状态。
RUNTIME_ROOT = Path(os.environ.get("LOCALAPPDATA", Path.home())) / "LakeOps" / "agency-orchestrator"
TASK_ROOT = RUNTIME_ROOT / "tasks"

MAX_DESCRIPTION_CHARS = 8_000
MAX_SELECTED_EXPERTS = 5
MAX_TASKS = 100
MAX_OUTPUT_CHARS = 1024 * 1024
TASK_TIMEOUT_SECONDS = 15 * 60
TASK_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")
EXPERT_ID_PATTERN = re.compile(r"^agency-[a-z0-9][a-z0-9-]{1,79}$")
ACTIVE_STATUSES = {"queued", "running", "cancelling"}


class AgencyOrchestratorError(RuntimeError):
    """向 HTTP 层暴露可安全展示的 Agency 业务错误。"""


def _now() -> str:
    """统一生成秒级本地时间，便于前端直接展示和排序。"""
    return datetime.now().isoformat(timespec="seconds")


def _write_json_atomic(path: Path, payload: Any) -> None:
    """原子持久化任务状态，避免服务退出时留下半份 JSON。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(temp_path, path)


def _front_matter_value(text: str, key: str) -> str:
    """读取 SKILL.md 顶部 YAML 中的简单字符串字段，不引入 YAML 依赖。"""
    if not text.startswith("---"):
        return ""
    end = text.find("\n---", 3)
    if end < 0:
        return ""
    match = re.search(
        rf"(?m)^{re.escape(key)}:\s*(?:[>|]\s*)?(.+?)\s*$",
        text[3:end],
    )
    return match.group(1).strip().strip('"\'') if match else ""


def _expert_category(expert_id: str) -> str:
    """根据稳定的技能目录名生成展示分类；分类只用于筛选，不参与执行权限。"""
    categories = [
        ("工程研发", ("developer", "engineer", "architect", "devops", "database", "api", "security", "tester", "qa", "sre", "technical")),
        ("数据智能", ("data", "analytics", "statistic", "ai-", "llm", "rag", "search", "gis", "geo")),
        ("产品设计", ("product", "ui-", "ux-", "design", "visual", "brand", "game", "xr-")),
        ("市场增长", ("marketing", "seo", "social", "content", "growth", "sales", "media", "podcast", "tiktok", "weibo", "wechat")),
        ("业务运营", ("operations", "finance", "account", "customer", "legal", "compliance", "project", "manager", "strategy", "hr-")),
    ]
    for category, markers in categories:
        if any(marker in expert_id for marker in markers):
            return category
    return "专业服务"


def list_experts() -> list[dict[str, Any]]:
    """动态加载全部 Agency 专家元数据，保证技能安装或更新后无需重复迁移。"""
    experts: list[dict[str, Any]] = []
    if not SKILLS_ROOT.is_dir():
        return experts

    resolved_root = SKILLS_ROOT.resolve()
    for skill_dir in sorted(resolved_root.glob("agency-*")):
        skill_file = (skill_dir / "SKILL.md").resolve()
        try:
            skill_file.relative_to(resolved_root)
        except ValueError:
            # 拒绝指向专家根目录之外的符号链接或目录联接。
            continue
        if not skill_dir.is_dir() or not skill_file.is_file():
            continue
        try:
            # 元数据只存在于文件头部，限制读取量可避免异常技能文件拖慢专家库。
            with skill_file.open("r", encoding="utf-8-sig") as stream:
                text = stream.read(32 * 1024)
        except (OSError, UnicodeDecodeError):
            continue
        expert_id = skill_dir.name
        name = _front_matter_value(text, "name") or expert_id
        description = _front_matter_value(text, "description")
        if not EXPERT_ID_PATTERN.fullmatch(expert_id) or name != expert_id or not description:
            continue
        experts.append(
            {
                "id": expert_id,
                "name": name,
                "description": description,
                "category": _expert_category(expert_id),
                "tags": expert_id.removeprefix("agency-").split("-"),
                "skillPath": str(skill_file),
            }
        )
    return experts


def list_public_experts() -> list[dict[str, Any]]:
    """返回可公开给浏览器的专家字段，不泄露本机绝对文件路径。"""
    public_experts = []
    for expert in list_experts():
        public_expert = {key: value for key, value in expert.items() if key != "skillPath"}
        public_expert["name"] = expert["id"].removeprefix("agency-").replace("-", " ").title()
        public_experts.append(public_expert)
    return public_experts


def get_expert_prompt(expert_id: Any) -> str:
    """按专家白名单读取完整提示词，供本机专家库查看或复制。"""
    if not isinstance(expert_id, str) or not EXPERT_ID_PATTERN.fullmatch(expert_id):
        raise AgencyOrchestratorError("专家 ID 不合法")
    expert = next((item for item in list_experts() if item["id"] == expert_id), None)
    if expert is None:
        raise AgencyOrchestratorError("专家不存在或已被移除")
    try:
        return Path(expert["skillPath"]).read_text(encoding="utf-8-sig")
    except (OSError, UnicodeDecodeError) as exc:
        raise AgencyOrchestratorError("无法读取专家提示词") from exc


def _select_experts(description: str, requested_ids: list[str]) -> list[dict[str, Any]]:
    """校验显式选择；未选择时根据任务关键词挑选少量专家并保留总编排与 QA。"""
    experts = list_experts()
    expert_by_id = {expert["id"]: expert for expert in experts}
    if not experts:
        raise AgencyOrchestratorError("未发现 agency-* 专家技能")

    invalid_ids = [expert_id for expert_id in requested_ids if expert_id not in expert_by_id]
    if invalid_ids:
        raise AgencyOrchestratorError("专家不存在或已被移除：" + "、".join(invalid_ids))

    selected_ids = list(dict.fromkeys(requested_ids))
    if not selected_ids:
        # 英文技术词可直接与技能 ID/描述匹配；中文任务使用常见领域词映射。
        normalized = description.lower()
        keyword_aliases = {
            "前端": "frontend", "后端": "backend", "接口": "api", "数据库": "database",
            "测试": "test", "安全": "security", "数据": "data", "分析": "analytics",
            "产品": "product", "设计": "design", "营销": "marketing", "销售": "sales",
            "内容": "content", "运维": "devops", "部署": "devops", "工作流": "workflow",
        }
        search_terms = set(re.findall(r"[a-z][a-z0-9-]{2,}", normalized))
        search_terms.update(
            alias for word, alias in keyword_aliases.items() if word in description
        )
        ranked: list[tuple[int, str]] = []
        for expert in experts:
            haystack = f"{expert['id']} {expert['description']}".lower()
            score = sum(1 for term in search_terms if term in haystack)
            if score:
                ranked.append((score, expert["id"]))
        ranked.sort(key=lambda item: (-item[0], item[1]))
        selected_ids = [expert_id for _, expert_id in ranked[:3]]
        if not selected_ids and "agency-senior-developer" in expert_by_id:
            selected_ids = ["agency-senior-developer"]

    # 总编排器负责拆解和交接，Reality Checker 负责最终质量门；重复项只保留一次。
    pipeline_ids = ["agency-agents-orchestrator", *selected_ids, "agency-reality-checker"]
    pipeline_ids = [
        expert_id for expert_id in dict.fromkeys(pipeline_ids) if expert_id in expert_by_id
    ]
    return [expert_by_id[expert_id] for expert_id in pipeline_ids]


def _find_codex_command() -> str:
    """定位可执行的 Codex CLI；Windows 优先使用 npm 生成的 cmd 包装器。"""
    command = shutil.which("codex.cmd") or shutil.which("codex")
    if not command:
        raise AgencyOrchestratorError("未找到 Codex CLI，请先安装并完成登录")
    return command


def _build_codex_command(output_path: Path) -> list[str]:
    """构造固定 CLI 参数；用户任务永远不进入命令行，只经标准输入传递。"""
    return [
        _find_codex_command(),
        "exec",
        "--enable", "multi_agent",
        "--disable", "hooks",
        "--sandbox", "workspace-write",
        "--ephemeral",
        "--ignore-user-config",
        "--skip-git-repo-check",
        "--color", "never",
        "--json",
        "--cd", str(WORKSPACE_ROOT),
        "--output-last-message", str(output_path),
        "-",
    ]


def _build_prompt(task: dict[str, Any], experts: list[dict[str, Any]]) -> str:
    """构造主编排 Agent 的完整上下文，并要求其读取所选专家的真实技能定义。"""
    expert_lines = "\n".join(
        f"- {expert['id']}：{expert['skillPath']}" for expert in experts
    )
    return f"""你正在执行 LakeOps Agency Orchestrator 提交的真实任务。

用户任务：
{task['description']}

必须参与的专家技能：
{expert_lines}

执行要求：
1. 首先完整读取 agency-agents-orchestrator 的 SKILL.md，并由它负责拆解、分工和质量门。
2. 完整读取以上每个专家的 SKILL.md；按任务需要使用多代理协作，将具体子任务交给对应专家。
3. 任务涉及本项目代码时，在当前工作区内直接完成最小修改并验证；不得破坏现有入湖功能。
4. 任务不涉及代码时，不要修改项目文件，直接产出用户要求的结果。
5. 所有代码注释与最终报告使用中文；每个执行阶段最多重试三次。
6. 最终回答必须说明参与专家、完成结果、验证证据以及未完成或被阻塞的事项。
"""


class AgencyTaskState:
    """线程安全的单任务执行器，负责持久化、调用 Codex、查询与取消。"""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.tasks: dict[str, dict[str, Any]] = {}
        self.processes: dict[str, subprocess.Popen[str]] = {}
        self._load_history()

    def _task_path(self, task_id: str) -> Path:
        return TASK_ROOT / f"{task_id}.json"

    def _load_history(self) -> None:
        """恢复最近任务；服务中断时仍处于活动态的任务标记为已中断。"""
        if not TASK_ROOT.is_dir():
            return
        records: list[dict[str, Any]] = []
        for path in TASK_ROOT.glob("*.json"):
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(record, dict) and TASK_ID_PATTERN.fullmatch(str(record.get("id", ""))):
                    records.append(record)
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                continue
        records.sort(key=lambda item: str(item.get("createdAt", "")), reverse=True)
        for record in records[:MAX_TASKS]:
            if record.get("status") in ACTIVE_STATUSES:
                record["status"] = "interrupted"
                record["finishedAt"] = _now()
                record["error"] = "服务重启导致任务执行状态中断"
                _write_json_atomic(self._task_path(record["id"]), record)
            self.tasks[record["id"]] = record

    def _persist_locked(self, task: dict[str, Any]) -> None:
        """持有状态锁时保存单个任务，并限制日志与结果文件大小。"""
        task["log"] = str(task.get("log") or "")[-MAX_OUTPUT_CHARS:]
        task["result"] = str(task.get("result") or "")[-MAX_OUTPUT_CHARS:]
        _write_json_atomic(self._task_path(task["id"]), task)

    def create(self, description: Any, requested_experts: Any) -> dict[str, Any]:
        """校验信任边界输入，创建任务并启动后台 Codex 编排线程。"""
        if not isinstance(description, str) or not description.strip():
            raise AgencyOrchestratorError("任务描述不能为空")
        description = description.strip()
        if "\x00" in description:
            raise AgencyOrchestratorError("任务描述不能包含 NUL 字符")
        if len(description) > MAX_DESCRIPTION_CHARS:
            raise AgencyOrchestratorError(f"任务描述不能超过 {MAX_DESCRIPTION_CHARS} 个字符")
        if requested_experts is None:
            requested_experts = []
        if not isinstance(requested_experts, list) or any(
            not isinstance(expert_id, str) for expert_id in requested_experts
        ):
            raise AgencyOrchestratorError("selectedExperts 必须是专家 ID 数组")
        if len(requested_experts) > MAX_SELECTED_EXPERTS:
            raise AgencyOrchestratorError(f"一次最多选择 {MAX_SELECTED_EXPERTS} 位专家")

        selected = _select_experts(description, requested_experts)
        provider = agency_providers.get_current_provider()
        with self.lock:
            if any(task.get("status") in ACTIVE_STATUSES for task in self.tasks.values()):
                raise AgencyOrchestratorError("已有 Agent 编排任务正在执行，请等待或先取消")
            task_id = uuid.uuid4().hex
            task = {
                "id": task_id,
                "description": description,
                "requestedExperts": list(dict.fromkeys(requested_experts)),
                "selectedExperts": [expert["id"] for expert in selected],
                "providerId": provider["id"],
                "providerLabel": provider["label"],
                "status": "queued",
                "stage": "等待启动",
                "createdAt": _now(),
                "startedAt": None,
                "finishedAt": None,
                "log": "",
                "result": "",
                "error": None,
                "cancelRequested": False,
            }
            self.tasks[task_id] = task
            self._persist_locked(task)

        worker = threading.Thread(
            target=self._run,
            args=(task_id, selected, provider),
            daemon=True,
            name=f"agency-task-{task_id[:8]}",
        )
        worker.start()
        return self.get(task_id) or {}

    def _run_cloud(
        self,
        task_id: str,
        experts: list[dict[str, Any]],
        provider: dict[str, Any],
    ) -> str:
        """通过云端模型依次执行规划、领域专家和 Reality Checker 质量门。"""
        task = self.tasks[task_id]
        orchestrator = experts[0]
        planner_prompt = (
            Path(orchestrator["skillPath"]).read_text(encoding="utf-8-sig")
            + "\n\n请为以下任务制定可执行计划，并明确每位专家的职责：\n"
            + task["description"]
        )
        plan = agency_providers.call_cloud_provider(provider, planner_prompt)
        with self.lock:
            task["stage"] = "编排计划已生成"
            task["log"] += "[编排器] 已完成任务拆解。\n"
            self._persist_locked(task)

        expert_results: list[str] = []
        domain_experts = [
            expert for expert in experts
            if expert["id"] not in {"agency-agents-orchestrator", "agency-reality-checker"}
        ]
        for expert in domain_experts:
            with self.lock:
                if self.tasks[task_id].get("cancelRequested"):
                    raise AgencyOrchestratorError("任务已取消")
                task["stage"] = "专家执行中：" + expert["id"]
                task["log"] += f"[专家] {expert['id']} 开始执行。\n"
                self._persist_locked(task)
            expert_prompt = (
                Path(expert["skillPath"]).read_text(encoding="utf-8-sig")
                + "\n\n用户任务：\n" + task["description"]
                + "\n\n编排计划：\n" + plan
                + "\n\n请只完成分配给你的专业工作，并给出可交付结果与验证证据。"
            )
            result = agency_providers.call_cloud_provider(provider, expert_prompt)
            expert_results.append(f"## {expert['id']}\n{result}")

        checker = next(
            (expert for expert in experts if expert["id"] == "agency-reality-checker"),
            orchestrator,
        )
        checker_prompt = (
            Path(checker["skillPath"]).read_text(encoding="utf-8-sig")
            + "\n\n用户任务：\n" + task["description"]
            + "\n\n编排计划：\n" + plan
            + "\n\n专家产出：\n" + "\n\n".join(expert_results)
            + "\n\n请执行最终质量检查，整合为中文最终交付；明确验证证据和未完成事项。"
        )
        return agency_providers.call_cloud_provider(provider, checker_prompt)

    def _run(
        self,
        task_id: str,
        experts: list[dict[str, Any]],
        provider: dict[str, Any],
    ) -> None:
        """调用 Codex 主编排 Agent；CLI 自身负责按技能约束派发多代理任务。"""
        output_path = TASK_ROOT / f"{task_id}.result.txt"
        try:
            with self.lock:
                task = self.tasks[task_id]
                task["status"] = "running"
                task["stage"] = "专家团队执行中"
                task["startedAt"] = _now()
                task["log"] = (
                    "[编排器] 执行提供商：" + task["providerLabel"] + "\n"
                    + "[编排器] 已选择专家：" + "、".join(task["selectedExperts"]) + "\n"
                )
                self._persist_locked(task)

            if provider["type"] == "api":
                result = self._run_cloud(task_id, experts, provider)
                with self.lock:
                    task = self.tasks[task_id]
                    task["finishedAt"] = _now()
                    task["status"] = "succeeded"
                    task["stage"] = "已完成"
                    task["result"] = result
                    self._persist_locked(task)
                return

            if provider["id"] == "codex-cli":
                command = _build_codex_command(output_path)
            elif provider["id"] == "claude-cli":
                command = [
                    provider["command"], "--print", "--output-format", "text",
                    "--input-format", "text", "--permission-mode", "acceptEdits",
                ]
            else:
                command = [provider["command"], "--prompt", "-"]

            creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
            if os.name == "nt":
                creation_flags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            process = subprocess.Popen(
                command,
                cwd=str(WORKSPACE_ROOT),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                shell=False,
                creationflags=creation_flags,
                start_new_session=os.name != "nt",
            )
            with self.lock:
                self.processes[task_id] = process
                task = self.tasks[task_id]
                should_cancel = bool(task.get("cancelRequested"))

            if should_cancel:
                self._terminate_process_tree(process)

            prompt = _build_prompt(self.tasks[task_id], experts)
            try:
                stdout, _ = process.communicate(input=prompt, timeout=TASK_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                self._terminate_process_tree(process)
                stdout, _ = process.communicate()
                raise AgencyOrchestratorError("Agent 编排超过 15 分钟，已自动终止")

            result = ""
            if output_path.exists():
                result = output_path.read_text(encoding="utf-8", errors="replace")
            if not result.strip():
                result = stdout

            with self.lock:
                task = self.tasks[task_id]
                task["log"] += stdout[-MAX_OUTPUT_CHARS:]
                task["finishedAt"] = _now()
                if task.get("cancelRequested"):
                    task["status"] = "cancelled"
                    task["stage"] = "已取消"
                elif process.returncode == 0:
                    task["status"] = "succeeded"
                    task["stage"] = "已完成"
                    task["result"] = result
                else:
                    task["status"] = "failed"
                    task["stage"] = "执行失败"
                    task["error"] = f"Codex CLI 退出码：{process.returncode}"
                    task["result"] = result
                self.processes.pop(task_id, None)
                self._persist_locked(task)
        except Exception as exc:
            with self.lock:
                task = self.tasks.get(task_id)
                if task is None:
                    return
                task["finishedAt"] = _now()
                if task.get("cancelRequested"):
                    task["status"] = "cancelled"
                    task["stage"] = "已取消"
                else:
                    task["status"] = "failed"
                    task["stage"] = "执行失败"
                    task["error"] = f"{type(exc).__name__}: {exc}"
                    task["log"] += "\n[编排器异常]\n" + traceback.format_exc()
                self.processes.pop(task_id, None)
                self._persist_locked(task)
        finally:
            try:
                output_path.unlink(missing_ok=True)
            except OSError:
                pass

    def _terminate_process_tree(self, process: subprocess.Popen[str]) -> None:
        """终止 Codex 及其子代理进程，确保取消后不留下后台任务。"""
        if process.poll() is not None:
            return
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        else:
            process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()

    def cancel(self, task_id: Any) -> dict[str, Any]:
        """取消指定活动任务；已完成任务保持不变并返回冲突错误。"""
        task = self._require_task(task_id)
        with self.lock:
            if task.get("status") not in ACTIVE_STATUSES:
                raise AgencyOrchestratorError("该任务已结束，无法取消")
            task["cancelRequested"] = True
            task["status"] = "cancelling"
            task["stage"] = "正在取消"
            process = self.processes.get(task["id"])
            self._persist_locked(task)
        if process is not None:
            threading.Thread(
                target=self._terminate_process_tree,
                args=(process,),
                daemon=True,
                name=f"agency-cancel-{task['id'][:8]}",
            ).start()
        return self.get(task["id"]) or {}

    def _require_task(self, task_id: Any) -> dict[str, Any]:
        """校验任务 ID 格式并返回任务，防止路径穿越或任意文件读取。"""
        if not isinstance(task_id, str) or not TASK_ID_PATTERN.fullmatch(task_id):
            raise AgencyOrchestratorError("任务 ID 不合法")
        with self.lock:
            task = self.tasks.get(task_id)
            if task is None:
                raise AgencyOrchestratorError("任务不存在")
            return task

    def get(self, task_id: Any) -> dict[str, Any] | None:
        """返回任务深拷贝，避免 HTTP 序列化期间读取到并发写入。"""
        if not isinstance(task_id, str) or not TASK_ID_PATTERN.fullmatch(task_id):
            return None
        with self.lock:
            task = self.tasks.get(task_id)
            return json.loads(json.dumps(task, ensure_ascii=False)) if task else None

    def list(self) -> list[dict[str, Any]]:
        """按创建时间倒序返回最近任务；列表保留结果以支持刷新后继续查看。"""
        with self.lock:
            tasks = sorted(
                self.tasks.values(),
                key=lambda item: str(item.get("createdAt", "")),
                reverse=True,
            )[:MAX_TASKS]
            return json.loads(json.dumps(tasks, ensure_ascii=False))


AGENCY_STATE = AgencyTaskState()
