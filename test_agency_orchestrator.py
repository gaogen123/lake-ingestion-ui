"""Agency 编排器最小安全与专家迁移自检。"""

import tempfile
import unittest
from pathlib import Path

import agency_orchestrator


class AgencyOrchestratorTest(unittest.TestCase):
    """覆盖专家发现、白名单和固定命令边界。"""

    def test_all_installed_experts_are_discovered(self) -> None:
        """本机所有合法 agency-* 技能都应进入专家库，公开字段不得泄露路径。"""
        installed_count = sum(
            1
            for path in agency_orchestrator.SKILLS_ROOT.glob("agency-*/SKILL.md")
            if path.is_file()
        )
        experts = agency_orchestrator.list_public_experts()
        self.assertEqual(len(experts), installed_count)
        self.assertTrue(experts)
        self.assertTrue(all("skillPath" not in expert for expert in experts))

    def test_unknown_expert_and_oversized_selection_are_rejected(self) -> None:
        """客户端只能提交专家库白名单中的 ID，且不能绕过最大选择数量。"""
        with self.assertRaises(agency_orchestrator.AgencyOrchestratorError):
            agency_orchestrator._select_experts("测试任务", ["agency-not-installed"])
        experts = agency_orchestrator.list_experts()
        with self.assertRaises(agency_orchestrator.AgencyOrchestratorError):
            agency_orchestrator.AGENCY_STATE.create(
                "测试任务",
                [expert["id"] for expert in experts[:6]],
            )

    def test_user_prompt_is_not_part_of_command_arguments(self) -> None:
        """包含 shell 元字符的任务只能进入 stdin prompt，不能进入命令参数。"""
        malicious_text = "x & whoami | calc"
        with tempfile.TemporaryDirectory() as temp_dir:
            command = agency_orchestrator._build_codex_command(Path(temp_dir) / "result.txt")
        prompt = agency_orchestrator._build_prompt(
            {"description": malicious_text},
            agency_orchestrator._select_experts("前端测试", ["agency-frontend-developer"]),
        )
        self.assertNotIn(malicious_text, command)
        self.assertEqual(command[-1], "-")
        self.assertIn(malicious_text, prompt)


if __name__ == "__main__":
    unittest.main()
