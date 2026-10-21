"""验证差分验收不会把缺失响应、副作用或断言失败判为一致。"""
import copy
import unittest
from run import compare


class ComparisonTests(unittest.TestCase):
    """注入人为差异，检查比较器的失败边界。"""
    def setUp(self):
        """准备包含响应和文件副作用的最小轨迹。"""
        self.report = {'requests': 1, 'assertion_failures': [],
                       'runtime': {'bash': 'same', 'ripgrep': 'same'},
                       'steps': [{'name': 'stdio/read', 'response': {'result': {'content': [{'type': 'text', 'text': 'exact'}]}},
                                  'files': {'a.txt': {'sha256': 'abc', 'mode': '0o644'}}}]}

    def test_identical_reports_pass(self):
        """完全一致且没有断言失败才能通过。"""
        self.assertEqual(compare(self.report, copy.deepcopy(self.report)), [])

    def test_response_and_file_mutations_fail(self):
        """响应文本、真实文件内容和权限变化都必须产生差异。"""
        for change in ('text', 'sha256', 'mode'):
            with self.subTest(change=change):
                other = copy.deepcopy(self.report)
                if change == 'text':
                    other['steps'][0]['response']['result']['content'][0]['text'] = 'different'
                else:
                    other['steps'][0]['files']['a.txt'][change] = 'different'
                self.assertTrue(compare(self.report, other))

    def test_missing_and_extra_steps_fail(self):
        """无法通过 zip 截断掩盖缺失或额外响应。"""
        other = copy.deepcopy(self.report)
        other['steps'] = []
        self.assertTrue(compare(self.report, other))
        self.assertTrue(compare(other, self.report))

    def test_json_boolean_is_not_a_number(self):
        """Python 的 False == 0 不能掩盖 JSON-RPC ID 类型变化。"""
        first = copy.deepcopy(self.report)
        other = copy.deepcopy(self.report)
        first['steps'][0]['response']['id'] = False
        other['steps'][0]['response']['id'] = 0
        self.assertTrue(compare(first, other))

    def test_shared_assertion_failure_fails(self):
        """即使两端同样失败，也不能证明功能成功。"""
        other = copy.deepcopy(self.report)
        other['assertion_failures'] = [{'name': 'read', 'reason': 'missing image'}]
        self.assertTrue(compare(other, copy.deepcopy(other)))

    def test_request_counts_and_dependency_versions_fail(self):
        """通知栅栏请求数量及外部工具版本漂移必须可见。"""
        other = copy.deepcopy(self.report)
        other['requests'] = 2
        self.assertTrue(compare(self.report, other))
        other = copy.deepcopy(self.report)
        other['runtime']['ripgrep'] = 'different'
        self.assertTrue(compare(self.report, other))

    def test_duplicate_case_names_fail(self):
        """重复命名不能覆盖原有用例并隐藏结果。"""
        other = copy.deepcopy(self.report)
        other['steps'] *= 2
        self.assertTrue(compare(other, other))


if __name__ == '__main__':
    unittest.main()
