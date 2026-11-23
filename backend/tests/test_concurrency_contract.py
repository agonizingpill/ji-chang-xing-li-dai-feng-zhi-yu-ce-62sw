"""生产代码并发与业务约束的静态契约测试。

集成测试在 SQLite 上验证业务语义；本文件确认 PostgreSQL 实现里
真正存在行级锁、固定加锁顺序和发布隔离，避免并发保障被误删。
"""

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class ConcurrencyContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.service = (ROOT / 'src' / 'provisional_service.py').read_text(encoding='utf-8')
        cls.routes = (ROOT / 'src' / 'provisional_routes.py').read_text(encoding='utf-8')
        cls.app = (ROOT / 'src' / 'app.py').read_text(encoding='utf-8')

    def test_estimate_locks_wave_rows_in_sorted_order(self):
        # 固定加锁顺序，避免暂估与撤回事务相互死锁
        self.assertIn('sorted((target_wave_id, source_wave_id))', self.service)
        self.assertIn('FOR UPDATE', self.service)

    def test_estimate_locks_candidate_count_rows(self):
        # 候选读数行级锁，与撤回的计数行锁互斥
        self.assertIn('FOR UPDATE OF b', self.service)

    def test_withdrawal_locks_wave_and_count_rows(self):
        self.assertIn("FROM flight_waves WHERE id=%s FOR UPDATE", self.service)
        self.assertIn('FOR UPDATE', self.service)
        # 撤回在同一事务内失效相关暂估
        self.assertIn("status='active'", self.service)
        self.assertIn("status='invalidated'", self.service)

    def test_estimate_excludes_withdrawn_readings(self):
        self.assertIn('NOT EXISTS', self.service)
        self.assertIn('count_withdrawals', self.service)
        # 支持按行李带撤回（NULL = 全部行李带）
        self.assertIn('x.belt_code IS NULL OR x.belt_code = b.belt_code', self.service)

    def test_business_guards_present(self):
        self.assertIn("source['status'] != 'closed'", self.service)
        self.assertIn("source['terminal'] != target['terminal']", self.service)
        self.assertIn('own_points >= MIN_POINTS', self.service)
        self.assertIn('len(points) < MIN_POINTS', self.service)

    def test_estimate_marked_reference_only_and_not_publishable(self):
        self.assertIn("'reference_only': True", self.service)
        self.assertIn("'publishable': False", self.service)

    def test_withdrawal_restricted_to_supervisor(self):
        self.assertIn("require('supervisor')", self.routes)

    def test_estimate_creation_restricted_to_writers(self):
        self.assertIn("require('operator', 'supervisor')", self.routes)

    def test_provisional_router_registered(self):
        self.assertIn('provisional', self.app)

    def test_migration_creates_required_tables(self):
        ddl = (ROOT / 'src' / 'db.py').read_text(encoding='utf-8')
        for table in ('count_withdrawals', 'provisional_estimates',
                      'provisional_estimate_readings'):
            self.assertIn(f'CREATE TABLE IF NOT EXISTS {table}', ddl)
        # 失效状态受 CHECK 约束限制
        self.assertIn("status IN ('active','invalidated')", ddl)


if __name__ == '__main__':
    unittest.main()
