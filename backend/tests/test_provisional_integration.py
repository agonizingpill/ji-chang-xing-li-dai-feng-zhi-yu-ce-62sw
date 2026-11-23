"""借读数暂估、故障撤回与并发安全的集成测试。

本地没有 PostgreSQL，这里用 SQLite 复刻生产 SQL 的等价语义：
- 生产用 SELECT ... FOR UPDATE 锁定波次行/计数行，SQLite 写事务用
  BEGIN IMMEDIATE 取得 RESERVED 锁，使同一时刻只有一个写事务，
  从而等价地串行化「暂估创建」与「故障撤回」。
- 撤回后重新查询读数会排除撤回区间，验证“撤回完成后不可能再产生
  引用该时段读数的有效暂估”这一不变量。
生产代码中的 FOR UPDATE / 锁顺序由 test_concurrency_contract.py 静态校验。
"""

import json
import os
import sqlite3
import sys
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

os.environ.setdefault('DATABASE_URL', 'postgresql://u:p@localhost:5432/db')
os.environ.setdefault('REDIS_URL', 'redis://localhost:6379/0')
os.environ.setdefault('JWT_SECRET', 'test-secret')
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.algorithm import stl_rolling_forecast  # noqa: E402
from src.provisional_service import scale_peak  # noqa: E402

UTC = timezone.utc
MIN_POINTS = 8


def ts(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S%z')


class Store:
    """SQLite 复刻 provisional_service 的表结构与业务 SQL（去掉 PG 专有语法）。"""

    def __init__(self):
        self.conn = sqlite3.connect(':memory:', check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute('PRAGMA journal_mode=WAL')
        self._lock = threading.Lock()
        self._build()
        self._seed()

    def _build(self):
        c = self.conn
        c.executescript(
            """
            CREATE TABLE flight_waves(
              id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE, terminal TEXT,
              arrival_start TEXT, arrival_end TEXT, planned_bags INTEGER,
              cancellation_state TEXT DEFAULT 'normal', status TEXT DEFAULT 'open');
            CREATE TABLE belt_counts(
              id INTEGER PRIMARY KEY AUTOINCREMENT, wave_id INTEGER, belt_code TEXT,
              counted_at TEXT, bag_count INTEGER, scan_source TEXT,
              status TEXT DEFAULT 'accepted',
              UNIQUE(wave_id,belt_code,counted_at));
            CREATE TABLE count_withdrawals(
              id INTEGER PRIMARY KEY AUTOINCREMENT, wave_id INTEGER, belt_code TEXT,
              window_start TEXT, window_end TEXT, reason TEXT, withdrawn_by INTEGER,
              created_at TEXT DEFAULT (datetime('now')));
            CREATE TABLE provisional_estimates(
              id INTEGER PRIMARY KEY AUTOINCREMENT, wave_id INTEGER, source_wave_id INTEGER,
              status TEXT DEFAULT 'active', source_points INTEGER, planned_bags_ratio REAL,
              raw_peak_bags REAL, estimated_peak_bags REAL,
              source_window_start TEXT, source_window_end TEXT,
              window_size INTEGER, snapshot_hash TEXT, result TEXT,
              invalidation_reason TEXT, invalidated_at TEXT, withdrawal_id INTEGER,
              created_by INTEGER, created_at TEXT DEFAULT (datetime('now')));
            CREATE TABLE provisional_estimate_readings(
              estimate_id INTEGER, count_id INTEGER,
              PRIMARY KEY(estimate_id, count_id));
            CREATE TABLE users(id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT, role TEXT);
            """
        )
        c.commit()

    def _seed(self):
        c = self.conn
        c.execute("INSERT INTO users(username,role) VALUES ('ops','operator')")
        c.execute("INSERT INTO users(username,role) VALUES ('mgr','supervisor')")
        base = datetime(2026, 11, 21, 6, 0, tzinfo=UTC)
        # 同航站楼 T2：已结束的供数波次（12 个读数）
        c.execute(
            "INSERT INTO flight_waves(code,terminal,arrival_start,arrival_end,planned_bags,status)"
            " VALUES ('SRC','T2',?,?,360,'closed')",
            (ts(base - timedelta(hours=5)), ts(base - timedelta(hours=3))))
        src = c.execute("SELECT id FROM flight_waves WHERE code='SRC'").fetchone()[0]
        start = base - timedelta(hours=5)
        for i in range(12):
            c.execute(
                "INSERT INTO belt_counts(wave_id,belt_code,counted_at,bag_count,scan_source)"
                " VALUES (?,?,?,?, 'counter')",
                (src, 'B1', ts(start + timedelta(minutes=10 * i)), 40 + i * 12 + (i % 4) * 3))
        # 目标波次：计数不足（3 个读数），计划 270 件
        c.execute(
            "INSERT INTO flight_waves(code,terminal,arrival_start,arrival_end,planned_bags,status)"
            " VALUES ('TGT','T2',?,?,270,'open')",
            (ts(base + timedelta(hours=2)), ts(base + timedelta(hours=4))))
        tgt = c.execute("SELECT id FROM flight_waves WHERE code='TGT'").fetchone()[0]
        tstart = base + timedelta(hours=2)
        for i in range(3):
            c.execute(
                "INSERT INTO belt_counts(wave_id,belt_code,counted_at,bag_count,scan_source)"
                " VALUES (?,?,?,?, 'counter')",
                (tgt, 'B9', ts(tstart + timedelta(minutes=10 * i)), 48 + i * 15))
        # 不同航站楼的已结束波次（不可借）
        c.execute(
            "INSERT INTO flight_waves(code,terminal,arrival_start,arrival_end,planned_bags,status)"
            " VALUES ('OTHER','T3',?,?,300,'closed')",
            (ts(base - timedelta(hours=8)), ts(base - timedelta(hours=6))))
        other = c.execute("SELECT id FROM flight_waves WHERE code='OTHER'").fetchone()[0]
        ostart = base - timedelta(hours=8)
        for i in range(10):
            c.execute(
                "INSERT INTO belt_counts(wave_id,belt_code,counted_at,bag_count,scan_source)"
                " VALUES (?,?,?,?,'counter')",
                (other, 'B2', ts(ostart + timedelta(minutes=10 * i)), 30 + i * 10))
        c.commit()
        self.ids = {'src': src, 'tgt': tgt, 'other': other}
        self.base = base

    # ---- 复刻生产查询：可借读数（排除撤回区间）----
    def available_readings(self, source_id):
        return self.conn.execute(
            """
            SELECT b.id, b.belt_code, b.counted_at, b.bag_count FROM belt_counts b
            WHERE b.wave_id=? AND b.status='accepted'
              AND NOT EXISTS (
                SELECT 1 FROM count_withdrawals x
                WHERE x.wave_id=b.wave_id
                  AND (x.belt_code IS NULL OR x.belt_code=b.belt_code)
                  AND b.counted_at >= x.window_start AND b.counted_at < x.window_end)
            ORDER BY b.counted_at
            """, (source_id,)).fetchall()

    def own_count(self, wave_id):
        return self.conn.execute(
            "SELECT COUNT(*) FROM belt_counts WHERE wave_id=? AND status='accepted'",
            (wave_id,)).fetchone()[0]

    # ---- 暂估创建（对应 provisional_service.create_estimate 的事务体）----
    def create_estimate(self, target_id, source_id, window_size=8, actor=1):
        with self._lock:
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                target = self.conn.execute(
                    'SELECT * FROM flight_waves WHERE id=?', (target_id,)).fetchone()
                source = self.conn.execute(
                    'SELECT * FROM flight_waves WHERE id=?', (source_id,)).fetchone()
                assert target and source
                if source['terminal'] != target['terminal']:
                    raise AssertionError('只能借用同航站楼波次的读数')
                if source['status'] != 'closed':
                    raise AssertionError('只能借用已结束波次的读数')
                if self.own_count(target_id) >= MIN_POINTS:
                    raise AssertionError('本波计数已足够，请直接发起正式预测')
                rows = self.available_readings(source_id)
                if len(rows) < MIN_POINTS:
                    raise AssertionError('供数波次可借的有效读数不足 8 个')
                points = [{'counted_at': datetime.strptime(r['counted_at'], '%Y-%m-%d %H:%M:%S%z'),
                           'bag_count': r['bag_count']} for r in rows]
                raw = stl_rolling_forecast(points, window_size)
                ratio, scaled = scale_peak(raw, target['planned_bags'],
                                           source['planned_bags'])
                cur = self.conn.execute(
                    """INSERT INTO provisional_estimates(wave_id,source_wave_id,status,
                       source_points,planned_bags_ratio,raw_peak_bags,estimated_peak_bags,
                       source_window_start,source_window_end,window_size,snapshot_hash,result,created_by)
                       VALUES (?,?,'active',?,?,?,?,?,?,?,?,?,?)""",
                    (target_id, source_id, len(rows), ratio, raw['peak_bags'],
                     scaled['peak_bags'], rows[0]['counted_at'], rows[-1]['counted_at'],
                     window_size, raw['snapshot_hash'],
                     json.dumps({'reference_only': True, 'publishable': False}), actor))
                est_id = cur.lastrowid
                self.conn.executemany(
                    'INSERT INTO provisional_estimate_readings(estimate_id,count_id) VALUES (?,?)',
                    [(est_id, r['id']) for r in rows])
                self.conn.commit()
                return est_id
            except Exception:
                self.conn.rollback()
                raise

    # ---- 故障撤回（对应 provisional_service.withdraw_window 的事务体）----
    def withdraw(self, wave_id, belt_code, start_dt, end_dt, reason='故障', actor=2):
        with self._lock:
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                wave = self.conn.execute(
                    'SELECT * FROM flight_waves WHERE id=?', (wave_id,)).fetchone()
                assert wave
                hits = self.conn.execute(
                    """SELECT b.id FROM belt_counts b
                       WHERE b.wave_id=? AND b.status='accepted'
                         AND (? IS NULL OR b.belt_code=?)
                         AND b.counted_at >= ? AND b.counted_at < ?""",
                    (wave_id, belt_code, belt_code, ts(start_dt), ts(end_dt))).fetchall()
                hit_ids = [r['id'] for r in hits]
                cur = self.conn.execute(
                    """INSERT INTO count_withdrawals(wave_id,belt_code,window_start,window_end,reason,withdrawn_by)
                       VALUES (?,?,?,?,?,?)""",
                    (wave_id, belt_code, ts(start_dt), ts(end_dt), reason, actor))
                withdrawal_id = cur.lastrowid
                invalidated = []
                if hit_ids:
                    placeholders = ','.join('?' * len(hit_ids))
                    est_rows = self.conn.execute(
                        f"""SELECT DISTINCT pe.id FROM provisional_estimate_readings pr
                            JOIN provisional_estimates pe ON pe.id=pr.estimate_id
                            WHERE pr.count_id IN ({placeholders}) AND pe.status='active'""",
                        hit_ids).fetchall()
                    scope = f'行李带 {belt_code} ' if belt_code else '全部行李带 '
                    text = (f"供数波次 {wave['code']} {scope} 读数被主管撤回：{reason}")
                    for r in est_rows:
                        self.conn.execute(
                            """UPDATE provisional_estimates SET status='invalidated',
                               invalidation_reason=?, invalidated_at=datetime('now'),
                               withdrawal_id=? WHERE id=?""",
                            (text, withdrawal_id, r['id']))
                        invalidated.append(r['id'])
                self.conn.commit()
                return withdrawal_id, invalidated
            except Exception:
                self.conn.rollback()
                raise

    def estimate(self, est_id):
        return dict(self.conn.execute(
            'SELECT * FROM provisional_estimates WHERE id=?', (est_id,)).fetchone())

    def estimate_reading_ids(self, est_id):
        return [r[0] for r in self.conn.execute(
            'SELECT count_id FROM provisional_estimate_readings WHERE estimate_id=?',
            (est_id,)).fetchall()]


class ProvisionalFlowTests(unittest.TestCase):
    def setUp(self):
        self.s = Store()

    def test_borrow_when_own_counts_insufficient(self):
        self.assertLess(self.s.own_count(self.s.ids['tgt']), MIN_POINTS)
        est_id = self.s.create_estimate(self.s.ids['tgt'], self.s.ids['src'])
        est = self.s.estimate(est_id)
        self.assertEqual(est['status'], 'active')
        self.assertEqual(est['source_points'], 12)
        # 折算系数 270/360 = 0.75，暂估峰值小于原始峰值
        self.assertAlmostEqual(est['planned_bags_ratio'], 0.75)
        self.assertLess(est['estimated_peak_bags'], est['raw_peak_bags'])
        self.assertAlmostEqual(
            est['estimated_peak_bags'], round(est['raw_peak_bags'] * 0.75, 2))
        # 记录了用数读数与时段
        self.assertEqual(len(self.s.estimate_reading_ids(est_id)), 12)
        self.assertTrue(est['source_window_start'] <= est['source_window_end'])

    def test_cannot_borrow_other_terminal(self):
        with self.assertRaises(AssertionError):
            self.s.create_estimate(self.s.ids['tgt'], self.s.ids['other'])

    def test_cannot_borrow_when_own_counts_sufficient(self):
        # 给目标波次补到 8 个读数
        tstart = self.s.base + timedelta(hours=2)
        for i in range(3, 8):
            self.s.conn.execute(
                "INSERT INTO belt_counts(wave_id,belt_code,counted_at,bag_count,scan_source)"
                " VALUES (?,?,?,?,'counter')",
                (self.s.ids['tgt'], 'B9',
                 ts(tstart + timedelta(minutes=10 * i)), 90 + i * 5))
        self.s.conn.commit()
        self.assertGreaterEqual(self.s.own_count(self.s.ids['tgt']), MIN_POINTS)
        with self.assertRaises(AssertionError):
            self.s.create_estimate(self.s.ids['tgt'], self.s.ids['src'])

    def test_withdrawal_invalidates_only_estimates_using_window(self):
        est_a = self.s.create_estimate(self.s.ids['tgt'], self.s.ids['src'])
        # 撤回供数波次第 4~6 个读数所在时段（索引 3,4,5）
        wstart = self.s.base - timedelta(hours=5) + timedelta(minutes=30)
        wend = self.s.base - timedelta(hours=5) + timedelta(minutes=60)
        _, invalidated = self.s.withdraw(self.s.ids['src'], 'B1', wstart, wend)
        self.assertIn(est_a, invalidated)
        est = self.s.estimate(est_a)
        self.assertEqual(est['status'], 'invalidated')
        self.assertIsNotNone(est['invalidation_reason'])
        self.assertIn('撤回', est['invalidation_reason'])
        self.assertIsNotNone(est['withdrawal_id'])
        # 撤回记录可统计本次失效的暂估数量（对应 list_withdrawals.affected_estimates）
        counted = self.s.conn.execute(
            'SELECT COUNT(*) FROM provisional_estimates WHERE withdrawal_id=?',
            (est['withdrawal_id'],)).fetchone()[0]
        self.assertEqual(counted, len(invalidated))

    def test_recalc_after_withdrawal_uses_only_valid_readings(self):
        first = self.s.create_estimate(self.s.ids['tgt'], self.s.ids['src'])
        # 撤回前 6 个读数的时段
        wstart = self.s.base - timedelta(hours=5)
        wend = self.s.base - timedelta(hours=5) + timedelta(minutes=60)
        self.s.withdraw(self.s.ids['src'], 'B1', wstart, wend)
        # 剩余 6 个有效读数，不足 8 个 -> 重算被拒绝
        self.assertEqual(len(self.s.available_readings(self.s.ids['src'])), 6)
        with self.assertRaises(AssertionError):
            self.s.create_estimate(self.s.ids['tgt'], self.s.ids['src'])
        # 旧暂估保持失效，不会“复活”
        self.assertEqual(self.s.estimate(first)['status'], 'invalidated')

    def test_withdrawal_window_without_readings_invalidates_nothing(self):
        est = self.s.create_estimate(self.s.ids['tgt'], self.s.ids['src'])
        far = self.s.base + timedelta(days=2)
        _, invalidated = self.s.withdraw(self.s.ids['src'], 'B1', far, far + timedelta(hours=1))
        self.assertEqual(invalidated, [])
        self.assertEqual(self.s.estimate(est)['status'], 'active')

    def test_belt_specific_withdrawal_does_not_touch_other_belts(self):
        # 新增另一条行李带的读数
        start = self.s.base - timedelta(hours=5)
        for i in range(12):
            self.s.conn.execute(
                "INSERT INTO belt_counts(wave_id,belt_code,counted_at,bag_count,scan_source)"
                " VALUES (?,?,?,?,'counter')",
                (self.s.ids['src'], 'B2',
                 ts(start + timedelta(minutes=10 * i)), 20 + i * 8))
        self.s.conn.commit()
        est = self.s.create_estimate(self.s.ids['tgt'], self.s.ids['src'])
        used = set(self.s.estimate_reading_ids(est))
        wstart = self.s.base - timedelta(hours=5)
        wend = self.s.base - timedelta(hours=5) + timedelta(minutes=60)
        _, invalidated = self.s.withdraw(self.s.ids['src'], 'B1', wstart, wend)
        # 暂估用了 B1 的读数 -> 失效；B2 读数全部保留，B1 仅窗口内读数被排除
        self.assertIn(est, invalidated)
        remaining = self.s.available_readings(self.s.ids['src'])
        self.assertEqual(len(remaining), 18)  # B1 剩 6 条 + B2 12 条
        self.assertEqual(sum(1 for r in remaining if r['belt_code'] == 'B2'), 12)
        # 撤回窗口内的 B1 读数（索引 0-5）一定不在有效集合
        withdrawn_window_ids = {r['id'] for r in self.s.conn.execute(
            """SELECT b.id FROM belt_counts b JOIN count_withdrawals x
               ON x.wave_id=b.wave_id AND x.belt_code=b.belt_code
              AND b.counted_at>=x.window_start AND b.counted_at<x.window_end
               WHERE b.wave_id=? AND b.belt_code='B1'""", (self.s.ids['src'],)).fetchall()}
        self.assertTrue({r['id'] for r in remaining}.isdisjoint(withdrawn_window_ids))
        # 失效暂估引用的读数集合不变（留痕）
        self.assertEqual(set(self.s.estimate_reading_ids(est)), used)

    def test_global_withdrawal_covers_all_belts(self):
        start = self.s.base - timedelta(hours=5)
        for i in range(12):
            self.s.conn.execute(
                "INSERT INTO belt_counts(wave_id,belt_code,counted_at,bag_count,scan_source)"
                " VALUES (?,?,?,?,'counter')",
                (self.s.ids['src'], 'B2',
                 ts(start + timedelta(minutes=10 * i)), 20 + i * 8))
        self.s.conn.commit()
        self.s.create_estimate(self.s.ids['tgt'], self.s.ids['src'])
        wstart = start
        wend = start + timedelta(hours=2)
        _, invalidated = self.s.withdraw(self.s.ids['src'], None, wstart, wend)
        self.assertEqual(len(invalidated), 1)
        self.assertEqual(len(self.s.available_readings(self.s.ids['src'])), 0)


class ConcurrencyTests(unittest.TestCase):
    """并发不变量：撤回完成后，不存在引用被撤回读数的有效暂估。"""

    def setUp(self):
        self.s = Store()
        self.errors = []

    def _worker(self, fn):
        try:
            fn()
        except Exception as exc:  # 串行化冲突是预期结果之一
            self.errors.append(str(exc))

    def test_concurrent_create_and_withdraw_never_leaks_valid_estimate(self):
        src, tgt = self.s.ids['src'], self.s.ids['tgt']
        wstart = self.s.base - timedelta(hours=5)
        wend = self.s.base - timedelta(hours=5) + timedelta(minutes=60)
        valid_estimates = []
        for _ in range(30):
            t1 = threading.Thread(target=self._worker, args=(
                lambda: valid_estimates.append(self.s.create_estimate(tgt, src)),))
            t2 = threading.Thread(target=self._worker, args=(
                lambda: self.s.withdraw(src, 'B1', wstart, wend),))
            t1.start(); t2.start(); t1.join(); t2.join()
            # 每轮后验证不变量：所有 active 暂估都不引用任何被撤回区间的读数
            withdrawn_ids = {r['id'] for r in self.s.conn.execute(
                """SELECT b.id FROM belt_counts b JOIN count_withdrawals x
                   ON x.wave_id=b.wave_id
                  AND (x.belt_code IS NULL OR x.belt_code=b.belt_code)
                  AND b.counted_at>=x.window_start AND b.counted_at<x.window_end
                   WHERE b.wave_id=?""", (src,)).fetchall()}
            for row in self.s.conn.execute(
                    "SELECT id FROM provisional_estimates WHERE status='active'").fetchall():
                used = {r[0] for r in self.s.conn.execute(
                    'SELECT count_id FROM provisional_estimate_readings WHERE estimate_id=?',
                    (row['id'],)).fetchall()}
                self.assertTrue(
                    used.isdisjoint(withdrawn_ids),
                    '存在引用已撤回读数的有效暂估，违反并发不变量')
        # 撤回一旦生效，可借读数必然少于 12；active 暂估引用的读数全部在有效集合内
        valid_ids = {r['id'] for r in self.s.available_readings(src)}
        for row in self.s.conn.execute(
                "SELECT id FROM provisional_estimates WHERE status='active'").fetchall():
            used = {r[0] for r in self.s.conn.execute(
                'SELECT count_id FROM provisional_estimate_readings WHERE estimate_id=?',
                (row['id'],)).fetchall()}
            self.assertTrue(used.issubset(valid_ids))


if __name__ == '__main__':
    unittest.main()
