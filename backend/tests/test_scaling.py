"""暂估折算口径的纯单元测试（不依赖数据库）。"""

import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

os.environ.setdefault('DATABASE_URL', 'postgresql://u:p@localhost:5432/db')
os.environ.setdefault('REDIS_URL', 'redis://localhost:6379/0')
os.environ.setdefault('JWT_SECRET', 'test-secret')
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi import HTTPException  # noqa: E402

from src.algorithm import stl_rolling_forecast  # noqa: E402
from src.provisional_service import estimate_with_borrow, scale_peak  # noqa: E402


def make_points(n=12, base=40):
    start = datetime(2026, 11, 21, 6, 0, tzinfo=timezone.utc)
    return [
        {'counted_at': start + timedelta(minutes=10 * i),
         'bag_count': base + i * 12 + (i % 4) * 3}
        for i in range(n)
    ]


class ScalingTests(unittest.TestCase):
    def test_ratio_is_planned_bags_proportion(self):
        raw = {'peak_bags': 200.0, 'predicted_next_4': [100.0, 200.0], 'trend_per_hour': 8.0}
        ratio, scaled = scale_peak(raw, target_planned_bags=270, source_planned_bags=360)
        self.assertAlmostEqual(ratio, 0.75)
        self.assertAlmostEqual(scaled['peak_bags'], 150.0)
        self.assertEqual(scaled['predicted_next_4'], [75.0, 150.0])
        self.assertAlmostEqual(scaled['trend_per_hour'], 6.0)

    def test_scaling_rounds_to_two_decimals(self):
        raw = {'peak_bags': 100.0, 'predicted_next_4': [], 'trend_per_hour': 1.0}
        _, scaled = scale_peak(raw, 100, 3)
        # 100 * (100/3) = 3333.333... -> 3333.33
        self.assertEqual(scaled['peak_bags'], 3333.33)

    def test_zero_source_planned_bags_rejected(self):
        raw = {'peak_bags': 100.0, 'predicted_next_4': [], 'trend_per_hour': 1.0}
        with self.assertRaises(HTTPException) as ctx:
            scale_peak(raw, 100, 0)
        self.assertEqual(ctx.exception.status_code, 409)

    def test_borrow_estimate_uses_same_snapshot_then_scales(self):
        points = make_points()
        raw, ratio, scaled = estimate_with_borrow(points, 270, 360, 8)
        # 原始峰值与算法直跑一致
        direct = stl_rolling_forecast(points, 8)
        self.assertEqual(raw['snapshot_hash'], direct['snapshot_hash'])
        self.assertAlmostEqual(raw['peak_bags'], direct['peak_bags'])
        # 暂估峰值 = 原始峰值 * 折算系数
        self.assertAlmostEqual(ratio, 0.75)
        self.assertAlmostEqual(scaled['peak_bags'], round(direct['peak_bags'] * 0.75, 2))
        self.assertLessEqual(scaled['peak_bags'], raw['peak_bags'])

    def test_borrow_estimate_requires_min_points(self):
        with self.assertRaises(ValueError):
            estimate_with_borrow(make_points(n=6), 270, 360, 8)


if __name__ == '__main__':
    unittest.main()
