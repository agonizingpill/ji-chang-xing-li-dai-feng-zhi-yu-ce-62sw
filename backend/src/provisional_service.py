"""借读数暂估峰：同航站楼已结束波次读数 + 计划行李量折算。

业务口径：
- 本波有效计数不足 8 个时，才允许借同航站楼已结束（closed）波次的有效读数做暂估。
- 暂估仅供值班参考，不能进入发布链路；本波计数够 8 个后走正式预测。
- 主管撤回供数波次的故障时段后，所有借过该时段读数的有效暂估立即失效，
  重算必须重新挑选未被撤回的有效读数。
- 撤回与暂估并发时，依赖 PostgreSQL 行锁（波次行 + 计数行）保证：
  撤回提交之后，不可能再产生引用了被撤回读数的有效暂估。
"""

import json

from fastapi import HTTPException

from .algorithm import stl_rolling_forecast
from .db import audit, connect

MIN_POINTS = 8


def lendable_waves(target_wave_id):
    """可向目标波次供数的同航站楼已结束波次，以及当前仍可借的有效读数概况。"""
    with connect() as conn:
        target = conn.execute('SELECT * FROM flight_waves WHERE id=%s', (target_wave_id,)).fetchone()
        if not target:
            raise HTTPException(404, '航班波次不存在')
        rows = conn.execute(
            """
            SELECT w.id, w.code, w.terminal, w.arrival_start, w.arrival_end, w.planned_bags,
                   COUNT(b.id) AS accepted_points,
                   COUNT(b.id) FILTER (WHERE x.id IS NULL) AS available_points,
                   COALESCE(MIN(b.counted_at) FILTER (WHERE x.id IS NULL), MIN(b.counted_at)) AS window_start,
                   MAX(b.counted_at) FILTER (WHERE x.id IS NULL) AS window_end
            FROM flight_waves w
            JOIN belt_counts b ON b.wave_id = w.id AND b.status = 'accepted'
            LEFT JOIN count_withdrawals x
                   ON x.wave_id = w.id
                  AND (x.belt_code IS NULL OR x.belt_code = b.belt_code)
                  AND b.counted_at >= x.window_start AND b.counted_at < x.window_end
            WHERE w.terminal = %s AND w.status = 'closed' AND w.id <> %s
            GROUP BY w.id
            HAVING COUNT(b.id) FILTER (WHERE x.id IS NULL) >= %s
            ORDER BY w.arrival_end DESC, w.code
            """,
            (target['terminal'], target_wave_id, MIN_POINTS),
        ).fetchall()
        return [dict(row) for row in rows]


def scale_peak(raw, target_planned_bags, source_planned_bags):
    """按计划行李量把供数波次的原始预测折算到目标波次。"""
    if source_planned_bags <= 0:
        raise HTTPException(409, '供数波次计划行李量为 0，无法按计划行李量折算')
    ratio = target_planned_bags / source_planned_bags
    scaled = {
        'peak_bags': round(raw['peak_bags'] * ratio, 2),
        'predicted_next_4': [round(v * ratio, 2) for v in raw['predicted_next_4']],
        'trend_per_hour': round(raw['trend_per_hour'] * ratio, 4),
    }
    return ratio, scaled


def estimate_with_borrow(points, target_planned_bags, source_planned_bags, window_size):
    """在供数读数上跑 STL 预测，再按计划行李量折算到目标波次。"""
    raw = stl_rolling_forecast(points, window_size)
    ratio, scaled = scale_peak(raw, target_planned_bags, source_planned_bags)
    return raw, ratio, scaled


def create_estimate(target_wave_id, source_wave_id, window_size, user):
    with connect() as conn:
        # 固定加锁顺序（目标 -> 供数，按 id 排序）避免与撤回事务死锁。
        first_id, second_id = sorted((target_wave_id, source_wave_id))
        conn.execute('SELECT id FROM flight_waves WHERE id=%s FOR UPDATE', (first_id,))
        if second_id != first_id:
            conn.execute('SELECT id FROM flight_waves WHERE id=%s FOR UPDATE', (second_id,))

        target = conn.execute('SELECT * FROM flight_waves WHERE id=%s', (target_wave_id,)).fetchone()
        if not target:
            raise HTTPException(404, '航班波次不存在')
        source = conn.execute('SELECT * FROM flight_waves WHERE id=%s', (source_wave_id,)).fetchone()
        if not source:
            raise HTTPException(404, '供数波次不存在')
        if source['id'] == target['id']:
            raise HTTPException(409, '不能借用本波自己的读数')
        if source['terminal'] != target['terminal']:
            raise HTTPException(409, '只能借用同航站楼波次的读数')
        if source['status'] != 'closed':
            raise HTTPException(409, '只能借用已结束波次的读数')

        own_points = conn.execute(
            "SELECT COUNT(*) c FROM belt_counts WHERE wave_id=%s AND status='accepted'",
            (target_wave_id,),
        ).fetchone()['c']
        if own_points >= MIN_POINTS:
            raise HTTPException(409, '本波计数已足够，请直接发起正式预测，不再生成暂估')

        # 锁定候选计数行：与撤回事务的 SELECT ... FOR UPDATE 互斥，
        # 保证拿到的读数集合在本事务提交前不会被撤回。
        points = conn.execute(
            """
            SELECT b.id, b.counted_at, b.bag_count
            FROM belt_counts b
            WHERE b.wave_id = %s AND b.status = 'accepted'
              AND NOT EXISTS (
                SELECT 1 FROM count_withdrawals x
                WHERE x.wave_id = b.wave_id
                  AND (x.belt_code IS NULL OR x.belt_code = b.belt_code)
                  AND b.counted_at >= x.window_start AND b.counted_at < x.window_end
              )
            ORDER BY b.counted_at
            FOR UPDATE OF b
            """,
            (source_wave_id,),
        ).fetchall()
        if len(points) < MIN_POINTS:
            raise HTTPException(409, '供数波次可借的有效读数不足 8 个，请更换供数波次')

        raw, ratio, scaled = estimate_with_borrow(
            points, target['planned_bags'], source['planned_bags'], window_size
        )
        result_payload = {
            'method': 'STL decomposition + rolling linear regression (borrowed)',
            'source_wave_id': source_wave_id,
            'source_wave_code': source['code'],
            'planned_bags_ratio': ratio,
            'raw_peak_bags': raw['peak_bags'],
            'raw_predicted_next_4': raw['predicted_next_4'],
            'predicted_next_4': scaled['predicted_next_4'],
            'seasonal_strength': raw['seasonal_strength'],
            'period': raw['period'],
            'input_points': raw['input_points'],
            'reference_only': True,
            'publishable': False,
        }
        row = conn.execute(
            """
            INSERT INTO provisional_estimates(
                wave_id, source_wave_id, status, source_points, planned_bags_ratio,
                raw_peak_bags, estimated_peak_bags, source_window_start, source_window_end,
                window_size, snapshot_hash, result, created_by)
            VALUES (%s,%s,'active',%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
            RETURNING *
            """,
            (target_wave_id, source_wave_id, len(points), ratio,
             raw['peak_bags'], scaled['peak_bags'],
             points[0]['counted_at'], points[-1]['counted_at'],
             window_size, raw['snapshot_hash'],
             json.dumps(result_payload, ensure_ascii=False), user['id']),
        ).fetchone()
        conn.executemany(
            'INSERT INTO provisional_estimate_readings(estimate_id,count_id) VALUES (%s,%s) '
            'ON CONFLICT DO NOTHING',
            [(row['id'], p['id']) for p in points],
        )
        audit(conn, user['id'], 'borrow_estimate', 'provisional_estimate', row['id'],
              {'wave_id': target_wave_id, 'source_wave_id': source_wave_id,
               'source_points': len(points), 'ratio': ratio,
               'estimated_peak_bags': scaled['peak_bags']})
        conn.commit()
        return dict(row)


def list_estimates(wave_id=None):
    sql = """
        SELECT e.*, w.code AS wave_code, w.terminal,
               sw.code AS source_wave_code, cw.reason AS withdrawal_reason_text
        FROM provisional_estimates e
        JOIN flight_waves w ON w.id = e.wave_id
        JOIN flight_waves sw ON sw.id = e.source_wave_id
        LEFT JOIN count_withdrawals cw ON cw.id = e.withdrawal_id
    """
    params = ()
    if wave_id is not None:
        sql += ' WHERE e.wave_id=%s'
        params = (wave_id,)
    sql += ' ORDER BY e.id DESC'
    with connect() as conn:
        rows = conn.execute(sql, params).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            readings = conn.execute(
                """
                SELECT r.count_id, b.belt_code, b.counted_at, b.bag_count
                FROM provisional_estimate_readings r
                JOIN belt_counts b ON b.id = r.count_id
                WHERE r.estimate_id=%s ORDER BY b.counted_at
                """,
                (row['id'],),
            ).fetchall()
            item['readings'] = [dict(r) for r in readings]
            result.append(item)
        return result


def list_withdrawals(wave_id=None):
    sql = """
        SELECT x.*, w.code AS wave_code, w.terminal, u.display_name AS withdrawn_by_name,
               (SELECT COUNT(*) FROM provisional_estimates pe
                 WHERE pe.withdrawal_id = x.id) AS affected_estimates
        FROM count_withdrawals x
        JOIN flight_waves w ON w.id = x.wave_id
        LEFT JOIN users u ON u.id = x.withdrawn_by
    """
    params = ()
    if wave_id is not None:
        sql += ' WHERE x.wave_id=%s'
        params = (wave_id,)
    sql += ' ORDER BY x.id DESC'
    with connect() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def withdraw_window(wave_id, belt_code, window_start, window_end, reason, user):
    if window_end <= window_start:
        raise HTTPException(400, '故障结束时间必须晚于开始时间')
    with connect() as conn:
        # 锁定供数波次行：与暂估事务互斥，且与并发撤回串行化。
        wave = conn.execute('SELECT * FROM flight_waves WHERE id=%s FOR UPDATE', (wave_id,)).fetchone()
        if not wave:
            raise HTTPException(404, '供数波次不存在')

        # 命中该故障时段的计数行加锁；暂估事务创建时同样锁定这些行，
        # 因此两条事务在同一批读数上不可能同时通过。
        hit_counts = conn.execute(
            """
            SELECT b.id FROM belt_counts b
            WHERE b.wave_id=%s AND b.status='accepted'
              AND (%s IS NULL OR b.belt_code=%s)
              AND b.counted_at >= %s AND b.counted_at < %s
            FOR UPDATE
            """,
            (wave_id, belt_code, belt_code, window_start, window_end),
        ).fetchall()
        hit_ids = [r['id'] for r in hit_counts]

        withdrawal = conn.execute(
            """
            INSERT INTO count_withdrawals(wave_id,belt_code,window_start,window_end,reason,withdrawn_by)
            VALUES (%s,%s,%s,%s,%s,%s) RETURNING *
            """,
            (wave_id, belt_code, window_start, window_end, reason, user['id']),
        ).fetchone()

        affected = []
        if hit_ids:
            # 所有借过该时段读数且仍有效的暂估 → 失效。
            affected = conn.execute(
                """
                SELECT DISTINCT pe.id
                FROM provisional_estimate_readings pr
                JOIN provisional_estimates pe ON pe.id = pr.estimate_id
                WHERE pr.count_id = ANY(%s) AND pe.status='active'
                FOR UPDATE OF pe
                """,
                (hit_ids,),
            ).fetchall()
            scope = f'行李带 {belt_code} ' if belt_code else '全部行李带 '
            reason_text = (
                f"供数波次 {wave['code']} {scope}{window_start:%Y-%m-%d %H:%M} 至 "
                f"{window_end:%Y-%m-%d %H:%M} 读数被主管撤回：{reason}"
            )
            for est in affected:
                conn.execute(
                    """
                    UPDATE provisional_estimates
                    SET status='invalidated', invalidation_reason=%s,
                        invalidated_at=now(), withdrawal_id=%s
                    WHERE id=%s
                    """,
                    (reason_text, withdrawal['id'], est['id']),
                )
        audit(conn, user['id'], 'withdraw', 'count_withdrawal', withdrawal['id'],
              {'wave_id': wave_id, 'belt_code': belt_code,
               'window_start': window_start.isoformat(), 'window_end': window_end.isoformat(),
               'reason': reason, 'invalidated_estimates': [e['id'] for e in affected]})
        conn.commit()
        return dict(withdrawal), [e['id'] for e in affected]
