import os
import time
from contextlib import contextmanager
import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.environ['DATABASE_URL']

@contextmanager
def connect():
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        yield conn

def wait_for_db():
    for _ in range(40):
        try:
            with connect() as conn:
                conn.execute('SELECT 1')
                return
        except Exception:
            time.sleep(1)
    raise RuntimeError('database unavailable')

def migrate():
    with connect() as conn:
        conn.execute('''
        CREATE TABLE IF NOT EXISTS users (
          id SERIAL PRIMARY KEY, username TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL,
          display_name TEXT NOT NULL, role TEXT NOT NULL CHECK (role IN ('operator','supervisor','viewer')), active BOOLEAN NOT NULL DEFAULT TRUE
        );
        CREATE TABLE IF NOT EXISTS flight_waves (
          id SERIAL PRIMARY KEY, code TEXT UNIQUE NOT NULL, terminal TEXT NOT NULL, arrival_start TIMESTAMPTZ NOT NULL,
          arrival_end TIMESTAMPTZ NOT NULL, flight_count INT NOT NULL CHECK (flight_count > 0), planned_bags INT NOT NULL CHECK (planned_bags >= 0),
          cancellation_state TEXT NOT NULL DEFAULT 'normal' CHECK (cancellation_state IN ('normal','cancelled_unmarked','cancelled_marked','diverted_marked')),
          status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','counting','forecasted','closed')), notes TEXT DEFAULT '', created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS belt_counts (
          id SERIAL PRIMARY KEY, wave_id INT NOT NULL REFERENCES flight_waves(id) ON DELETE CASCADE, belt_code TEXT NOT NULL,
          counted_at TIMESTAMPTZ NOT NULL, bag_count INT NOT NULL CHECK (bag_count >= 0), scan_source TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'accepted' CHECK (status IN ('accepted','voided')), UNIQUE(wave_id,belt_code,counted_at)
        );
        CREATE TABLE IF NOT EXISTS peak_forecasts (
          id SERIAL PRIMARY KEY, wave_id INT NOT NULL REFERENCES flight_waves(id), status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','running','ready','blocked','failed','published')),
          peak_bags NUMERIC(10,2), forecast_at TIMESTAMPTZ, seasonal_strength NUMERIC(8,4), trend_per_hour NUMERIC(8,4),
          window_size INT, snapshot_hash TEXT, parameters JSONB NOT NULL DEFAULT '{}'::jsonb, result JSONB NOT NULL DEFAULT '{}'::jsonb,
          failure_reason TEXT, created_by INT REFERENCES users(id), attempts INT NOT NULL DEFAULT 0, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS staff_shifts (
          id SERIAL PRIMARY KEY, shift_code TEXT UNIQUE NOT NULL, wave_id INT REFERENCES flight_waves(id), start_at TIMESTAMPTZ NOT NULL,
          end_at TIMESTAMPTZ NOT NULL, planned_staff INT NOT NULL CHECK (planned_staff >= 0), capacity INT NOT NULL CHECK (capacity > 0),
          status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','needs_confirmation','confirmed','over_capacity','completed')),
          confirmation_note TEXT DEFAULT '', confirmed_by INT REFERENCES users(id), confirmed_at TIMESTAMPTZ
        );
        CREATE TABLE IF NOT EXISTS congestion_reviews (
          id SERIAL PRIMARY KEY, wave_id INT NOT NULL REFERENCES flight_waves(id), forecast_id INT REFERENCES peak_forecasts(id),
          observed_peak INT NOT NULL CHECK (observed_peak >= 0), queue_minutes INT NOT NULL CHECK (queue_minutes >= 0), cause TEXT NOT NULL,
          action TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','actioned','closed')), reviewed_by INT REFERENCES users(id), reviewed_at TIMESTAMPTZ
        );
        CREATE TABLE IF NOT EXISTS count_withdrawals (
          id SERIAL PRIMARY KEY, wave_id INT NOT NULL REFERENCES flight_waves(id),
          belt_code TEXT, window_start TIMESTAMPTZ NOT NULL, window_end TIMESTAMPTZ NOT NULL,
          reason TEXT NOT NULL, withdrawn_by INT REFERENCES users(id),
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CHECK (window_end > window_start)
        );
        CREATE INDEX IF NOT EXISTS idx_count_withdrawals_wave ON count_withdrawals(wave_id);
        CREATE TABLE IF NOT EXISTS provisional_estimates (
          id SERIAL PRIMARY KEY, wave_id INT NOT NULL REFERENCES flight_waves(id),
          source_wave_id INT NOT NULL REFERENCES flight_waves(id),
          status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','invalidated')),
          source_points INT NOT NULL CHECK (source_points >= 0),
          planned_bags_ratio NUMERIC(12,6) NOT NULL,
          raw_peak_bags NUMERIC(10,2) NOT NULL,
          estimated_peak_bags NUMERIC(10,2) NOT NULL,
          source_window_start TIMESTAMPTZ NOT NULL,
          source_window_end TIMESTAMPTZ NOT NULL,
          window_size INT NOT NULL, snapshot_hash TEXT NOT NULL,
          result JSONB NOT NULL DEFAULT '{}'::jsonb,
          invalidation_reason TEXT, invalidated_at TIMESTAMPTZ,
          withdrawal_id INT REFERENCES count_withdrawals(id),
          created_by INT REFERENCES users(id), created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS idx_provisional_estimates_wave ON provisional_estimates(wave_id);
        CREATE TABLE IF NOT EXISTS provisional_estimate_readings (
          estimate_id INT NOT NULL REFERENCES provisional_estimates(id) ON DELETE CASCADE,
          count_id INT NOT NULL REFERENCES belt_counts(id),
          PRIMARY KEY (estimate_id, count_id)
        );
        CREATE INDEX IF NOT EXISTS idx_per_count ON provisional_estimate_readings(count_id);
        CREATE TABLE IF NOT EXISTS audit_logs (
          id SERIAL PRIMARY KEY, actor_id INT REFERENCES users(id), action TEXT NOT NULL, entity_type TEXT NOT NULL, entity_id INT, detail JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        ''')

def audit(conn, actor_id, action, entity_type, entity_id=None, detail=None):
    conn.execute('INSERT INTO audit_logs(actor_id,action,entity_type,entity_id,detail) VALUES (%s,%s,%s,%s,%s::jsonb)', (actor_id, action, entity_type, entity_id, __import__('json').dumps(detail or {}, ensure_ascii=False)))
