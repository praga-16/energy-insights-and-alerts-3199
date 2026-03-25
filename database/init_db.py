#!/usr/bin/env python3
"""Initialize SQLite database for the energy-insights-and-alerts database container.

This script is intentionally self-contained and compatible with the existing
database helper scripts (backup/restore, db_shell.py, db_connection.txt guidance).

It will:
- Create/upgrade the SQLite schema for the energy domain:
  sites/meters, raw readings, hourly/daily aggregates, anomaly events,
  alert rules, and alert delivery state.
- Seed realistic demo data if the database is empty (idempotent inserts).

Notes:
- SQLite foreign keys are enabled for the connection used in this script.
- For compatibility, the legacy `app_info` and `users` tables are retained.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta, timezone
from math import sin, pi


DB_NAME = "myapp.db"
DB_USER = "kaviasqlite"  # Not used for SQLite, but kept for consistency
DB_PASSWORD = "kaviadefaultpassword"  # Not used for SQLite, but kept for consistency
DB_PORT = "5000"  # Not used for SQLite, but kept for consistency


def _utc_now() -> datetime:
    """Return current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


def _to_iso_z(dt: datetime) -> str:
    """Convert a timezone-aware datetime to ISO-8601 with Z suffix."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _execute(cursor: sqlite3.Cursor, sql: str) -> None:
    """Execute a SQL statement (DDL/DML), raising on error with context."""
    try:
        cursor.execute(sql)
    except sqlite3.Error as e:
        raise RuntimeError(f"SQLite error executing SQL:\n{sql}\nError: {e}") from e


def _execute_params(cursor: sqlite3.Cursor, sql: str, params: tuple) -> None:
    """Execute a parametrized SQL statement."""
    try:
        cursor.execute(sql, params)
    except sqlite3.Error as e:
        raise RuntimeError(f"SQLite error executing SQL:\n{sql}\nParams: {params}\nError: {e}") from e


def _create_schema(cursor: sqlite3.Cursor) -> None:
    """Create the database schema (idempotent)."""
    # Legacy/support tables kept for compatibility with the template
    _execute(
        cursor,
        """
        CREATE TABLE IF NOT EXISTS app_info (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT UNIQUE NOT NULL,
            value TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )

    _execute(
        cursor,
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )

    # Energy domain tables
    _execute(
        cursor,
        """
        CREATE TABLE IF NOT EXISTS sites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            address_line1 TEXT,
            city TEXT,
            state TEXT,
            postal_code TEXT,
            timezone TEXT NOT NULL DEFAULT 'UTC',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )

    _execute(
        cursor,
        """
        CREATE TABLE IF NOT EXISTS meters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            meter_type TEXT NOT NULL DEFAULT 'electricity',
            unit TEXT NOT NULL DEFAULT 'kWh',
            is_active INTEGER NOT NULL DEFAULT 1,
            installed_at TIMESTAMP,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (site_id) REFERENCES sites(id) ON DELETE CASCADE
        )
        """,
    )

    # Raw readings: one row per interval.
    _execute(
        cursor,
        """
        CREATE TABLE IF NOT EXISTS meter_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            meter_id INTEGER NOT NULL,
            ts TIMESTAMP NOT NULL,                  -- ISO-8601 string recommended
            value REAL NOT NULL,                    -- consumption for the interval
            quality TEXT NOT NULL DEFAULT 'measured',-- measured|estimated|missing|corrected
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (meter_id) REFERENCES meters(id) ON DELETE CASCADE,
            UNIQUE (meter_id, ts)
        )
        """,
    )

    # Aggregates: store precomputed rollups for fast analytics.
    _execute(
        cursor,
        """
        CREATE TABLE IF NOT EXISTS meter_aggregates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            meter_id INTEGER NOT NULL,
            period TEXT NOT NULL,                   -- 'hour' or 'day'
            period_start TIMESTAMP NOT NULL,         -- ISO-8601
            period_end TIMESTAMP NOT NULL,           -- ISO-8601
            total_value REAL NOT NULL,               -- sum within period
            avg_value REAL,                          -- average reading value
            min_value REAL,
            max_value REAL,
            sample_count INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (meter_id) REFERENCES meters(id) ON DELETE CASCADE,
            UNIQUE (meter_id, period, period_start)
        )
        """,
    )

    # Anomaly events: detected abnormal usage patterns.
    _execute(
        cursor,
        """
        CREATE TABLE IF NOT EXISTS anomaly_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            meter_id INTEGER NOT NULL,
            start_ts TIMESTAMP NOT NULL,
            end_ts TIMESTAMP,
            anomaly_type TEXT NOT NULL,             -- spike|drop|flatline|drift|missing_data
            severity TEXT NOT NULL,                 -- low|medium|high|critical
            score REAL,                              -- e.g., z-score or model score
            baseline_value REAL,
            observed_value REAL,
            details_json TEXT,                       -- JSON blob for model/debug info
            status TEXT NOT NULL DEFAULT 'open',     -- open|acknowledged|resolved
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (meter_id) REFERENCES meters(id) ON DELETE CASCADE
        )
        """,
    )

    # Alert rules: per-site/per-meter notification rules.
    _execute(
        cursor,
        """
        CREATE TABLE IF NOT EXISTS alert_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER NOT NULL,
            meter_id INTEGER,                        -- optional meter-level rule
            name TEXT NOT NULL,
            is_enabled INTEGER NOT NULL DEFAULT 1,
            severity_threshold TEXT NOT NULL DEFAULT 'high', -- minimum severity to alert
            cooldown_minutes INTEGER NOT NULL DEFAULT 60,     -- avoid spam
            channels_json TEXT NOT NULL DEFAULT '["in_app"]', -- e.g., ["in_app","email"]
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (site_id) REFERENCES sites(id) ON DELETE CASCADE,
            FOREIGN KEY (meter_id) REFERENCES meters(id) ON DELETE CASCADE
        )
        """,
    )

    # Alert delivery state: tracks which anomaly events were notified and how.
    _execute(
        cursor,
        """
        CREATE TABLE IF NOT EXISTS alert_deliveries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anomaly_event_id INTEGER NOT NULL,
            alert_rule_id INTEGER,
            channel TEXT NOT NULL,                   -- in_app|email|sms|webhook
            delivery_status TEXT NOT NULL,           -- pending|sent|failed|suppressed
            attempt_count INTEGER NOT NULL DEFAULT 0,
            last_attempt_at TIMESTAMP,
            delivered_at TIMESTAMP,
            error_message TEXT,
            dedupe_key TEXT,                         -- optional idempotency key
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (anomaly_event_id) REFERENCES anomaly_events(id) ON DELETE CASCADE,
            FOREIGN KEY (alert_rule_id) REFERENCES alert_rules(id) ON DELETE SET NULL,
            UNIQUE (anomaly_event_id, channel)
        )
        """,
    )

    # Helpful indexes for time-series queries
    _execute(cursor, "CREATE INDEX IF NOT EXISTS idx_meters_site_id ON meters(site_id)")
    _execute(cursor, "CREATE INDEX IF NOT EXISTS idx_readings_meter_ts ON meter_readings(meter_id, ts)")
    _execute(cursor, "CREATE INDEX IF NOT EXISTS idx_aggs_meter_period_start ON meter_aggregates(meter_id, period, period_start)")
    _execute(cursor, "CREATE INDEX IF NOT EXISTS idx_anom_meter_start ON anomaly_events(meter_id, start_ts)")
    _execute(cursor, "CREATE INDEX IF NOT EXISTS idx_alert_rules_site_meter ON alert_rules(site_id, meter_id)")
    _execute(cursor, "CREATE INDEX IF NOT EXISTS idx_alert_deliv_status ON alert_deliveries(delivery_status)")


def _seed_app_info(cursor: sqlite3.Cursor) -> None:
    """Seed app metadata (idempotent)."""
    # Preserve legacy keys, update to energy app.
    _execute_params(
        cursor,
        "INSERT OR REPLACE INTO app_info (key, value) VALUES (?, ?)",
        ("project_name", "energy-insights-and-alerts"),
    )
    _execute_params(
        cursor,
        "INSERT OR REPLACE INTO app_info (key, value) VALUES (?, ?)",
        ("version", "0.2.0"),
    )
    _execute_params(
        cursor,
        "INSERT OR REPLACE INTO app_info (key, value) VALUES (?, ?)",
        ("author", "Kavia"),
    )
    _execute_params(
        cursor,
        "INSERT OR REPLACE INTO app_info (key, value) VALUES (?, ?)",
        (
            "description",
            "Energy analytics schema with readings, aggregates, anomalies, and alert delivery state.",
        ),
    )


def _seed_users(cursor: sqlite3.Cursor) -> None:
    """Seed a couple demo users (idempotent)."""
    users = [
        ("ops_manager", "ops.manager@acme.example"),
        ("energy_analyst", "energy.analyst@acme.example"),
    ]
    for username, email in users:
        _execute_params(
            cursor,
            "INSERT OR IGNORE INTO users (username, email) VALUES (?, ?)",
            (username, email),
        )


def _seed_energy_domain(cursor: sqlite3.Cursor) -> None:
    """Seed sites/meters/readings/anomalies (only if empty)."""
    cursor.execute("SELECT COUNT(*) FROM sites")
    if cursor.fetchone()[0] > 0:
        print("Seed: sites already exist, skipping energy-domain seed.")
        return

    now = _utc_now().replace(minute=0, second=0, microsecond=0)
    start = now - timedelta(days=7)

    # Sites
    sites = [
        ("Acme HQ - Midtown", "100 Market St", "San Francisco", "CA", "94105", "America/Los_Angeles"),
        ("Acme Warehouse - East Bay", "42 Industrial Way", "Oakland", "CA", "94607", "America/Los_Angeles"),
    ]
    site_ids: list[int] = []
    for name, addr, city, state, postal, tz in sites:
        _execute_params(
            cursor,
            """
            INSERT INTO sites (name, address_line1, city, state, postal_code, timezone)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (name, addr, city, state, postal, tz),
        )
        site_ids.append(int(cursor.lastrowid))

    # Meters: 2 per site
    meters = [
        (site_ids[0], "Main Building - Electric", "electricity", "kWh"),
        (site_ids[0], "Main Building - HVAC Submeter", "electricity", "kWh"),
        (site_ids[1], "Warehouse - Electric", "electricity", "kWh"),
        (site_ids[1], "Warehouse - Cold Storage", "electricity", "kWh"),
    ]
    meter_ids: list[int] = []
    for site_id, name, meter_type, unit in meters:
        _execute_params(
            cursor,
            """
            INSERT INTO meters (site_id, name, meter_type, unit, installed_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (site_id, name, meter_type, unit, _to_iso_z(start - timedelta(days=60))),
        )
        meter_ids.append(int(cursor.lastrowid))

    # Create readings: hourly for 7 days
    # Patterns:
    # - HQ Main: weekday daytime higher, nights lower; one spike anomaly
    # - HQ HVAC: correlated but lower amplitude; one flatline period
    # - Warehouse: steadier baseline; one drop anomaly
    # - Cold storage: fairly constant with mild oscillation
    spike_ts = (now - timedelta(days=2)).replace(hour=14)
    drop_ts = (now - timedelta(days=3)).replace(hour=3)
    flatline_start = (now - timedelta(days=1)).replace(hour=6)
    flatline_end = flatline_start + timedelta(hours=6)

    def base_profile(dt: datetime) -> float:
        # 24h sinusoidal with peak mid-afternoon
        hour = dt.hour + dt.minute / 60.0
        return 1.0 + 0.4 * sin((hour - 14) * 2 * pi / 24)

    def weekday_multiplier(dt: datetime) -> float:
        # Mon-Fri -> 1.0, weekend -> 0.8
        return 1.0 if dt.weekday() < 5 else 0.8

    for meter_idx, meter_id in enumerate(meter_ids):
        dt = start
        while dt < now:
            iso_ts = _to_iso_z(dt)

            if meter_idx == 0:
                # HQ main building
                val = 120.0 * base_profile(dt) * weekday_multiplier(dt)
                if dt == spike_ts:
                    val *= 2.2  # spike
                quality = "measured"

            elif meter_idx == 1:
                # HQ HVAC submeter
                val = 45.0 * base_profile(dt) * weekday_multiplier(dt)
                # Flatline: stuck sensor, constant value
                if flatline_start <= dt < flatline_end:
                    val = 20.0
                    quality = "measured"
                else:
                    quality = "measured"

            elif meter_idx == 2:
                # Warehouse electric: steadier with small diurnal cycle
                val = 80.0 * (1.0 + 0.15 * sin((dt.hour - 12) * 2 * pi / 24))
                if dt == drop_ts:
                    val *= 0.25  # sudden drop
                quality = "measured"

            else:
                # Cold storage: mostly constant, small oscillation
                val = 60.0 * (1.0 + 0.05 * sin((dt.hour - 6) * 2 * pi / 24))
                quality = "measured"

            _execute_params(
                cursor,
                """
                INSERT OR IGNORE INTO meter_readings (meter_id, ts, value, quality)
                VALUES (?, ?, ?, ?)
                """,
                (meter_id, iso_ts, float(round(val, 3)), quality),
            )
            dt += timedelta(hours=1)

    # Aggregates: hourly (mirror readings) and daily totals
    # Hourly aggregates (sum=reading)
    cursor.execute("SELECT meter_id, ts, value FROM meter_readings ORDER BY meter_id, ts")
    for meter_id, ts, value in cursor.fetchall():
        # period_end is next hour for hourly
        ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        period_start = ts
        period_end = _to_iso_z(ts_dt + timedelta(hours=1))
        _execute_params(
            cursor,
            """
            INSERT OR IGNORE INTO meter_aggregates
              (meter_id, period, period_start, period_end, total_value, avg_value, min_value, max_value, sample_count)
            VALUES (?, 'hour', ?, ?, ?, ?, ?, ?, 1)
            """,
            (meter_id, period_start, period_end, value, value, value, value),
        )

    # Daily aggregates
    for meter_id in meter_ids:
        day = start.replace(hour=0)
        while day < now:
            day_start = day
            day_end = day_start + timedelta(days=1)
            cursor.execute(
                """
                SELECT
                  COUNT(*),
                  SUM(value),
                  AVG(value),
                  MIN(value),
                  MAX(value)
                FROM meter_readings
                WHERE meter_id = ?
                  AND ts >= ?
                  AND ts < ?
                """,
                (meter_id, _to_iso_z(day_start), _to_iso_z(day_end)),
            )
            cnt, s, avg, mn, mx = cursor.fetchone()
            if cnt and s is not None:
                _execute_params(
                    cursor,
                    """
                    INSERT OR IGNORE INTO meter_aggregates
                      (meter_id, period, period_start, period_end, total_value, avg_value, min_value, max_value, sample_count)
                    VALUES (?, 'day', ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        meter_id,
                        _to_iso_z(day_start),
                        _to_iso_z(day_end),
                        float(round(s, 3)),
                        float(round(avg, 3)) if avg is not None else None,
                        float(round(mn, 3)) if mn is not None else None,
                        float(round(mx, 3)) if mx is not None else None,
                        int(cnt),
                    ),
                )
            day += timedelta(days=1)

    # Seed anomaly events corresponding to the patterns above
    # HQ spike anomaly
    cursor.execute("SELECT id FROM meters WHERE name = ?", ("Main Building - Electric",))
    hq_main_id = cursor.fetchone()[0]
    _execute_params(
        cursor,
        """
        INSERT INTO anomaly_events
          (meter_id, start_ts, end_ts, anomaly_type, severity, score, baseline_value, observed_value, details_json, status)
        VALUES (?, ?, ?, 'spike', 'high', ?, ?, ?, ?, 'open')
        """,
        (
            hq_main_id,
            _to_iso_z(spike_ts),
            _to_iso_z(spike_ts + timedelta(hours=1)),
            3.4,
            140.0,
            310.0,
            '{"model":"zscore","z":3.4,"note":"Unusual mid-afternoon usage spike."}',
        ),
    )
    spike_event_id = int(cursor.lastrowid)

    # Warehouse drop anomaly
    cursor.execute("SELECT id FROM meters WHERE name = ?", ("Warehouse - Electric",))
    wh_id = cursor.fetchone()[0]
    _execute_params(
        cursor,
        """
        INSERT INTO anomaly_events
          (meter_id, start_ts, end_ts, anomaly_type, severity, score, baseline_value, observed_value, details_json, status)
        VALUES (?, ?, ?, 'drop', 'medium', ?, ?, ?, ?, 'open')
        """,
        (
            wh_id,
            _to_iso_z(drop_ts),
            _to_iso_z(drop_ts + timedelta(hours=1)),
            2.1,
            78.0,
            20.0,
            '{"model":"zscore","z":-2.1,"note":"Sudden consumption drop (possible outage or meter issue)."}',
        ),
    )
    drop_event_id = int(cursor.lastrowid)

    # HVAC flatline anomaly
    cursor.execute("SELECT id FROM meters WHERE name = ?", ("Main Building - HVAC Submeter",))
    hvac_id = cursor.fetchone()[0]
    _execute_params(
        cursor,
        """
        INSERT INTO anomaly_events
          (meter_id, start_ts, end_ts, anomaly_type, severity, score, baseline_value, observed_value, details_json, status)
        VALUES (?, ?, ?, 'flatline', 'high', ?, ?, ?, ?, 'acknowledged')
        """,
        (
            hvac_id,
            _to_iso_z(flatline_start),
            _to_iso_z(flatline_end),
            3.0,
            38.0,
            20.0,
            '{"model":"heuristic","rule":"flatline_6h","note":"HVAC submeter stuck at constant value."}',
        ),
    )
    flat_event_id = int(cursor.lastrowid)

    # Alert rules: one per site, plus a meter-specific rule for HQ main
    _execute_params(
        cursor,
        """
        INSERT INTO alert_rules (site_id, meter_id, name, severity_threshold, cooldown_minutes, channels_json)
        VALUES (?, NULL, 'HQ Anomaly Alerts', 'high', 60, '["in_app","email"]')
        """,
        (site_ids[0],),
    )
    hq_rule_id = int(cursor.lastrowid)

    _execute_params(
        cursor,
        """
        INSERT INTO alert_rules (site_id, meter_id, name, severity_threshold, cooldown_minutes, channels_json)
        VALUES (?, NULL, 'Warehouse Alerts', 'medium', 120, '["in_app"]')
        """,
        (site_ids[1],),
    )
    wh_rule_id = int(cursor.lastrowid)

    _execute_params(
        cursor,
        """
        INSERT INTO alert_rules (site_id, meter_id, name, severity_threshold, cooldown_minutes, channels_json)
        VALUES (?, ?, 'HQ Main Meter - Critical Only', 'critical', 180, '["in_app","email"]')
        """,
        (site_ids[0], hq_main_id),
    )

    # Alert deliveries: represent that some events were delivered
    _execute_params(
        cursor,
        """
        INSERT OR IGNORE INTO alert_deliveries
          (anomaly_event_id, alert_rule_id, channel, delivery_status, attempt_count, last_attempt_at, delivered_at, dedupe_key)
        VALUES (?, ?, 'in_app', 'sent', 1, ?, ?, ?)
        """,
        (
            spike_event_id,
            hq_rule_id,
            _to_iso_z(spike_ts + timedelta(minutes=2)),
            _to_iso_z(spike_ts + timedelta(minutes=2)),
            f"anom:{spike_event_id}:in_app",
        ),
    )
    _execute_params(
        cursor,
        """
        INSERT OR IGNORE INTO alert_deliveries
          (anomaly_event_id, alert_rule_id, channel, delivery_status, attempt_count, last_attempt_at, delivered_at, dedupe_key)
        VALUES (?, ?, 'email', 'sent', 1, ?, ?, ?)
        """,
        (
            spike_event_id,
            hq_rule_id,
            _to_iso_z(spike_ts + timedelta(minutes=4)),
            _to_iso_z(spike_ts + timedelta(minutes=4)),
            f"anom:{spike_event_id}:email",
        ),
    )

    # Delivery failure example for warehouse drop
    _execute_params(
        cursor,
        """
        INSERT OR IGNORE INTO alert_deliveries
          (anomaly_event_id, alert_rule_id, channel, delivery_status, attempt_count, last_attempt_at, error_message, dedupe_key)
        VALUES (?, ?, 'in_app', 'failed', 2, ?, 'Downstream notification service timeout', ?)
        """,
        (
            drop_event_id,
            wh_rule_id,
            _to_iso_z(drop_ts + timedelta(minutes=10)),
            f"anom:{drop_event_id}:in_app",
        ),
    )

    # Suppressed example for acknowledged flatline (cooldown / suppression)
    _execute_params(
        cursor,
        """
        INSERT OR IGNORE INTO alert_deliveries
          (anomaly_event_id, alert_rule_id, channel, delivery_status, attempt_count, last_attempt_at, dedupe_key)
        VALUES (?, ?, 'in_app', 'suppressed', 0, ?, ?)
        """,
        (
            flat_event_id,
            hq_rule_id,
            _to_iso_z(flatline_start + timedelta(minutes=1)),
            f"anom:{flat_event_id}:in_app",
        ),
    )

    print("Seed: energy-domain demo data inserted.")


def _write_connection_guidance() -> None:
    """Write/refresh db_connection.txt and db_visualizer/sqlite.env (compatibility)."""
    current_dir = os.getcwd()
    connection_string = f"sqlite:///{current_dir}/{DB_NAME}"

    try:
        with open("db_connection.txt", "w", encoding="utf-8") as f:
            f.write("# SQLite connection methods:\n")
            f.write(f"# Python: sqlite3.connect('{DB_NAME}')\n")
            f.write(f"# Connection string: {connection_string}\n")
            f.write(f"# File path: {current_dir}/{DB_NAME}\n")
        print("Connection information saved to db_connection.txt")
    except Exception as e:
        print(f"Warning: Could not save connection info: {e}")

    # Create environment variables file for Node.js viewer
    db_path = os.path.abspath(DB_NAME)
    os.makedirs("db_visualizer", exist_ok=True)

    try:
        with open("db_visualizer/sqlite.env", "w", encoding="utf-8") as f:
            f.write(f'export SQLITE_DB="{db_path}"\n')
        print("Environment variables saved to db_visualizer/sqlite.env")
    except Exception as e:
        print(f"Warning: Could not save environment variables: {e}")


def main() -> None:
    """Create/upgrade schema and seed realistic demo data."""
    print("Starting SQLite setup...")

    db_exists = os.path.exists(DB_NAME)
    if db_exists:
        print(f"SQLite database already exists at {DB_NAME}")
        try:
            conn = sqlite3.connect(DB_NAME)
            conn.execute("SELECT 1")
            conn.close()
            print("Database is accessible and working.")
        except Exception as e:
            print(f"Warning: Database exists but may be corrupted: {e}")
    else:
        print("Creating new SQLite database...")

    conn = sqlite3.connect(DB_NAME)
    try:
        cursor = conn.cursor()

        # Always enable foreign key enforcement for this connection.
        cursor.execute("PRAGMA foreign_keys = ON")

        # Schema + seed
        _create_schema(cursor)
        _seed_app_info(cursor)
        _seed_users(cursor)
        _seed_energy_domain(cursor)

        conn.commit()

        # Stats
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        table_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM meter_readings")
        readings_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM anomaly_events")
        anomaly_count = cursor.fetchone()[0]

    finally:
        conn.close()

    _write_connection_guidance()

    print("\nSQLite setup complete!")
    print(f"Database: {DB_NAME}")
    print(f"Location: {os.getcwd()}/{DB_NAME}")
    print("")
    print("To use with Node.js viewer, run: source db_visualizer/sqlite.env")
    print("")
    print("Database statistics:")
    print(f"  Tables: {table_count}")
    print(f"  Meter readings: {readings_count}")
    print(f"  Anomaly events: {anomaly_count}")

    # If sqlite3 CLI is available, show how to use it
    try:
        import subprocess

        result = subprocess.run(["which", "sqlite3"], capture_output=True, text=True)
        if result.returncode == 0:
            print("")
            print("SQLite CLI is available. You can also use:")
            print(f"  sqlite3 {DB_NAME}")
    except Exception:
        pass

    print("\nScript completed successfully.")


if __name__ == "__main__":
    main()
