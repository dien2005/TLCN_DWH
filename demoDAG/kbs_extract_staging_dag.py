"""
DAG: kbs_extract_staging
Mục đích: Extract dữ liệu giá (OHLCV) từ KBS qua vnstock, load vào staging.kbs_price_raw.
Tương ứng Khối 1 (Extract) + Khối 2 (Staging) trong kiến trúc pipeline.
"""
from __future__ import annotations

import time
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

# Tuần 3: danh sách mã tiêu biểu để test DAG + staging. 
# Full list sẽ dùng ở Tuần 5 (backfill lịch sử).
SYMBOLS = [
    "VNM", "FPT", "HPG", "VCB", "VIC", "MSN", "MWG", "GAS",
    "VHM", "TCB", "CTG", "BID", "SSI", "VRE", "PLX",
    "POW", "STB", "HDB", "MBB", "VJC",
]

# Throttle giữa các request để tránh rate-limit của KBS Guest tier
SLEEP_BETWEEN_REQUESTS_SEC = 2

logger = logging.getLogger(__name__)


def _get_dwh_connection_string() -> str:
    """Lấy connection string Postgres DWH từ Airflow Connection 'dwh_postgres'."""
    conn = BaseHook.get_connection("dwh_postgres")
    return (
        f"postgresql://{conn.login}:{conn.password}"
        f"@{conn.host}:{conn.port}/{conn.schema}"
    )


def extract_and_load_kbs_price(**context):
    """
    Với mỗi mã trong SYMBOLS: gọi vnstock lấy dữ liệu giá từ KBS,
    upsert vào staging.kbs_price_raw (tránh trùng nhờ UNIQUE constraint).
    """
    import psycopg2
    from vnstock import Vnstock

    conn_str = _get_dwh_connection_string()
    pg_conn = psycopg2.connect(conn_str)
    pg_cursor = pg_conn.cursor()

    total_rows = 0

    for symbol in SYMBOLS:
        try:
            logger.info("Đang lấy dữ liệu giá cho mã %s từ KBS...", symbol)

            stock = Vnstock().stock(symbol=symbol, source="KBS")
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            df = stock.quote.history(start=start_date, end=end_date)

            if df is None or df.empty:
                logger.warning("Không có dữ liệu trả về cho mã %s, bỏ qua.", symbol)
                continue

            for _, row in df.iterrows():
                pg_cursor.execute(
                    """
                    INSERT INTO staging.kbs_price_raw
                        (symbol, trade_date, open_price, high_price, low_price, close_price, volume, source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'KBS')
                    ON CONFLICT (symbol, trade_date, source)
                    DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume,
                        _loaded_at = now();
                    """,
                    (
                        symbol,
                        row["time"],
                        row["open"],
                        row["high"],
                        row["low"],
                        row["close"],
                        int(row["volume"]),
                    ),
                )
                total_rows += 1

            pg_conn.commit()
            logger.info("Đã upsert %d dòng cho mã %s.", len(df), symbol)

        except Exception as exc:
            logger.error("Lỗi khi xử lý mã %s: %s", symbol, exc)
            pg_conn.rollback()

        finally:
            time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

    pg_cursor.close()
    pg_conn.close()
    logger.info("Hoàn tất DAG - tổng %d dòng đã upsert vào staging.kbs_price_raw.", total_rows)


default_args = {
    "owner": "KIMDIEN",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="kbs_extract_staging",
    description="Extract giá KBS qua vnstock, load vào staging layer",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["kbs", "extract", "staging"],
) as dag:

    extract_load_task = PythonOperator(
        task_id="extract_and_load_kbs_price",
        python_callable=extract_and_load_kbs_price,
    )