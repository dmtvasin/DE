"""Загрузка csv файла в DuckDB из фиксированного локального пути."""

import os
import time

import duckdb

CSV_PATH = r"C:\Users\dmitrii.vasin\source\repos\Learn\Тестовые\drivee\drivee_dbt_proj\data\dataset_test.csv"
DUCKDB_PATH = r"C:\Users\dmitrii.vasin\source\repos\Learn\Тестовые\drivee\drivee_dbt_proj\data\warehouse.duckdb"


def load_csv_to_duckdb():
    """Загрузить CSV в DuckDB как raw.rides таблицу."""
    print(f"Загрузка CSV в DuckDB: {DUCKDB_PATH}")
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV файл не найден: {CSV_PATH}")
    start = time.time()

    con = duckdb.connect(DUCKDB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE raw.rides AS
        SELECT * FROM read_csv_auto('{CSV_PATH}', HEADER=TRUE)
    """
    )

    count = con.execute("SELECT COUNT(*) FROM raw.rides").fetchone()[0]
    elapsed = time.time() - start
    print(f"Загружено {count:,} записей в raw.rides ({elapsed:.1f} сек)")

    con.close()


def main():
    load_csv_to_duckdb()
    print("Готово. База обновлена.")


if __name__ == "__main__":
    main()
