#!/usr/bin/env python3
"""
run_sql.py - Run a SQL statement against a Databricks SQL Warehouse from the command line.

Prereqs:
  pip install databricks-sql-connector

Auth (recommended via env vars):
  export DATABRICKS_SERVER_HOSTNAME="company-lakehouse-us-dev.cloud.databricks.com"
  export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<warehouse-id>"
  export DATABRICKS_TOKEN="dapi..."

Examples:
  ./run_sql.py -q "SELECT 1"
  ./run_sql.py -q "SHOW CATALOGS" --format table
  ./run_sql.py -q "SELECT * FROM admin.usdev_users LIMIT 10" --format csv --out /tmp/users.csv
  ./run_sql.py --file query.sql --format json
  ./run_sql.py -q "SELECT * FROM mytable WHERE id = :id" --param id=123
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from typing import Any, Dict, List, Tuple, Optional

from databricks import sql


def _parse_params(param_list: Optional[List[str]]) -> Dict[str, Any]:
    """
    Parse --param key=value into a dict. Values are kept as strings by default,
    with best-effort conversion to int/float/bool/null.
    """
    params: Dict[str, Any] = {}
    if not param_list:
        return params

    def coerce(v: str) -> Any:
        vl = v.strip()
        if vl.lower() in ("null", "none"):
            return None
        if vl.lower() in ("true", "false"):
            return vl.lower() == "true"
        # int
        try:
            if vl.startswith("0") and len(vl) > 1 and vl.isdigit():
                # keep leading-zero strings as strings (e.g., zip codes)
                return vl
            return int(vl)
        except ValueError:
            pass
        # float
        try:
            return float(vl)
        except ValueError:
            return vl

    for item in param_list:
        if "=" not in item:
            raise ValueError(f"Invalid --param '{item}'. Expected key=value.")
        k, v = item.split("=", 1)
        k = k.strip()
        if not k:
            raise ValueError(f"Invalid --param '{item}'. Key is empty.")
        params[k] = coerce(v)
    return params


def _read_query(args: argparse.Namespace) -> str:
    if args.query:
        return args.query.strip()
    if args.file:
        with open(args.file, "r", encoding="utf-8") as f:
            return f.read().strip()
    raise ValueError("Provide either --query or --file.")


def _get_required_env(name: str, fallback: Optional[str] = None) -> str:
    v = os.environ.get(name, fallback)
    if not v:
        raise ValueError(f"Missing required setting: {name} (set env var or pass CLI arg if supported).")
    return v


def _connect(hostname: str, http_path: str, token: str, ssl_ca: Optional[str]) -> sql.Connection:
    kwargs: Dict[str, Any] = dict(
        server_hostname=hostname,
        http_path=http_path,
        access_token=token,
    )
    # Connector supports specifying an SSL CA bundle via 'tls_trusted_ca_file'
    # in newer versions. If not supported in your installed version, it will error;
    # in that case, fix CA certs at OS level or upgrade the connector.
    if ssl_ca:
        kwargs["tls_trusted_ca_file"] = ssl_ca
    return sql.connect(**kwargs)


def _write_csv(out_path: Optional[str], columns: List[str], rows: List[Tuple[Any, ...]]) -> None:
    out_f = open(out_path, "w", newline="", encoding="utf-8") if out_path else sys.stdout
    try:
        w = csv.writer(out_f)
        if columns:
            w.writerow(columns)
        for r in rows:
            w.writerow(list(r))
    finally:
        if out_path:
            out_f.close()


def _write_json(out_path: Optional[str], columns: List[str], rows: List[Tuple[Any, ...]]) -> None:
    data = [dict(zip(columns, r)) for r in rows] if columns else [list(r) for r in rows]
    payload = json.dumps(data, ensure_ascii=False, default=str, indent=2)
    if out_path:
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
    else:
        print(payload)


def _write_table(columns: List[str], rows: List[Tuple[Any, ...]]) -> None:
    # Minimal pretty table without extra deps
    str_rows = [[("" if v is None else str(v)) for v in r] for r in rows]
    widths = [len(c) for c in columns] if columns else []
    for r in str_rows:
        if not widths:
            widths = [0] * len(r)
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(cell))

    def line(sep: str = "-") -> str:
        return "+" + "+".join(sep * (w + 2) for w in widths) + "+"

    def fmt_row(r: List[str]) -> str:
        return "|" + "|".join(f" {r[i].ljust(widths[i])} " for i in range(len(widths))) + "|"

    if columns:
        print(line("-"))
        print(fmt_row(columns))
        print(line("="))
    for r in str_rows:
        print(fmt_row(r))
    if widths:
        print(line("-"))


def main() -> int:
    p = argparse.ArgumentParser(description="Run SQL against a Databricks SQL Warehouse.")
    p.add_argument("-q", "--query", help="SQL query to execute (wrap in quotes).")
    p.add_argument("-f", "--file", help="Path to a .sql file to execute.")
    p.add_argument("--param", action="append", help="Named parameter key=value (repeatable). Uses :key in SQL.")
    p.add_argument("--format", choices=["table", "csv", "json"], default="table", help="Output format.")
    p.add_argument("--out", help="Output file path (csv/json only). Default: stdout.")
    p.add_argument("--max-rows", type=int, default=5000, help="Max rows to fetch (default 5000).")
    p.add_argument("--hostname", help="Databricks workspace hostname (overrides env DATABRICKS_SERVER_HOSTNAME).")
    p.add_argument("--http-path", help="SQL Warehouse http_path (overrides env DATABRICKS_HTTP_PATH).")
    p.add_argument("--token", help="PAT token (overrides env DATABRICKS_TOKEN).")
    p.add_argument("--ssl-ca", help="Path to trusted CA bundle (optional).")
    p.add_argument("--quiet", action="store_true", help="Suppress non-result messages.")

    args = p.parse_args()

    try:
        query = _read_query(args)
        params = _parse_params(args.param)

        hostname = args.hostname or _get_required_env("DATABRICKS_SERVER_HOSTNAME")
        http_path = args.http_path or _get_required_env("DATABRICKS_HTTP_PATH")
        token = args.token or _get_required_env("DATABRICKS_TOKEN")

        with _connect(hostname, http_path, token, args.ssl_ca) as conn:
            with conn.cursor() as cur:
                if not args.quiet:
                    print(f"-- Connected to {hostname} ({http_path})", file=sys.stderr)

                # Execute with/without parameters
                if params:
                    cur.execute(query, parameters=params)
                else:
                    cur.execute(query)

                # If query returns results, fetch and print
                if cur.description is not None:
                    columns = [d[0] for d in cur.description]
                    rows = cur.fetchmany(args.max_rows)
                    # Note: fetchmany returns up to max_rows; for very large outputs, extend as needed.
                    if args.format == "csv":
                        _write_csv(args.out, columns, rows)
                    elif args.format == "json":
                        _write_json(args.out, columns, rows)
                    else:
                        _write_table(columns, rows)
                else:
                    if not args.quiet:
                        print("-- Statement executed (no result set).", file=sys.stderr)

        return 0

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
