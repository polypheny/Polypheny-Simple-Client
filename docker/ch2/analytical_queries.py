"""SQL projections over the live CH2 document collections."""
import re
from pathlib import Path

import constants

# Only fields used by the analytical workload are projected.
FIELDS = {
    "item": ("i_id", "i_name i_data", "i_price"),
    "stock": ("s_w_id s_i_id s_quantity s_order_cnt", "", ""),
    "supplier": ("su_suppkey su_nationkey", "su_name su_address su_phone su_comment", ""),
    "nation": ("n_nationkey n_regionkey", "n_name", ""),
    "region": ("r_regionkey", "r_name", ""),
    "customer": ("c_id c_w_id c_d_id", "c_last c_city c_phone c_state", "c_balance"),
    "orders": ("o_id o_w_id o_d_id o_c_id o_carrier_id o_ol_cnt", "o_entry_d", ""),
    "neworder": ("no_o_id no_d_id no_w_id", "", ""),
}


def columns(fields, path="'lax $."):
    result = []
    for names, sql_type in zip(fields, ("INTEGER", "VARCHAR(1024)", "DECIMAL(18,2)")):
        for name in names.split():
            document = "CAST(src.d AS VARCHAR(65536))"
            query_path = path + name
            value = f"JSON_VALUE({document}, {query_path}')"
            if sql_type.startswith("VARCHAR") and name not in ("o_entry_d", "ol_delivery_d"):
                # JSON_VALUE otherwise interprets numeric strings as numbers (losing leading zeroes).
                string_path = query_path.removeprefix("'lax ")
                value = f'''SUBSTRING(JSON_VALUE({document}, 'lax $.concat("x", {string_path})'), 2)'''
            result.append(f"CAST({value} AS {sql_type}) AS {name}")
            if name in ("o_entry_d", "ol_delivery_d"):
                result.append(f"{day_number(value)} AS {name}_day")
    return result


def day_number(value):
    # Avoid the server's broken VARCHAR-to-TIMESTAMP conversion and date arithmetic.
    # Missing delivery dates sort before CH2's dates and are excluded by the queries.
    value = f"COALESCE({value}, '0001-01-01 00:00:00')"
    year = f"(CAST(SUBSTRING({value}, 1, 4) AS INTEGER) - 1)"
    return (f"365 * {year} + FLOOR({year} / 4.0) - FLOOR({year} / 100.0) "
            f"+ FLOOR({year} / 400.0) + EXTRACT(DOY FROM CAST(SUBSTRING({value}, 1, 10) AS DATE))")


def load_queries(namespace):
    if not re.fullmatch(r"[a-z][a-z0-9_]*", namespace):
        raise ValueError(f"Invalid CH2 namespace: {namespace}")
    # Example: FROM {{customer}} c in ch2.sql becomes a subquery over this
    # run's customer collection. JSON_VALUE exposes fields such as c.c_id.
    projections = {}
    # The CH2 keys make these rows unique. DISTINCT also keeps the document
    # projections separate from joins in Polypheny's planner.
    for table, fields in FIELDS.items():
        projections[table] = f'SELECT DISTINCT {", ".join(columns(fields))} FROM "{namespace}"."{table}" src'
    line_path = "'lax $.o_orderline[' || CAST(pos.idx AS VARCHAR(2)) || ']."
    line_fields = ("ol_number ol_i_id ol_supply_w_id ol_quantity", "ol_delivery_d", "ol_amount")
    fields = columns(FIELDS["orders"]) + columns(line_fields, line_path)
    positions = ", ".join(f"({i})" for i in range(constants.MAX_OL_CNT))
    projections["orderlines"] = (
        f'SELECT DISTINCT {", ".join(fields)} FROM "{namespace}"."orders" src '
        f'CROSS JOIN (VALUES {positions}) AS pos(idx) '
        f"WHERE JSON_VALUE(CAST(src.d AS VARCHAR(65536)), {line_path}ol_number') IS NOT NULL"
    )
    text = Path(__file__).with_name("queries").joinpath("ch2.sql").read_text()
    parts = re.split(r"(?m)^-- (Q\d{2})\s*$", text)
    queries = {}
    for name, body in zip(parts[1::2], parts[2::2]):
        body = body.strip().rstrip(";")
        for table, projection in projections.items():
            body = body.replace("{{" + table + "}}", "(" + projection + ")")
        queries[name] = body
    return queries
