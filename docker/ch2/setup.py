"""Create the document collections used by the CH2 loader in a fresh namespace."""
import json
import os
import re
import sys
from contextlib import contextmanager

import constants
from drivers.polyphenydriver import PolyphenyDriver
from util.scaleparameters import makeWithScaleFactor


@contextmanager
def setup_connection(config):
    driver = PolyphenyDriver(None, 0)
    driver.loadConfig(config)
    try:
        yield driver
        driver.connection.commit()
    except Exception:
        driver.connection.rollback()
        raise
    finally:
        driver.connection.close()


def validate_load(warehouses, scale_factor, empty=False):
    scale = makeWithScaleFactor(warehouses, 1, scale_factor)
    districts = warehouses * scale.districtsPerWarehouse
    expected = {
        "item": scale.items,
        "warehouse": warehouses,
        "district": districts,
        "customer": districts * scale.customersPerDistrict,
        "orders": districts * scale.customersPerDistrict,
        "neworder": districts * scale.newOrdersPerDistrict,
        "history": districts * scale.customersPerDistrict,
        "stock": warehouses * scale.items,
        "supplier": constants.NUM_SUPPLIERS,
        "nation": constants.NUM_NATIONS,
        "region": constants.NUM_REGIONS,
        # CH2 embeds order lines in orders rather than loading this collection.
        constants.TABLENAME_ORDERLINE: 0,
    }
    config = {key: default for key, (_, default) in PolyphenyDriver.DEFAULT_CONFIG.items()}
    counts = {}
    with setup_connection(config) as driver:
        for table, count in expected.items():
            collection = constants.COLLECTIONS_DICT[table]
            driver.executeMql(f'db.{collection}.aggregate([{{"$count":"count"}}])')
            row = next(iter(driver.cursor), {})
            actual = int(row.get("count", 0))
            wanted = 0 if empty else count
            if actual != wanted:
                raise RuntimeError(f"Incomplete CH2 load: {collection}: expected {wanted}, got {actual}")
            counts[collection] = actual
    return counts


def main():
    config = {key: default for key, (_, default) in PolyphenyDriver.DEFAULT_CONFIG.items()}
    namespace = config["namespace"]
    if not re.fullmatch(r"[a-z][a-z0-9_]*", namespace):
        raise ValueError(f"Invalid document namespace: {namespace}")
    store = os.environ.get("CH2_STORE")
    placement = f".store({json.dumps(store)})" if store else ""
    with setup_connection(config) as driver:
        if sys.argv[1:] == ["--ready"]:
            driver.cursor.executeany("sql", "SELECT 1", namespace="public")
            return
        if os.environ.get("CH2_CREATE_STORE", "false").lower() == "true":
            if not store or not re.fullmatch(r"[a-z][a-z0-9_]*", store):
                raise ValueError(f"Invalid adapter name: {store}")
            settings = json.dumps({
                "mode": "remote",
                "host": os.environ.get("CH2_MONGO_HOST", "mongo"),
                "port": os.environ.get("CH2_MONGO_PORT", "27017"),
                "trxLifetimeLimit": "1209600",
            }).replace("'", "''")
            driver.cursor.executeany(
                "sql",
                f"ALTER ADAPTERS ADD \"{store}\" USING 'MongoDB' AS STORE WITH '{settings}'",
                namespace="public",
            )
            driver.connection.commit()
        driver.cursor.executeany("sql", f'CREATE DOCUMENT NAMESPACE "{namespace}"', namespace="public")
        for table in constants.CH2_TABLE_COLUMNS:
            collection = constants.COLLECTIONS_DICT[table]
            driver.executeMql(f"db.createCollection({json.dumps(collection)}){placement}")


if __name__ == "__main__":
    main()
