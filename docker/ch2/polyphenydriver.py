""" inspired structure by: https://github.com/couchbaselabs/ch2/blob/main/ch2driver/pytpcc/drivers/mongodbdriver.py """

import polypheny
import constants
import json
import os
import logging
import time
from decimal import Decimal
from pathlib import Path
from measurements import Measurements
from analytical_queries import load_queries as load_builtin_queries

from .abstractdriver import AbstractDriver

def mql_json(value):
    def encode_decimal(number):
        if isinstance(number, Decimal):
            if number == number.to_integral_value():
                return int(number)
            return {"$numberDecimal": str(number)}
        raise TypeError(f"Unsupported MQL value {type(number).__name__}")

    return json.dumps(value, default=encode_decimal)


def load_queries(namespace):
    filename = os.environ.get("CH2_QUERY_FILE")
    queries = json.loads(Path(filename).read_text()) if filename else load_builtin_queries(namespace)
    selected = os.environ.get("CH2_QUERIES", "").strip()
    valid_names = [f"Q{i:02d}" for i in range(1, 23)]
    names = [name.strip() for name in selected.split(",")] if selected else valid_names
    if len(names) != len(set(names)):
        raise ValueError("Duplicate analytical query names")
    for name in names:
        if name not in valid_names or not isinstance(queries.get(name), str) or not queries[name].strip():
            raise ValueError(f"Missing or invalid analytical query: {name}")
    return {name: queries[name] for name in names}


class PolyphenyDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "host": ("Polypheny hostname", os.environ.get("CH2_HOST", "polypheny")),
        "port": ("Prism port", os.environ.get("CH2_PORT", 20590)),
        "namespace": ("Document namespace", os.environ.get("CH2_NAMESPACE", "ch2")),
        "user": ("Username", os.environ.get("CH2_USER", "pa")),
        "password": ("Password", os.environ.get("CH2_PASSWORD", "")),
    }


    def __init__(self, ddl, clientId, TAFlag="T",
                     schema=constants.CH2_DRIVER_SCHEMA["CH2"],
                     preparedTransactionQueries=None,
                     analyticalQueries=constants.CH2_DRIVER_ANALYTICAL_QUERIES["HAND_OPTIMIZED_QUERIES"],
                     customerExtraFields=constants.CH2_CUSTOMER_EXTRA_FIELDS["NOT_SET"],
                     ordersExtraFields=constants.CH2_ORDERS_EXTRA_FIELDS["NOT_SET"],
                     itemExtraFields=constants.CH2_ITEM_EXTRA_FIELDS["NOT_SET"],
                     **kwargs):
        super().__init__("polypheny", ddl)
        self.client_id = clientId
        self.TAFlag = TAFlag
        self.schema = schema
        self.analyticalQueries = analyticalQueries
        self.preparedTransactionQueries = preparedTransactionQueries or {}

        self.customerExtraFields = customerExtraFields
        self.ordersExtraFields = ordersExtraFields
        self.itemExtraFields = itemExtraFields

        self.connection = None
        self.cursor = None
        self.tx_status = ""
        self.measurements = None
        self.batch_size = int(os.environ.get("CH2_BATCH_SIZE", "100"))
        self.queries = None
        self.loaded_documents = 0
        self.next_load_report = 0

    def makeDefaultConfig(self):
        return self.DEFAULT_CONFIG

    def loadConfig(self, config):
        self.namespace = config["namespace"]

        self.connection = polypheny.connect(
            address=(config["host"], int(config["port"])),
            username=config["user"],
            password=config["password"],
            transport="plain"
        )
        self.cursor = self.connection.cursor()
        if self.TAFlag == "A":
            self.queries = load_queries(self.namespace)
            default_language = "mongo" if os.environ.get("CH2_QUERY_FILE") else "sql"
            self.query_language = os.environ.get("CH2_QUERY_LANGUAGE", default_language)
            if not os.environ.get("CH2_QUERY_FILE") and self.query_language != "sql":
                raise ValueError("The built-in CH2 queries require SQL")

    def insertBatch(self, collection, documents):
        data = mql_json(documents)
        query = f"db.{collection}.insertMany({data})"

        try:
            self.cursor.executeany(
                "mongo",
                query,
                namespace=self.namespace
            )
            self.connection.commit()

        except Exception:
            self.connection.rollback()
            raise

    def loadTuples( self, tableName, tuples ):
        if len( tuples ) == 0:
            return

        logging.debug("Loading %d tuples for tableName %s" % (len(tuples), tableName))
        assert tableName in constants.CH2_TABLE_COLUMNS, "Unexpected table %s" % tableName


        collection = constants.COLLECTIONS_DICT[tableName]

        cur_batch = []

        for t in tuples:
            _, val = self.getOneDoc(tableName, t)
            cur_batch.append(val)

            if len(cur_batch) >= self.batch_size:
                self.insertLoadBatch(collection, cur_batch)
                cur_batch = []

        if cur_batch:
            self.insertLoadBatch(collection, cur_batch)

    def insertLoadBatch(self, collection, documents):
        self.insertBatch(collection, documents)
        self.loaded_documents += len(documents)
        now = time.monotonic()
        if now >= self.next_load_report:
            print(f"CH2 load worker {self.client_id}: {self.loaded_documents} documents committed "
                  f"(latest collection: {collection})", flush=True)
            self.next_load_report = now + 15

    def loadFinish(self):
        print(f"CH2 load worker {self.client_id}: finished loading {self.loaded_documents} documents", flush=True)

    def executeStart(self):
        self.measurements = Measurements(self.client_id, self.TAFlag)

    def executeFinish(self):
        self.measurements.save()
        self.connection.close()

    def executeTransaction(self, txn, params, duration, endBenchmarkTime, queryIterNum):
        if txn == constants.QueryTypes.CH2:
            return super().executeTransaction(txn, params, duration, endBenchmarkTime, queryIterNum)
        start = time.monotonic_ns()
        try:
            result, _ = super().executeTransaction(txn, params, duration, endBenchmarkTime, queryIterNum)
            aborted = txn == constants.TransactionTypes.NEW_ORDER and result is None
            if aborted:
                self.connection.rollback()
                self.tx_status = "aborted"
            else:
                self.connection.commit()
            self.measurements.record(txn, start, "aborted" if aborted else "ok")
            return result, self.txStatus()
        except Exception:
            self.measurements.record(txn, start, "error")
            self.measurements.save()
            self.connection.rollback()
            raise

    def txStatus(self):
        return self.tx_status

    def executeMql(self, query):
        self.cursor.executeany(
            "mongo",
            query,
            namespace=self.namespace
        )


    def findOne(self, collection, filter, projection=None, sort=None):
        if sort:
            return next(iter(self.findMany(collection, filter, projection, sort)), None)
        self.executeFind(collection, filter, projection, limit=1)
        return next(iter(self.cursor), None)

    def executeFind(self, collection, filter, projection=None, limit=None):
        args = mql_json(filter)
        if projection is not None:
            args += f", {mql_json(projection)}"
        query = f"db.{collection}.find({args})"
        if limit:
            query += f".limit({limit})"
        self.executeMql(query)

    def updateOne(self, collection, filter, update):
        if "$inc" in update:
            # Prism's MQL parser needs decimal literals for fractional increments.
            increments = {
                field: {"$numberDecimal": str(value)} if isinstance(value, (float, Decimal)) else value
                for field, value in update["$inc"].items()
            }
            update = {**update, "$inc": increments}
        query = (
            f"db.{collection}.updateOne("
            f"{mql_json(filter)}, {mql_json(update)})"
        )
        self.executeMql(query)


    def deleteOne(self, collection, filter):
        query = f"db.{collection}.deleteOne({mql_json(filter)})"
        self.executeMql(query)


    def findMany(self, collection, filter, projection=None, sort=None):
        self.executeFind(collection, filter, projection)
        rows = list(self.cursor)
        # Sort the matching documents locally. This server ignores $sort in aggregate.
        for field, direction in reversed((sort or {}).items()):
            def field_value(row):
                for part in field.split("."):
                    row = row[part]
                return row
            rows.sort(key=field_value, reverse=direction < 0)
        return rows


    def insertOne(self, collection, document):
        query = f"db.{collection}.insertOne({mql_json(document)})"
        self.executeMql(query)

    def doDelivery(self, params):
        self.tx_status = ""
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = str(params["ol_delivery_d"])

        neworders = constants.COLLECTIONS_DICT["neworder"]
        orders = constants.COLLECTIONS_DICT["orders"]
        customers = constants.COLLECTIONS_DICT["customer"]

        result = []
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1):
            ## getNewOrder
            no = self.findOne(neworders, {"no_d_id": d_id, "no_w_id": w_id}, sort={"no_o_id": 1})
            if no is None:
                continue
            assert len(no) > 0
            o_id = no["no_o_id"]

            ## getCId
            o = self.findOne(orders, {"o_id": o_id, "o_d_id": d_id, "o_w_id": w_id}, {"o_c_id": 1})
            assert o is not None
            c_id = o["o_c_id"]

            ## sumOLAmount
            orderLines = self.findOne(orders, {"o_id": o_id, "o_d_id": d_id, "o_w_id": w_id}, {"o_orderline": 1})
            assert orderLines is not None

            ol_total = sum(Decimal(str(ol["ol_amount"])) for ol in orderLines["o_orderline"])

            lines = orderLines["o_orderline"]
            for line in lines:
                line["ol_delivery_d"] = ol_delivery_d
            self.updateOne(
                orders, {"o_id": o_id, "o_d_id": d_id, "o_w_id": w_id},
                {"$set": {"o_carrier_id": o_carrier_id, "o_orderline": lines}}
            )

            ## updateCustomer
            self.updateOne(customers, {"c_id": c_id, "c_d_id": d_id, "c_w_id": w_id}, {"$inc": {"c_balance": ol_total}})

            ## deleteNewOrder
            self.deleteOne(neworders, {"_id": no["_id"]})

            assert ol_total is not None
            assert ol_total > 0.0

            result.append((d_id, o_id))

        return result


    def doNewOrder(self, params):
        self.tx_status = ""
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = str(params["o_entry_d"])
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]

        if self.schema == constants.CH2_DRIVER_SCHEMA["CH2"]:
            s_dist_col = "s_dist_%02d" % d_id
        else:
            s_dist_col = "s_dists"

        assert len(i_ids) > 0
        assert len(i_ids) == len(i_w_ids)
        assert len(i_ids) == len(i_qtys)

        all_local = (not i_w_ids or [w_id] * len(i_w_ids) == i_w_ids)

        item = constants.COLLECTIONS_DICT["item"]
        warehouse = constants.COLLECTIONS_DICT["warehouse"]
        district = constants.COLLECTIONS_DICT["district"]
        customer = constants.COLLECTIONS_DICT["customer"]
        neworder = constants.COLLECTIONS_DICT["neworder"]
        orders = constants.COLLECTIONS_DICT["orders"]
        stock = constants.COLLECTIONS_DICT["stock"]

        ## getItems
        items = self.findMany(
            item,
            {"i_id": {"$in": i_ids}},
            {"i_id": 1, "i_price": 1, "i_name": 1, "i_data": 1}
        )

        ## Invalid item causes an abort
        if set(i_ids) - {item["i_id"] for item in items}:
            return

        ## getWarehouseTaxRate
        w = self.findOne(warehouse, {"w_id": w_id}, {"w_tax": 1})
        assert w
        w_tax = w["w_tax"]

        ## getDistrict
        d = self.findOne(
            district,
            {"d_id": d_id, "d_w_id": w_id},
            {"d_tax": 1, "d_next_o_id": 1}
        )
        assert d
        d_tax = d["d_tax"]
        d_next_o_id = d["d_next_o_id"]

        ## incrementNextOrderId
        self.updateOne(
            district,
            {"d_id": d_id, "d_w_id": w_id},
            {"$inc": {"d_next_o_id": 1}}
        )

        ## getCustomer
        if self.schema == constants.CH2_DRIVER_SCHEMA["CH2"]:
            c = self.findOne(
                customer,
                {"c_id": c_id, "c_d_id": d_id, "c_w_id": w_id},
                {"c_discount": 1, "c_last": 1, "c_credit": 1}
            )
        else:
            c = self.findOne(
                customer,
                {"c_id": c_id, "c_d_id": d_id, "c_w_id": w_id},
                {"c_discount": 1, "c_name.c_last": 1, "c_credit": 1}
            )
        assert c
        c_discount = c["c_discount"]

        ## Insert Order Information
        ol_cnt = len(i_ids)
        o_carrier_id = constants.NULL_CARRIER_ID

        ## createNewOrder
        self.insertOne(
            neworder,
            {
                "no_o_id": d_next_o_id,
                "no_d_id": d_id,
                "no_w_id": w_id
            }
        )

        o = {
            "o_id": d_next_o_id,
            "o_entry_d": o_entry_d,
            "o_carrier_id": o_carrier_id,
            "o_ol_cnt": ol_cnt,
            "o_all_local": all_local,
            "o_d_id": d_id,
            "o_w_id": w_id,
            "o_c_id": c_id
        }

        o["o_orderline"] = []

        ## Insert Order Item Information
        item_data = []
        total = 0

        for i in range(ol_cnt):
            ol_number = i + 1
            ol_supply_w_id = i_w_ids[i]
            ol_i_id = i_ids[i]
            ol_quantity = i_qtys[i]

            itemInfo = next(item for item in items if item["i_id"] == ol_i_id)
            i_name = itemInfo["i_name"]
            i_data = itemInfo["i_data"]
            i_price = itemInfo["i_price"]

            ## getStockInfo
            si = self.findOne(stock, {"s_i_id": ol_i_id, "s_w_id": ol_supply_w_id})
            assert si

            s_quantity = si["s_quantity"]
            s_ytd = si["s_ytd"]
            s_order_cnt = si["s_order_cnt"]
            s_remote_cnt = si["s_remote_cnt"]
            s_data = si["s_data"]

            if self.schema == constants.CH2_DRIVER_SCHEMA["CH2"]:
                s_dist_xx = si[s_dist_col]
            else:
                s_dist_xx = si[s_dist_col][d_id - 1]

            ## Update stock
            s_ytd += ol_quantity

            if s_quantity >= ol_quantity + 10:
                s_quantity = s_quantity - ol_quantity
            else:
                s_quantity = s_quantity + 91 - ol_quantity

            s_order_cnt += 1

            if ol_supply_w_id != w_id:
                s_remote_cnt += 1

            self.updateOne(
                stock,
                {"s_i_id": ol_i_id, "s_w_id": ol_supply_w_id},
                {"$set": {
                    "s_quantity": s_quantity,
                    "s_ytd": s_ytd,
                    "s_order_cnt": s_order_cnt,
                    "s_remote_cnt": s_remote_cnt
                }}
            )

            if (constants.ORIGINAL_STRING in i_data
                    and constants.ORIGINAL_STRING in s_data):
                brand_generic = "B"
            else:
                brand_generic = "G"

            ol_amount = ol_quantity * i_price
            total += ol_amount

            ol = {
                "ol_number": ol_number,
                "ol_i_id": ol_i_id,
                "ol_supply_w_id": ol_supply_w_id,
                "ol_delivery_d": o_entry_d,
                "ol_quantity": ol_quantity,
                "ol_amount": ol_amount,
                "ol_dist_info": s_dist_xx
            }

            o["o_orderline"].append(ol)

            item_data.append(
                (i_name, s_quantity, brand_generic, i_price, ol_amount)
            )

        self.insertOne(orders, o)

        ## Adjust total for discount
        total *= (1 - c_discount) * (1 + w_tax + d_tax)

        misc = [(w_tax, d_tax, d_next_o_id, total)]

        return [c, misc, item_data]


    def doOrderStatus(self, params):
        self.tx_status = ""
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]

        assert w_id
        assert d_id

        customer = constants.COLLECTIONS_DICT["customer"]
        orders = constants.COLLECTIONS_DICT["orders"]

        search_fields = {"c_w_id": w_id, "c_d_id": d_id}

        if self.schema == constants.CH2_DRIVER_SCHEMA["CH2"]:
            return_fields = {"c_id": 1, "c_first": 1, "c_middle": 1, "c_last": 1, "c_balance": 1}
        else:
            return_fields = {"c_id": 1, "c_name.c_first": 1, "c_name.c_middle": 1, "c_name.c_last": 1, "c_balance": 1}

        if c_id is not None:
            ## getCustomerByCustomerId
            search_fields["c_id"] = c_id
            c = self.findOne(customer, search_fields, return_fields)
            assert c

        else:
            ## getCustomersByLastName
            if self.schema == constants.CH2_DRIVER_SCHEMA["CH2"]:
                search_fields["c_last"] = c_last
                sort_fields = {"c_first": 1}
            else:
                search_fields["c_name.c_last"] = c_last
                sort_fields = {"c_name.c_first": 1}

            all_customers = self.findMany(customer, search_fields, return_fields, sort_fields)
            namecnt = len(all_customers)
            assert namecnt > 0

            index = int((namecnt - 1) / 2)
            c = all_customers[index]
            c_id = c["c_id"]

        assert len(c) > 0
        assert c_id is not None

        ## getLastOrder
        order = self.findOne(
            orders,
            {"o_w_id": w_id, "o_d_id": d_id, "o_c_id": c_id},
            {"o_id": 1, "o_carrier_id": 1, "o_entry_d": 1},
            sort={"o_id": -1}
        )

        orderLines = []

        if order:
            ## getOrderLines
            orderLines = self.findMany(
                orders,
                {"o_w_id": w_id, "o_d_id": d_id, "o_id": order["o_id"]},
                {"o_orderline": 1}
            )

        return [c, order, orderLines]


    def doPayment(self, params):
        self.tx_status = ""
        w_id = params["w_id"]
        d_id = params["d_id"]
        h_amount = params["h_amount"]
        c_w_id = params["c_w_id"]
        c_d_id = params["c_d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        h_date = str(params["h_date"])

        customer = constants.COLLECTIONS_DICT["customer"]
        warehouse = constants.COLLECTIONS_DICT["warehouse"]
        district = constants.COLLECTIONS_DICT["district"]
        history = constants.COLLECTIONS_DICT["history"]

        search_fields = {"c_w_id": c_w_id, "c_d_id": c_d_id}

        if c_id is not None:
            ## getCustomerByCustomerId
            search_fields["c_id"] = c_id
            c = self.findOne(customer, search_fields)
            assert c
        else:
            ## getCustomersByLastName
            if self.schema == constants.CH2_DRIVER_SCHEMA["CH2"]:
                search_fields["c_last"] = c_last
                sort_fields = {"c_first": 1}
            else:
                search_fields["c_name.c_last"] = c_last
                sort_fields = {"c_name.c_first": 1}

            all_customers = self.findMany(customer, search_fields, sort=sort_fields)
            namecnt = len(all_customers)
            assert namecnt > 0
            index = int((namecnt - 1) / 2)
            c = all_customers[index]
            c_id = c["c_id"]

        assert len(c) > 0
        assert c_id is not None
        c_data = c["c_data"]

        ## getWarehouse
        if self.schema == constants.CH2_DRIVER_SCHEMA["CH2"]:
            w = self.findOne(warehouse, {"w_id": w_id},
                {"w_name": 1, "w_street_1": 1, "w_street_2": 1,
                 "w_city": 1, "w_state": 1, "w_zip": 1})
        else:
            w = self.findOne(warehouse, {"w_id": w_id},
                {"w_name": 1, "w_address.w_street_1": 1,
                 "w_address.w_street_2": 1, "w_address.w_city": 1,
                 "w_address.w_state": 1, "w_address.w_zip": 1})

        assert w

        ## updateWarehouseBalance
        self.updateOne(warehouse, {"w_id": w_id}, {"$inc": {"w_ytd": h_amount}})

        ## getDistrict
        if self.schema == constants.CH2_DRIVER_SCHEMA["CH2"]:
            d = self.findOne(district, {"d_w_id": w_id, "d_id": d_id},
                {"d_name": 1, "d_street_1": 1, "d_street_2": 1,
                 "d_city": 1, "d_state": 1, "d_zip": 1})
        else:
            d = self.findOne(district, {"d_w_id": w_id, "d_id": d_id},
                {"d_name": 1, "d_address.d_street_1": 1,
                 "d_address.d_street_2": 1, "d_address.d_city": 1,
                 "d_address.d_state": 1, "d_address.d_zip": 1})

        assert d

        ## updateDistrictBalance
        self.updateOne(district, {"d_w_id": w_id, "d_id": d_id},
                       {"$inc": {"d_ytd": h_amount}})

        ## Build CUSTOMER update command
        customer_update = {
            "$inc": {
                "c_balance": h_amount * -1,
                "c_ytd_payment": h_amount,
                "c_payment_cnt": 1
            }
        }

        ## Customer Credit Information
        if c["c_credit"] == constants.BAD_CREDIT:
            newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
            c_data = newData + "|" + c_data

            if len(c_data) > constants.MAX_C_DATA:
                c_data = c_data[:constants.MAX_C_DATA]

            customer_update["$set"] = {"c_data": c_data}

        ## Build History
        h_data = "%s    %s" % (w["w_name"], d["d_name"])
        h = {
            "h_d_id": d_id,
            "h_w_id": w_id,
            "h_date": h_date,
            "h_amount": h_amount,
            "h_data": h_data
        }

        ## updateCustomer
        self.updateOne(
            customer,
            {"c_id": c_id, "c_d_id": c_d_id, "c_w_id": c_w_id},
            customer_update
        )

        ## insertHistory
        self.insertOne(history, h)

        return [w, d, c]


    def doStockLevel(self, params):
        self.tx_status = ""
        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]

        district = constants.COLLECTIONS_DICT["district"]
        orders = constants.COLLECTIONS_DICT["orders"]
        stock = constants.COLLECTIONS_DICT["stock"]

        ## getOId
        d = self.findOne(district, {"d_w_id": w_id, "d_id": d_id}, {"d_next_o_id": 1})
        assert d
        o_id = d["d_next_o_id"]

        ## getStockCount
        orderLines = self.findMany(
            orders,
            {"o_w_id": w_id, "o_d_id": d_id, "o_id": {"$lt": o_id, "$gte": o_id - 20}},
            {"o_orderline": 1}
        )

        ol_ids = set(ol["ol_i_id"] for o in orderLines for ol in o["o_orderline"])

        stocks = self.findMany(
            stock,
            {"s_w_id": w_id, "s_i_id": {"$in": list(ol_ids)}, "s_quantity": {"$lt": threshold}},
            {"s_i_id": 1}
        )

        return len(stocks)


    def runCH2Queries(self, duration, endBenchmarkTime, queryIterNum):
        self.tx_status = ""
        timings = {}
        order = constants.CH2_QUERIES_PERM[self.client_id % len(constants.CH2_QUERIES_PERM)]
        for name in order:
            if time.time() >= endBenchmarkTime:
                break
            if name not in self.queries:
                continue
            start = time.monotonic_ns()
            start_time = time.strftime("%H:%M:%S")
            try:
                self.cursor.executeany(self.query_language, self.queries[name],
                                       namespace=os.environ.get("CH2_QUERY_NAMESPACE", self.namespace))
                for _ in self.cursor:
                    pass
                self.connection.commit()
                self.measurements.record(name, start)
            except Exception:
                self.measurements.record(name, start, "error")
                self.measurements.save()
                self.connection.rollback()
                raise
            elapsed_ms = (time.monotonic_ns() - start) / 1_000_000
            # Include the final completed query even if it crosses the time limit.
            timings[name] = [self.client_id + 1, queryIterNum + 1, start_time,
                             f"{elapsed_ms}ms", time.strftime("%H:%M:%S")]
        return timings
