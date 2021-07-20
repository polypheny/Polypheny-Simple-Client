CREATE VIEW "user_materialized" AS SELECT * FROM "user"
CREATE VIEW "bid_materialized" AS SELECT * FROM "bid"
CREATE VIEW "picture_materialized" AS SELECT * FROM "picture"
CREATE VIEW "auction_materialized" AS SELECT * FROM "auction"
CREATE VIEW "category_materialized" AS SELECT * FROM "category"
CREATE VIEW "countAuction_materialized" AS SELECT count(*) as "NUMBER" FROM "auction"
CREATE VIEW "countBid_materialized" AS SELECT count(*) as "NUMBER" FROM "bid"
CREATE VIEW "nextEndingAuctions_materialized" AS SELECT a.id, a.title, a.end_date FROM "auction" a