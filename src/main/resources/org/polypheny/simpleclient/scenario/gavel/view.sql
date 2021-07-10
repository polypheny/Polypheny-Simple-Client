CREATE VIEW "user_view" AS SELECT * FROM "user"
CREATE VIEW "bid_view" AS SELECT * FROM "bid"
CREATE VIEW "picture_view" AS SELECT * FROM "picture"
CREATE VIEW "auction_view" AS SELECT * FROM "auction"
CREATE VIEW "category_view" AS SELECT * FROM "category"
CREATE VIEW "countAuction" AS SELECT count(*) as "NUMBER" FROM "auction"
CREATE VIEW "countBid" AS SELECT count(*) as "NUMBER" FROM "bid"
CREATE VIEW "nextEndingAuctions" AS SELECT a.id, a.title, a.end_date FROM "auction" a