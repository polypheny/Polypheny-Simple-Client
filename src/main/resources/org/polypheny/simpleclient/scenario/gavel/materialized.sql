CREATE MATERIALIZED VIEW "user_materialized" AS SELECT * FROM "user" FRESHNESS MANUAL
CREATE MATERIALIZED VIEW "bid_materialized" AS SELECT * FROM "bid" FRESHNESS MANUAL
CREATE MATERIALIZED VIEW "picture_materialized" AS SELECT * FROM "picture" FRESHNESS MANUAL
CREATE MATERIALIZED VIEW "auction_materialized" AS SELECT * FROM "auction" FRESHNESS MANUAL
CREATE MATERIALIZED VIEW "category_materialized" AS SELECT * FROM "category" FRESHNESS MANUAL
CREATE MATERIALIZED VIEW "countAuction_materialized" AS SELECT count(*) as "NUMBER" FROM "auction" FRESHNESS MANUAL
CREATE MATERIALIZED VIEW "countBid_materialized" AS SELECT count(*) as "NUMBER" FROM "bid" FRESHNESS MANUAL
CREATE MATERIALIZED VIEW "auctionCategory_materialized" AS SELECT "id", "title", "category", "end_date" FROM "auction" FRESHNESS MANUAL
CREATE MATERIALIZED VIEW "topHundredSellerByNumberOfAuctions_materialized" AS SELECT u.last_name, u.first_name, count(a.id) as number FROM auction a INNER JOIN "user" u ON a."user" = u.id GROUP BY a."user", u.last_name, u.first_name ORDER BY number desc FRESHNESS MANUAL
CREATE MATERIALIZED VIEW "highestBid_materialized" AS SELECT "last_name", "first_name" FROM "user" WHERE "user"."id" = (SELECT highest.highestUser FROM (SELECT "bid"."user" as highestUser, MAX("bid"."amount") FROM "bid" GROUP BY "bid"."user" ORDER BY MAX("bid"."amount") DESC) as highest Limit 1) FRESHNESS MANUAL
CREATE MATERIALIZED VIEW "priceBetween_materialized" AS SELECT "auction"."title", "bid"."amount" FROM "auction", "category", "bid" WHERE "bid"."auction" = "auction"."id" AND "auction"."category" = "category"."id" AND "bid"."amount" > 1000 AND "bid"."amount" < 1000000 AND not exists ( SELECT "category"."name" FROM "category" WHERE "category"."name" in ('Travel', 'Stamps', 'Motors')) ORDER BY "bid"."amount" DESC FRESHNESS MANUAL