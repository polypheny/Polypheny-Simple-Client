CREATE VIEW "user_view" AS SELECT * FROM "user"
CREATE VIEW "bid_view" AS SELECT * FROM "bid"
CREATE VIEW "picture_view" AS SELECT * FROM "picture"
CREATE VIEW "auction_view" AS SELECT * FROM "auction"
CREATE VIEW "category_view" AS SELECT * FROM "category"
CREATE VIEW "countAuction" AS SELECT count(*) as "NUMBER" FROM "auction"
CREATE VIEW "countBid" AS SELECT count(*) as "NUMBER" FROM "bid"
CREATE VIEW "auctionCategory_view" AS SELECT "id", "title", "end_date", "category" FROM "auction"
CREATE VIEW topHundredSellerByNumberOfAuctions_view AS SELECT u.last_name, u.first_name, count(a.id) as number FROM auction a INNER JOIN "user" u ON a."user" = u.id GROUP BY a."user", u.last_name, u.first_name ORDER BY number desc Limit 100
CREATE VIEW highestBid_view AS SELECT "last_name", "first_name" FROM "user" WHERE "user"."id" = (SELECT highest.highestUser FROM (SELECT "bid"."user" as highestUser, MAX("bid"."amount") FROM "bid" GROUP BY "bid"."user" ORDER BY MAX("bid"."amount") DESC) as highest Limit 1)
CREATE VIEW "priceBetween_view" AS SELECT "auction"."title", "bid"."amount" FROM "auction", "bid" WHERE "bid"."auction" = "auction"."id" AND "bid"."amount" > 10 AND "bid"."amount" < 1000000 ORDER BY "bid"."amount" DESC Limit 100