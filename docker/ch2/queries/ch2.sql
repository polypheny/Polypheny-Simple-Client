-- Polypheny SQL port of couchbaselabs/ch2, analytical_queries/mongodb/ch2.sql.
-- Table names refer to query-local projections in analytical_queries.py, not copied data.
-- HAVING COUNT(*) > 0 keeps grouped aggregates out of unsupported Mongo join pushdowns.

-- Q01
SELECT o.ol_number, SUM(o.ol_quantity) AS sum_qty, SUM(o.ol_amount) AS sum_amount,
       AVG(o.ol_quantity) AS avg_qty, AVG(o.ol_amount) AS avg_amount, COUNT(*) AS count_order
FROM {{orderlines}} o
WHERE o.ol_delivery_d > '2014-07-01 00:00:00'
GROUP BY o.ol_number
ORDER BY o.ol_number;

-- Q02
SELECT su.su_suppkey, su.su_name, n.n_name, i.i_id, i.i_name,
       su.su_address, su.su_phone, su.su_comment
FROM {{item}} i
JOIN {{stock}} s ON i.i_id = s.s_i_id
JOIN {{supplier}} su ON MOD(s.s_w_id * s.s_i_id, 10000) = su.su_suppkey
JOIN {{nation}} n ON su.su_nationkey = n.n_nationkey
JOIN {{region}} r ON n.n_regionkey = r.r_regionkey
JOIN (SELECT s1.s_i_id AS m_i_id, MIN(s1.s_quantity) AS m_s_quantity
      FROM {{stock}} s1
      JOIN {{supplier}} su1 ON MOD(s1.s_w_id * s1.s_i_id, 10000) = su1.su_suppkey
      JOIN {{nation}} n1 ON su1.su_nationkey = n1.n_nationkey
      JOIN {{region}} r1 ON n1.n_regionkey = r1.r_regionkey
      WHERE r1.r_name LIKE 'Europ%'
      GROUP BY s1.s_i_id) m
  ON i.i_id = m.m_i_id AND s.s_quantity = m.m_s_quantity
WHERE i.i_data LIKE '%b' AND r.r_name LIKE 'Europ%'
ORDER BY n.n_name, su.su_name, i.i_id
LIMIT 100;

-- Q03
SELECT o.o_id, o.o_w_id, o.o_d_id, SUM(o.ol_amount) AS revenue, o.o_entry_d
FROM {{customer}} c, {{neworder}} no1, {{orderlines}} o
WHERE c.c_state LIKE 'a%' AND c.c_id = o.o_c_id AND c.c_w_id = o.o_w_id
  AND c.c_d_id = o.o_d_id AND no1.no_w_id = o.o_w_id AND no1.no_d_id = o.o_d_id
  AND no1.no_o_id = o.o_id AND o.o_entry_d < '2017-03-15 00:00:00.000000'
GROUP BY o.o_id, o.o_w_id, o.o_d_id, o.o_entry_d
ORDER BY revenue DESC, o.o_entry_d;

-- Q04
SELECT o.o_ol_cnt AS ol_cnt, COUNT(*) AS order_count
FROM (SELECT DISTINCT o_id, o_w_id, o_d_id, o_ol_cnt
      FROM {{orderlines}} ol
      WHERE ol.o_entry_d >= '2015-07-01 00:00:00.000000'
        AND ol.o_entry_d < '2015-10-01 00:00:00.000000'
        AND CASE WHEN ol.ol_delivery_d IS NULL THEN FALSE ELSE
            (ol.ol_delivery_d_day > ol.o_entry_d_day + 7
          OR (ol.ol_delivery_d_day = ol.o_entry_d_day + 7
              AND SUBSTRING(ol.ol_delivery_d || '.000000', 12, 15) >= SUBSTRING(ol.o_entry_d || '.000000', 12, 15))) END) o
GROUP BY o.o_ol_cnt
ORDER BY ol_cnt;

-- Q05
SELECT n.n_name AS nname, ROUND(SUM(o.ol_amount), 2) AS revenue
FROM {{customer}} c
JOIN {{orderlines}} o ON c.c_id = o.o_c_id AND c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id
JOIN {{stock}} s ON o.o_w_id = s.s_w_id AND o.ol_i_id = s.s_i_id
JOIN {{supplier}} su ON MOD(s.s_w_id * s.s_i_id, 10000) = su.su_suppkey
JOIN {{nation}} n ON su.su_nationkey = n.n_nationkey
JOIN {{region}} r ON n.n_regionkey = r.r_regionkey
WHERE (POSITION(SUBSTRING(c.c_state, 1, 1) IN '##########%%%%%%%%%%##########%%%%%%%%%%########0123456789#######ABCDEFGHIJKLMNOPQRSTUVWXYZ%%%%%%abcdefghijklmnopqrstuvwxyz') - 1) = su.su_nationkey
  AND r.r_name = 'Asia'
  AND o.o_entry_d >= '2016-01-01 00:00:00.000000'
  AND o.o_entry_d < '2017-01-01 00:00:00.000000'
GROUP BY n.n_name
HAVING COUNT(*) > 0
ORDER BY revenue DESC;

-- Q06
SELECT CASE WHEN COUNT(*) > 0 THEN SUM(o.ol_amount) ELSE NULL END AS revenue
FROM {{orderlines}} o
WHERE o.ol_delivery_d >= '2016-01-01 00:00:00.000000'
  AND o.ol_delivery_d < '2017-01-01 00:00:00.000000' AND o.ol_amount > 600;

-- Q07
SELECT su.su_nationkey AS supp_nation, SUBSTRING(c.c_state, 1, 1) AS cust_nation,
       CAST(SUBSTRING(o.o_entry_d, 1, 4) AS INTEGER) AS l_year,
       ROUND(SUM(o.ol_amount), 2) AS revenue
FROM {{orderlines}} o
JOIN {{stock}} s ON o.ol_supply_w_id = s.s_w_id AND o.ol_i_id = s.s_i_id
JOIN {{supplier}} su ON MOD(s.s_w_id * s.s_i_id, 10000) = su.su_suppkey
JOIN {{customer}} c ON c.c_id = o.o_c_id AND c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id
JOIN {{nation}} n1 ON su.su_nationkey = n1.n_nationkey
JOIN {{nation}} n2 ON (POSITION(SUBSTRING(c.c_state, 1, 1) IN '##########%%%%%%%%%%##########%%%%%%%%%%########0123456789#######ABCDEFGHIJKLMNOPQRSTUVWXYZ%%%%%%abcdefghijklmnopqrstuvwxyz') - 1) = n2.n_nationkey
WHERE ((n1.n_name = 'Germany' AND n2.n_name = 'Cambodia')
    OR (n1.n_name = 'Cambodia' AND n2.n_name = 'Germany'))
  AND o.ol_delivery_d BETWEEN '2017-01-01 00:00:00.000000' AND '2018-12-31 00:00:00.000000'
GROUP BY su.su_nationkey, SUBSTRING(c.c_state, 1, 1), CAST(SUBSTRING(o.o_entry_d, 1, 4) AS INTEGER)
HAVING COUNT(*) > 0
ORDER BY supp_nation, cust_nation, l_year;

-- Q08
SELECT CAST(SUBSTRING(o.o_entry_d, 1, 4) AS INTEGER) AS l_year,
       ROUND(SUM(CASE WHEN n2.n_name = 'Germany' THEN o.ol_amount ELSE 0 END)
             / NULLIF(SUM(o.ol_amount), 0), 2) AS mkt_share
FROM {{orderlines}} o
JOIN {{stock}} s ON o.ol_i_id = s.s_i_id AND o.ol_supply_w_id = s.s_w_id
JOIN {{item}} i ON i.i_id = s.s_i_id
JOIN {{supplier}} su ON MOD(s.s_w_id * s.s_i_id, 10000) = su.su_suppkey
JOIN {{customer}} c ON c.c_id = o.o_c_id AND c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id
JOIN {{nation}} n1 ON n1.n_nationkey = (POSITION(SUBSTRING(c.c_state, 1, 1) IN '##########%%%%%%%%%%##########%%%%%%%%%%########0123456789#######ABCDEFGHIJKLMNOPQRSTUVWXYZ%%%%%%abcdefghijklmnopqrstuvwxyz') - 1)
JOIN {{region}} r ON n1.n_regionkey = r.r_regionkey
JOIN {{nation}} n2 ON su.su_nationkey = n2.n_nationkey
WHERE o.ol_i_id < 1000 AND r.r_name = 'Europe'
  AND o.o_entry_d BETWEEN '2017-01-01 00:00:00.000000' AND '2018-12-31 00:00:00.000000'
  AND i.i_data LIKE '%b' AND i.i_id = o.ol_i_id
GROUP BY CAST(SUBSTRING(o.o_entry_d, 1, 4) AS INTEGER)
HAVING COUNT(*) > 0
ORDER BY l_year;

-- Q09
SELECT n.n_name, CAST(SUBSTRING(o.o_entry_d, 1, 4) AS INTEGER) AS l_year,
       SUM(o.ol_amount) AS sum_profit
FROM {{orderlines}} o
JOIN {{stock}} s ON o.ol_i_id = s.s_i_id AND o.ol_supply_w_id = s.s_w_id
JOIN {{item}} i ON o.ol_i_id = i.i_id
JOIN {{supplier}} su ON MOD(s.s_w_id * s.s_i_id, 10000) = su.su_suppkey
JOIN {{nation}} n ON su.su_nationkey = n.n_nationkey
WHERE i.i_data LIKE '%bb'
GROUP BY n.n_name, CAST(SUBSTRING(o.o_entry_d, 1, 4) AS INTEGER)
HAVING COUNT(*) > 0
ORDER BY n.n_name, l_year DESC;

-- Q10
SELECT c.c_id AS cid, c.c_last AS clast, SUM(o.ol_amount) AS revenue,
       c.c_city AS ccity, c.c_phone AS cphone, n.n_name AS nname
FROM {{customer}} c, {{orderlines}} o, {{nation}} n
WHERE c.c_id = o.o_c_id AND c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id
  AND o.o_entry_d >= '2015-10-01 00:00:00.000000'
  AND o.o_entry_d < '2016-01-01 00:00:00.000000'
  AND n.n_nationkey = (POSITION(SUBSTRING(c.c_state, 1, 1) IN '##########%%%%%%%%%%##########%%%%%%%%%%########0123456789#######ABCDEFGHIJKLMNOPQRSTUVWXYZ%%%%%%abcdefghijklmnopqrstuvwxyz') - 1)
GROUP BY c.c_id, c.c_last, c.c_city, c.c_phone, n.n_name
HAVING COUNT(*) > 0
ORDER BY revenue DESC
LIMIT 20;

-- Q11
SELECT s.s_i_id, SUM(s.s_order_cnt) AS ordercount
FROM {{stock}} s
JOIN {{supplier}} su ON MOD(s.s_w_id * s.s_i_id, 10000) = su.su_suppkey
JOIN {{nation}} n ON su.su_nationkey = n.n_nationkey
WHERE n.n_name = 'Germany'
GROUP BY s.s_i_id
HAVING COUNT(*) > 0 AND SUM(s.s_order_cnt) > (
    SELECT CASE WHEN COUNT(*) > 0 THEN SUM(s1.s_order_cnt) * 0.00005 ELSE NULL END FROM {{stock}} s1
    JOIN {{supplier}} su1 ON MOD(s1.s_w_id * s1.s_i_id, 10000) = su1.su_suppkey
    JOIN {{nation}} n1 ON su1.su_nationkey = n1.n_nationkey
    WHERE n1.n_name = 'Germany')
ORDER BY ordercount DESC;

-- Q12
SELECT o.o_ol_cnt AS ol_cnt,
       SUM(CASE WHEN o.o_carrier_id IN (1, 2) THEN 1 ELSE 0 END) AS high_line_count,
       SUM(CASE WHEN o.o_carrier_id NOT IN (1, 2) THEN 1 ELSE 0 END) AS low_line_count
FROM {{orderlines}} o
WHERE o.o_entry_d <= o.ol_delivery_d
  AND o.ol_delivery_d >= '2016-01-01 00:00:00.000000'
  AND o.ol_delivery_d < '2017-01-01 00:00:00.000000'
GROUP BY o.o_ol_cnt
ORDER BY ol_cnt;

-- Q13
SELECT co.c_count, COUNT(*) AS custdist
FROM (SELECT c.c_id, COUNT(o.o_id) AS c_count FROM {{customer}} c
      LEFT JOIN {{orders}} o ON c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id
                           AND c.c_id = o.o_c_id AND o.o_carrier_id > 8
      GROUP BY c.c_id) co
GROUP BY co.c_count
ORDER BY custdist DESC, co.c_count DESC;

-- Q14
SELECT CASE WHEN COUNT(*) > 0 THEN
       100.00 * SUM(CASE WHEN i.i_data LIKE 'pr%' THEN o.ol_amount ELSE 0 END)
       / (1 + SUM(o.ol_amount)) ELSE NULL END AS promo_revenue
FROM {{orderlines}} o, {{item}} i
WHERE o.ol_i_id = i.i_id AND o.ol_delivery_d >= '2017-09-01 00:00:00.000000'
  AND o.ol_delivery_d < '2017-10-01 00:00:00.000000';

-- Q15
SELECT su.su_suppkey, su.su_name, su.su_address, su.su_phone, r.total_revenue
FROM {{supplier}} su,
     (SELECT MOD(s.s_w_id * s.s_i_id, 10000) AS supplier_no, SUM(o.ol_amount) AS total_revenue
      FROM {{orderlines}} o, {{stock}} s WHERE o.ol_i_id = s.s_i_id AND o.ol_supply_w_id = s.s_w_id
        AND o.ol_delivery_d >= '2018-01-01 00:00:00.000000'
        AND o.ol_delivery_d < '2018-04-01 00:00:00.000000'
      GROUP BY MOD(s.s_w_id * s.s_i_id, 10000)) r
WHERE su.su_suppkey = r.supplier_no AND r.total_revenue = (
    SELECT MAX(r1.total_revenue) FROM (
        SELECT MOD(s.s_w_id * s.s_i_id, 10000) AS supplier_no, SUM(o.ol_amount) AS total_revenue
        FROM {{orderlines}} o, {{stock}} s WHERE o.ol_i_id = s.s_i_id AND o.ol_supply_w_id = s.s_w_id
          AND o.ol_delivery_d >= '2018-01-01 00:00:00.000000'
          AND o.ol_delivery_d < '2018-04-01 00:00:00.000000'
        GROUP BY MOD(s.s_w_id * s.s_i_id, 10000)) r1)
ORDER BY su.su_suppkey;

-- Q16
SELECT i.i_name AS iname, SUBSTRING(i.i_data, 1, 3) AS brand, i.i_price AS iprice,
       COUNT(DISTINCT MOD(s.s_w_id * s.s_i_id, 10000)) AS supplier_cnt
FROM {{stock}} s, {{item}} i
WHERE i.i_id = s.s_i_id AND i.i_data NOT LIKE 'zz%'
  AND MOD(s.s_w_id * s.s_i_id, 10000) NOT IN (
      SELECT su.su_suppkey FROM {{supplier}} su WHERE su.su_comment LIKE '%Customer%Complaints%')
GROUP BY i.i_name, SUBSTRING(i.i_data, 1, 3), i.i_price
ORDER BY supplier_cnt DESC;

-- Q17
SELECT CASE WHEN COUNT(*) > 0 THEN SUM(o.ol_amount) / 2.0 ELSE NULL END AS avg_yearly
FROM {{orderlines}} o,
     (SELECT i.i_id AS iid, AVG(o1.ol_quantity) AS a FROM {{item}} i, {{orderlines}} o1
      WHERE i.i_data LIKE '%b' AND o1.ol_i_id = i.i_id GROUP BY i.i_id) t
WHERE o.ol_i_id = t.iid AND o.ol_quantity < t.a;

-- Q18
SELECT c.c_last AS clast, c.c_id, o.o_id, o.o_entry_d, o.o_ol_cnt, SUM(o.ol_amount) AS ol_sum
FROM {{customer}} c, {{orderlines}} o
WHERE c.c_id = o.o_c_id AND c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id
GROUP BY o.o_id, o.o_w_id, o.o_d_id, c.c_id, c.c_last, o.o_entry_d, o.o_ol_cnt
HAVING SUM(o.ol_amount) > 200
ORDER BY ol_sum DESC, o.o_entry_d
LIMIT 100;

-- Q19
SELECT CASE WHEN COUNT(*) > 0 THEN SUM(o.ol_amount) ELSE NULL END AS revenue
FROM {{orderlines}} o, {{item}} i
WHERE ((i.i_data LIKE '%h' AND o.ol_quantity BETWEEN 7 AND 17
        AND i.i_price BETWEEN 1 AND 5 AND o.o_w_id IN (37, 29, 70))
    OR (i.i_data LIKE '%t' AND o.ol_quantity BETWEEN 16 AND 26
        AND i.i_price BETWEEN 1 AND 10 AND o.o_w_id IN (78, 17, 6))
    OR (i.i_data LIKE '%m' AND o.ol_quantity BETWEEN 24 AND 34
        AND i.i_price BETWEEN 1 AND 15 AND o.o_w_id IN (91, 95, 15)))
  AND o.ol_i_id = i.i_id AND i.i_price BETWEEN 1 AND 15;

-- Q20
SELECT su.su_name, su.su_address
FROM {{supplier}} su, {{nation}} n
WHERE su.su_suppkey IN (
    SELECT MOD(s.s_i_id * s.s_w_id, 10000) FROM {{stock}} s
    JOIN {{item}} i ON s.s_i_id = i.i_id
    JOIN {{orderlines}} o ON o.ol_i_id = s.s_i_id
    WHERE i.i_data LIKE 'co%' AND o.ol_delivery_d >= '2016-01-01 12:00:00'
      AND o.ol_delivery_d < '2017-01-01 12:00:00'
    GROUP BY s.s_i_id, s.s_w_id, s.s_quantity
    HAVING COUNT(*) > 0 AND 20 * s.s_quantity > SUM(o.ol_quantity))
  AND su.su_nationkey = n.n_nationkey AND n.n_name = 'Germany'
ORDER BY su.su_name;

-- Q21
SELECT z.su_name, COUNT(*) AS numwait
FROM (
    SELECT su.su_name
    FROM {{orderlines}} o
    JOIN {{stock}} s ON o.o_w_id = s.s_w_id AND o.ol_i_id = s.s_i_id
    JOIN {{supplier}} su ON MOD(s.s_w_id * s.s_i_id, 10000) = su.su_suppkey
    JOIN {{nation}} n ON su.su_nationkey = n.n_nationkey
    JOIN (SELECT o_id, o_w_id, o_d_id, MAX(ol_delivery_d) AS last_delivery
          FROM {{orderlines}} ol
          WHERE ol.o_entry_d BETWEEN '2017-12-01 00:00:00' AND '2017-12-31 00:00:00'
          GROUP BY o_id, o_w_id, o_d_id HAVING COUNT(*) > 0) last_line
      ON last_line.o_id = o.o_id AND last_line.o_w_id = o.o_w_id AND last_line.o_d_id = o.o_d_id
    WHERE n.n_name = 'Peru'
      AND o.o_entry_d BETWEEN '2017-12-01 00:00:00' AND '2017-12-31 00:00:00'
      AND CASE WHEN o.ol_delivery_d IS NULL THEN FALSE ELSE
          (o.ol_delivery_d_day > o.o_entry_d_day + 150
        OR (o.ol_delivery_d_day = o.o_entry_d_day + 150
            AND SUBSTRING(o.ol_delivery_d || '.000000', 12, 15) > SUBSTRING(o.o_entry_d || '.000000', 12, 15))) END
    GROUP BY o.o_w_id, o.o_d_id, o.o_id, n.n_nationkey, su.su_suppkey, s.s_w_id, s.s_i_id, su.su_name
    HAVING MIN(o.ol_delivery_d) = MAX(last_line.last_delivery) AND COUNT(*) > 0) z
GROUP BY z.su_name
LIMIT 100;

-- Q22
SELECT SUBSTRING(c.c_state, 1, 1) AS country, COUNT(*) AS numcust, SUM(c.c_balance) AS totacctbal
FROM {{customer}} c
LEFT JOIN (SELECT DISTINCT o_c_id, o_w_id, o_d_id FROM {{orders}} o
           WHERE o.o_entry_d BETWEEN '2013-12-01 00:00:00' AND '2013-12-31 00:00:00') o
  ON o.o_c_id = c.c_id AND o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id
WHERE SUBSTRING(c.c_phone, 1, 1) IN ('1', '2', '3', '4', '5', '6', '7')
  AND c.c_balance > (SELECT AVG(c1.c_balance) FROM {{customer}} c1
                    WHERE c1.c_balance > 0.00 AND SUBSTRING(c1.c_phone, 1, 1) IN ('1', '2', '3', '4', '5', '6', '7'))
  AND o.o_c_id IS NULL
GROUP BY SUBSTRING(c.c_state, 1, 1)
ORDER BY country;
