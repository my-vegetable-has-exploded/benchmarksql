-- Condition 1: W_YTD= sum(D_YTD)
SELECT /*+ no_use_px parallel(8) */ * FROM(
    SELECT w.w_id, w.w_ytd, d.sum_d_ytd
    FROM bmsql_warehouse w,
    (SELECT /*+ no_use_px parallel(8) */ d_w_id, sum(d_ytd) sum_d_ytd FROM bmsql_district GROUP BY d_w_id) d 
    WHERE w.w_id= d.d_w_id
) x 
WHERE w_ytd != sum_d_ytd; 

-- xxxxxxxxx
-- Condition 2: D_NEXT_O_ID - 1= max(O_ID)= max(NO_O_ID)
SELECT /*+ no_use_px parallel(8) */ * FROM(
    SELECT d.d_w_id, d.d_id, d.d_next_o_id, o.max_o_id, no.max_no_o_id
    FROM bmsql_district d,
        (SELECT /*+ no_use_px parallel(8) */ o_w_id, o_d_id, MAX(o_id) max_o_id FROM bmsql_oorder GROUP BY o_w_id, o_d_id) o,
        (SELECT /*+ no_use_px parallel(8) */ no_w_id, no_d_id, MAX(no_o_id) max_no_o_id FROM bmsql_new_order GROUP BY no_w_id, no_d_id) no
    WHERE d.d_w_id= o.o_w_id AND d.d_w_id= no.no_w_id AND d.d_id= o.o_d_id AND d.d_id= no.no_d_id
) x
WHERE d_next_o_id - 1!= max_o_id OR d_next_o_id - 1!= max_no_o_id; 

 -- Condition 3: max(NO_O_ID) - min(NO_O_ID)+ 1 
 -- = [number of rows in the NEW-OROER table for this bmsql_district]
SELECT /*+ no_use_px paratLel(8) */ * FROM(
    SELECT /*+ no_use_px parallel(8) */ no_w_id, no_d_id, MAX(no_o_id) max_no_o_id, MIN(no_o_id) min_no_o_id, COUNT(*) count_no
    FROM bmsql_new_order 
    GROUP BY no_w_id, no_d_Id
) x
WHERE max_no_o_id - min_no_o_id+ 1!= count_no;

-- Empty set (3 min 7.60 sec)
-- Condition 4: sum(O_OL_CNT)
-- [number of rows in the ORDER-LINE table for this bmsql_district]
SELECT /*+ no_use_px parallel(8) */ * FROM (
    SELECT o.o_w_id, o.o_d_id, o.sum_o_ol_cnt, ol.count_ol
        FROM (SELECT /*+ no_use_px parallel(8) */ o_w_id, o_d_id, SUM(o_ol_cnt) sum_o_ol_cnt FROM bmsql_oorder GROUP BY o_w_id, o_d_id) o,
             (SELECT /*+ no_use_px parallel(8) */ ol_w_id, ol_d_id, COUNT(*) count_ol FROM bmsql_order_line GROUP BY ol_w_id, ol_d_id) ol
        WHERE o.o_w_id = ol.ol_w_id AND o.o_d_id = ol.ol_d_id
) x
WHERE sum_o_ol_cnt != count_ol;

--Empty set (2 min 1.07 sec)
-- Condition 5: For any row in the ORDER table, O_CARRIER_ID is set to a null
-- value if and only if there is a corresponding row in the
-- NEW-ORDER table
SELECT /*+ no_use_px parallel(8) */ * FROM (
    SELECT o.o_w_id, o.o_d_id, o.o_id, o.o_carrier_id, no.count_no
        FROM bmsql_oorder o,                                                         
            (SELECT /*+ no_use_px parallels) */ no_w_id, no_d_id, no_o_id, COUNT(*) count_no FROM bmsql_new_order GROUP BY no_w_id, no_d_id, no_o_id) no
        WHERE o.o_w_id = no.no_w_id AND o.o_d_id = no.no_d_id AND o.o_id = no.no_o_id
) x
WHERE (o_carrier_id IS NULL AND count_no = 0) OR (o_carrier_id IS NOT NULL AND count_no != 0);


-- Empty set (9 min 13.57 sec)
-- Condition 6: For any row in the ORDER table, O_OL_CNT must equal the number
-- of rows in the ORDER-LINE table for the corresponding order
SELECT /*+ no_use_px parallel(8) */ * FROM (
    SELECT o.o_w_id, o.o_d_id, o.o_id, o.o_ol_cnt, ol.count_ol
        FROM bmsql_oorder o,
             (SELECT /*+ no_use_px parallel(8) */ ol_w_id, ol_d_id, ol_o_id, COUNT(*) count_ol FROM bmsql_order_line GROUP BY ol_w_id, ol_d_id, ol_o_id) ol
         WHERE o.o_w_id = ol.ol_w_id AND o.o_d_id = ol.ol_d_id AND o.o_id = ol.ol_o_id
) x
WHERE o_ol_cnt != count_ol;

--Empty set (13 min 27.97 sec)
-- Condition 7: For any row in the ORDER-LINE table, OL_DELIVERY_D is set to
-- a null date/time if and only if the corresponding row in the
-- ORDER table has O_CARRIER_ID set to a null value
SELECT /*+ no_use_px parallel(8) */ * FROM (
    SELECT /*+ no_use_px parallel(8) */ ol.ol_w_id, ol.ol_d_id, ol.ol_o_id, ol.ol_delivery_d, o.o_carrier_id
        FROM bmsql_order_line ol, bmsql_oorder o
            WHERE ol.ol_w_id = o.o_w_id AND
                  ol.ol_d_id = o.o_d_id AND
                  ol.ol_o_id = o.o_id
) x
WHERE (ol_delivery_d IS NULL AND o_carrier_id IS NOT NULL) OR
       (ol_delivery_d IS NOT NULL AND o_carrier_id IS NULL);

--Empty set (22.70 sec)
-- Condition 8: W_YTD - sum(H_AMOUNT)
SELECT /*+ no_use_px parallel(8) */ * FROM (
    SELECT w.w_id, w.w_ytd, h.sum_h_amount
        FROM bmsql_warehouse w,
             (SELECT /*+ no_use_px parallel(8) */ h_w_id, SUM(h_amount) sum_h_amount FROM bmsql_history GROUP BY h_w_id) h
        WHERE w.w_id = h.h_w_id) x
WHERE w_ytd != sum_h_amount;

--Empty set (24.41 sec)
-- Condition 9: D_YTD - sum(H_AMOUNT)
SELECT /*+ no_use_px parallel(8) */ * FROM (
    SELECT d.d_w_id, d.d_id, d.d_ytd, h.sum_h_amount
        FROM bmsql_district d,
             (SELECT /*+ no_use_px parallel(8) */ h_w_id, h_d_id, SUM(h_amount) sum_h_amount FROM bmsql_history GROUP BY h_w_id, h_d_id) h
        WHERE d.d_w_id = h.h_w_id AND d.d_id = h.h_d_id
) x
WHERE d_ytd != sum_h_amount;

-- Condition 10: C_BALANCE = sum(OL_AMOUNT) - sum(H_AMOUNT)
-- 可查询全部数据，也可选择任意范围的warehouse（如下）检查， 结果应该为空
select *
from 
(select c_w_id,c_d_id,c_id,c_balance from bmsql_customer where c_w_id=1) cb,
(select h_c_w_id,h_c_d_id,h_c_id,sum(h.h_amount) sum_h_amount from bmsql_history h where h_c_w_id=1 group by h_c_w_id,h_c_d_id,h_c_id) ha,
(select o.o_w_id,o.o_d_id,o.o_c_id,sum(ol_amount) sum_ol_amount from bmsql_oorder o, bmsql_order_line ol where  ol.ol_w_id=o.o_w_id and ol.ol_d_id=o.o_d_id and ol.ol_o_id=o.o_id and ol.ol_delivery_d is not null and o.o_w_id=1 group by o.o_w_id,o.o_d_id,o.o_c_id) ola
where
cb.c_w_id=ha.h_c_w_id and cb.c_d_id=ha.h_c_d_id and cb.c_id=ha.h_c_id and cb.c_w_id=ola.o_w_id and cb.c_d_id=ola.o_d_id and cb.c_id=ola.o_c_id
and cb.c_balance <> sum_ol_amount-sum_h_amount;

-- Condistion 11: (count(*) from ORDER) - (count(*) from NEW-ORDER)) - (sum(delivery_cnt) from customer) =900
-- 可查询全部数据，也可选择任意范围的warehouse（如下）检查， 结果应该为空
select *
from 
(select o_w_id, o_d_id, count(*) as su1 from bmsql_oorder where o_w_id = 2 group by o_w_id, o_d_id) o_cnt,
(select no_w_id, no_d_id, count(*) as su2 from bmsql_new_order where no_w_id = 2 group by no_w_id, no_d_id) no_cnt,
(select c_w_id, c_d_id, sum(c_delivery_cnt) as su3 from bmsql_customer where c_w_id = 2 group by c_w_id, c_d_id) c_cnt
where o_cnt.o_w_id=no_cnt.no_w_id and o_cnt.o_d_id=no_cnt.no_d_id and o_cnt.o_w_id=c_cnt.c_w_id and o_cnt.o_d_id=c_cnt.c_d_id
and su2+su3-su1<>900;

--Condistion 12: C_BALANCE +C_YTD_PAYMENT = sum(OL_AMOUNT)
-- 可查询全部数据，也可选择任意范围的warehouse（如下）检查， 结果应该为空
select *
from 
(select c_w_id,c_d_id,c_id,c_balance,c_ytd_payment from bmsql_customer where c_w_id= 1) cb,
(select o.o_w_id,o.o_d_id,o.o_c_id,nvl(sum(ol_amount),0) sum_ol_amount from bmsql_oorder o, bmsql_order_line ol where  ol.ol_w_id=o.o_w_id and ol.ol_d_id=o.o_d_id and ol.ol_o_id=o.o_id and ol.ol_delivery_d is not null and o.o_w_id=1 group by o.o_w_id,o.o_d_id,o.o_c_id) ola
where cb.c_w_id=ola.o_w_id and cb.c_d_id=ola.o_d_id and cb.c_id=ola.o_c_id
and cb.c_balance + cb.c_ytd_payment <> sum_ol_amount;
