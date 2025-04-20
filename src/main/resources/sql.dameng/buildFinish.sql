-- ----
-- Extra commands to run after the tables are created, loaded,
-- indexes built and extra's created.
-- ----
stat on bmsql_config;
stat on bmsql_warehouse;
stat on bmsql_district;
stat on bmsql_customer;
stat on bmsql_history;
stat on bmsql_oorder;
stat on bmsql_new_order;
stat on bmsql_order_line;

alter table bmsql_config read only;
alter table bmsql_item read only;

select checkpoint(100);
SP_SET_PARA_VALUE(1,'SWITCH_CONN',1);
