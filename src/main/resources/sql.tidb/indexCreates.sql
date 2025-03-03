create index bmsql_customer_idx
  on  bmsql_customer (c_w_id, c_d_id, c_last, c_first);
create index bmsql_oorder_idx
  on  bmsql_oorder (o_w_id, o_d_id, o_c_id, o_id);
