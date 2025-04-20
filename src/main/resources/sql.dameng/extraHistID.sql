-- ----
-- Extra Schema objects/definitions for history.hist_id in Oracle
-- ----

-- ----
--	This is an extra column not present in the TPC-C
--	specs. It is useful for replication systems like
--	Bucardo and Slony-I, which like to have a primary
--	key on a table. It is an auto-increment or serial
--	column type. The definition below is compatible
--	with Oracle 11g, using the sequence in a trigger.
-- ----
-- Adjust the sequence above the current max(hist_id)

create or replace procedure createsequence 
as
 n integer\;
 stmt1 varchar(200)\;
 begin 
   select count(*)+1 into n from BMSQL_history\;
   if(n != 1) then
      select max(hist_id) + 1 into n from BMSQL_history\;
   end if\;
   PRINT n\;
   stmt1:='create sequence bmsql_hist_id_seq start with '||n||' MAXVALUE 9223372036854775807 CACHE 50000;'\;
   EXECUTE IMMEDIATE stmt1\;
end;

call createsequence;
alter table BMSQL_history modify hist_id integer default (bmsql_hist_id_seq.nextval);
