--创建分区组
DECLARE
    SQLSTR  VARCHAR(8188)\;
    WSTEP   INT\;
    WLEFT   INT\;
    PSTEP   INT\;
    PLEFT   INT\;
    WID     INT\;
    PID     INT\;
    PID2    INT\;
    K       INT\;
    WAREHOUSE INT\;  --总仓库数
    PARTITION_NUM INT\;  --分区数
    TBS_NUM INT\;    --表空间数
    E1      EXCEPTION FOR -20051, '仓库数不能小于分区数'\;
    E2      EXCEPTION FOR -20051, '分区数不能小于表空间数'\;
BEGIN
    SELECT COUNT(*) INTO TBS_NUM  FROM DPC_TABLESPACE\;
    WAREHOUSE = LJL\;
    PARTITION_NUM = TBS_NUM \;
    WSTEP   = WAREHOUSE / PARTITION_NUM\;  --每个分区的仓库数
    WLEFT   = WAREHOUSE % PARTITION_NUM\;  --剩余仓库数,平均分布到前几个分区
    PSTEP   = PARTITION_NUM / TBS_NUM\;  --每个表空间的分区数
    PLEFT   = PARTITION_NUM % TBS_NUM\;  --剩余分区数,平均分布到前几个表空间
    WID     =0\;         --仓库ID
    PID     =0\;         --分区ID

    IF WSTEP = 0 THEN
        RAISE E1\;
    END IF\;

    IF PSTEP = 0 THEN
        RAISE E2\;
    END IF\;

    --构造SQL
    --PRINT 'CREATE PARTITION GROUP BENCHMARKSQL.TPCC_GROUP PARTITION BY RANGE(INT)('\;
    SQLSTR = 'CREATE PARTITION GROUP TPCC_GROUP PARTITION BY RANGE(INT)('\;
    --遍历表空间和分区
    FOR I IN 1..TBS_NUM LOOP
        --计算表空间包含的分区id范围
        PID2 = PID + PSTEP + CASE WHEN I <= PLEFT THEN 1 ELSE 0 END\;
        FOR J IN PID + 1..PID2 LOOP
            --计算分区对应的仓库id上限
            WID = WID + WSTEP + CASE WHEN J <= WLEFT THEN 1 ELSE 0 END\;
            --构造除最后一个分区以外的分区sql
            IF J != PARTITION_NUM THEN
                k = j % TBS_NUM\;
                IF K = 0 THEN
                    K = TBS_NUM\;
                END IF\;
                IF K < 10 THEN
                    SQLSTR = SQLSTR || chr(10) || 'PARTITION P' || J || ' VALUES EQU OR LESS THAN (' || WID || ') STORAGE (ON TS_00' || K || '),'\;
                ELSEIF K < 100 THEN
                    SQLSTR = SQLSTR || chr(10) || 'PARTITION P' || J || ' VALUES EQU OR LESS THAN (' || WID || ') STORAGE (ON TS_0' || K || '),'\;
                ELSE 
                    SQLSTR = SQLSTR || chr(10) || 'PARTITION P' || J || ' VALUES EQU OR LESS THAN (' || WID || ') STORAGE (ON TS_' || K || '),'\;
                END IF\;
            END IF\;
        END LOOP\;
        PID = PID2\;
    END LOOP\;
    --构造最后一个分区sql
    k = PARTITION_NUM % TBS_NUM\;
    IF K = 0 THEN K = TBS_NUM\;END IF\;
    IF K < 10 THEN
        SQLSTR = SQLSTR || chr(10) || 'PARTITION P' || PARTITION_NUM || ' VALUES LESS THAN (MAXVALUE) STORAGE (ON TS_00' || k || '))\;'\;
    ELSEIF K < 100 THEN
        SQLSTR = SQLSTR || chr(10) || 'PARTITION P' || PARTITION_NUM || ' VALUES LESS THAN (MAXVALUE) STORAGE (ON TS_0' || k || '))\;'\;
    ELSE 
        SQLSTR = SQLSTR || chr(10) || 'PARTITION P' || PARTITION_NUM || ' VALUES LESS THAN (MAXVALUE) STORAGE (ON TS_' || k || '))\;'\;
    END IF\;
    --PRINT SQLSTR\;
    EXECUTE IMMEDIATE SQLSTR\;
END;
