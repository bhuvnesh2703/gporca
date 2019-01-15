SELECT COB_DATE, SUM (COALESCE(USD_IR_UNIFIED_PV01, 0)) AS USD_IR_UNIFIED_PV01 FROM cdwuser.U_EXP_TRENDS WHERE cob_date in ('2018-02-28', '2018-02-27') AND book='TSTJY' GROUP BY COB_DATE