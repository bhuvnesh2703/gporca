Select A.COB_DATE, A.CCC_TAPS_COMPANY, A.PRODUCT_DESCRIPTION_DECOMP, SUM(cast(A.USD_CM_DELTA_DECOMP/1000 as numeric (15,5))) AS Delta, SUM(cast(ABS(a.USD_CM_DELTA_DECOMP/1000) as numeric(15,5))) as abs_Delta FROM cdwuser. U_DECOMP_MSR a WHERE A.COB_DATE IN ('2018-02-28','2018-02-27') AND A.CCC_BANKING_TRADING = 'TRADING' AND A.CCC_TAPS_COMPANY IN ('0302','0342', '0517') AND A.DIVISION = 'IED' AND USD_CM_DELTA_DECOMP is not null GROUP BY A.COB_DATE, A.CCC_TAPS_COMPANY,A.PRODUCT_DESCRIPTION_DECOMP