Select
COB_DATE,
sum(a.USD_IRPV01SPRD)*100 as IRPV01
from cdwuser.U_IR_MSR a
where
    A.COB_DATE in ('2018-02-28','2018-01-31')
    and CCC_STRATEGY = 'LEGACY MUNI DERIVATIVES'
    and a.CURVE_NAME like '%aco%'
Group by COB_DATE