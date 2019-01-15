Select
COB_DATE,
a.FID1_INDEX_FAMILY||'.'||FID1_INDEX_SECTOR||'.'||FID1_INDEX_SERIES as INDEX,
sum(CASE WHEN COB_DATE = '2018-02-28' THEN Coalesce(USD_INDEX_PV01,0) ELSE 0 END) AS INDEX_BASIS_01,
sum(CASE WHEN COB_DATE = '2018-02-27' THEN Coalesce(USD_INDEX_PV01,0) ELSE 0 END) AS INDEX_BASIS_01_DOD,
ABS(sum(CASE WHEN COB_DATE = '2018-02-28' THEN Coalesce(USD_INDEX_PV01,0) ELSE 0 END)) ABS_INDEX_BASIS_01
from cdwuser.U_IR_MSR a
where 
    A.COB_DATE in ('2018-02-28','2018-02-27')
    and CCC_BUSINESS_AREA = 'DSP - CREDIT'
    and FID1_INDEX_FAMILY not in ('BESPOKE INDEX','UNDEFINED')
Group by
COB_DATE,
a.FID1_INDEX_FAMILY||'.'||FID1_INDEX_SECTOR||'.'||FID1_INDEX_SERIES