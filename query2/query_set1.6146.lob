select 
a.ISSUER_COUNTRY_CODE_DECOMP, a.COB_DATE, LE_GROUP,
    sum(a.USD_EQ_DELTA_DECOMP)/1000 as Delta
from cdwuser.u_Decomp_msr a
where 
 cob_date IN ('2018-02-28','2018-02-21')
and a.DIVISION = 'IED' and
a.ISSUER_COUNTRY_CODE_DECOMP in ('ITA','ESP','PRT','GRC') and 
a.CCC_BANKING_TRADING = 'TRADING'
group by 
a.ISSUER_COUNTRY_CODE_DECOMP, a.COB_DATE, LE_GROUP
order by abs(sum(a.USD_EQ_DELTA_DECOMP)) desc