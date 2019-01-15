Select
cob_date,
MRD_RATING, 
sum(USD_DEFAULT_PNL) as JTD 
from cdwuser.u_cr_msr a
where 
    A.COB_DATE in ('2018-02-28','2018-01-31')
    and CCC_BUSINESS_AREA in ('DSP - CREDIT') 
    and CRM_FL = 'Y' 
group by
cob_date,
MRD_RATING