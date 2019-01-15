With CRMBooks(ApprovedBook) as (Values
('ABCOR'), ('ABDYN'), ('ABEPS'), ('ABIGG'), ('ABITX'), ('ABMAN'), ('ABSCP'), ('ABTRI'), ('ACHPP'), ('ACUBE')
, ('CSTRX'), ('DSTRD'), ('HYATT'), ('HYCDX'), ('TIREV'), ('TOCBO'), ('TOCTP'), ('TOFUN'), ('TOHOT'), ('TOJPC')
, ('TOMIX'), ('TOSYN'), ('TOVEN'), ('TQNTO'), ('TRCDX'), ('TSIDX'), ('TIATM'), ('TOCAP') )

Select
COB_DATE,
BOOK,
Case When ApprovedBook is null Then 'N' Else 'Y' End as IS_CRM_APPROVED_BOOK,
sum(a.USD_DEFAULT_PNL) LGD
from cdwuser.U_CR_MSR a
Left Join CRMBooks b
on (a.BOOK = b.ApprovedBook)
where
    A.COB_DATE in ('2018-02-28','2018-02-27')
    and CCC_BUSINESS_AREA = 'DSP - CREDIT'
    and CRM_FL = 'Y'
group by
COB_DATE,
BOOK,
Case When ApprovedBook is null Then 'N' Else 'Y' End