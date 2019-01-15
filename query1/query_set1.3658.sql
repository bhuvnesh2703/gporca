select a.cob_date,a.CCC_BUSINESS_AREA,a.CCC_HIERARCHY_LEVEL8, a.ISSUER_COUNTRY_CODE ,(case when (a.COB_DATE = '2018-02-28') then sum(a.USD_EQ_DELTA_DECOMP) else 0 end) AS Delta_COB ,(case when (a.COB_DATE = '2018-02-28') then 0 else sum(a.USD_EQ_DELTA_DECOMP) end) AS Delta_COMP From cdwuser.U_DM_EQ a where (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-02-21') and a.IS_UK_GROUP = 'Y' AND a.CCC_DIVISION='INSTITUTIONAL EQUITY DIVISION'  AND  a.CCC_BANKING_TRADING<>'BANKING'  Group by a.cob_date,a.CCC_BUSINESS_AREA,a.CCC_HIERARCHY_LEVEL8, a.ISSUER_COUNTRY_CODE