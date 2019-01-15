SELECT      a.cob_date AS VARDATE,      a.CURRENCY_OF_MEASURE,     Sum(usd_fx) AS USD_FX FROM     cdwuser.u_fx_msr a  WHERE     (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-02-21') and A.CCC_PL_REPORTING_REGION in ('ASIA PACIFIC') AND      A.CCC_DIVISION IN ('INSTITUTIONAL EQUITY DIVISION')  AND      (a.ccc_business_area <> 'INTERNATIONAL WEALTH MGMT') AND        CCC_BANKING_TRADING='TRADING' group by     cob_date,     a.CURRENCY_OF_MEASURE