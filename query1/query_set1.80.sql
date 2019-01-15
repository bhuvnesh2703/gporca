SELECT      a.COB_DATE,     a.ISSUER_COUNTRY_CODE,     sum(a.USD_EXPOSURE) AS usd_NET_exposure FROM      CDWUSER.U_CR_MSR a WHERE  /* (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-02-21') and A.CCC_PL_REPORTING_REGION in ('ASIA PACIFIC') AND      (a.VAR_EXCL_FL <> 'Y') AND     a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND      a.CCC_BANKING_TRADING = 'TRADING' AND      (a.ccc_business_area <> 'INTERNATIONAL WEALTH MGMT') AND      a.PRODUCT_TYPE_CODE NOT IN ('ASCOT', 'FUTURE') */ (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-02-21') and A.CCC_PL_REPORTING_REGION in ('ASIA PACIFIC') AND      a.CCC_BANKING_TRADING = 'TRADING' and     a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' and     a.CREDIT_RISK_ISSUER_NAME not in ('REPUBLIC OF INDONESIA', 'STEINHOFF FINANCE HOLDING GMBH',  'BAYER AG','MORGAN STANLEY'/*,'UNKNOWN'*/) and     a.CREDIT_RISK_ISSUER_NAME not like 'BERKSHIRE HATHA%' and     a.CREDIT_RISK_ISSUER_NAME not like '%REPUBLIC OF CHINA' and     (PRODUCT_TYPE_CODE in ('DEFSWAP','SWAP','OPTION') or PRODUCT_TYPE_CODE like  '%BOND%' or PRODUCT_TYPE_CODE like 'CONVRT') and     ticker not in ('7048257', '7048274') GROUP BY      a.COB_DATE,     a.ISSUER_COUNTRY_CODE