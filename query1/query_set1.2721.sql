SELECT a.cob_date,         a.ccc_risk_manager_login,        SUM(coalesce(a.USD_EQ_DELTA_DECOMP,0)) AS delta_usd,               (Case WHEN (a.USD_EQ_DELTA_DECOMP > 0) THEN SUM(coalesce(a.USD_EQ_DELTA_DECOMP,0))  END) AS LNRAMV,               (Case WHEN (a.USD_EQ_DELTA_DECOMP < 0) THEN SUM(coalesce(a.USD_EQ_DELTA_DECOMP,0)) END) AS SNRAMV FROM           cdwuser.u_decomp_msr a WHERE (COB_DATE = '2018-02-28' or COB_DATE = '2018-02-27') and         A.CCC_DIVISION IN ('INSTITUTIONAL EQUITY DIVISION')  AND (a.CCC_STRATEGY_DETAILS in ('EMERGING MARKETS DSP','EMERGING MARKETS1','INVENTORY OPT EM','MSET EM DSP') OR a.CCC_RISK_MANAGER_LOGIN IN ('christeb','limaleo','portek','eliasc','pessoat','pintoand'))  AND a.ISSUER_COUNTRY_REGION ='LATAM'   GROUP  BY a.cob_date,         a.ccc_risk_manager_login,        a.USD_EQ_DELTA_DECOMP