SELECT a.BOOK, a.ACCOUNT, a.SPG_DESC, SUM (usd_exposure) AS net_exposure, SUM (USD_PV01SPRD) AS SPV01, a.COB_DATE, a.ccc_business_area, a.INSURER_RATING, A.VINTAGE, a.COUNTRY_CD_OF_RISK, a.tapscusip FROM cdwuser.U_CR_MSR a WHERE a.ccc_business_area IN ('SECURITIZED PRODUCTS GRP') AND a.COUNTRY_CD_OF_RISK IN ('ITA', 'PRT', 'IRL', 'GRC', 'ESP') AND a.ccc_pl_reporting_region IN ('EMEA') AND a.SPG_DESC not in('CORPORATE INDEX','CORPORATE DEFAULT SWAP','CORPORATE CLO','CMBS LOAN', 'WAREHOUSE ABS LOAN') and a.COB_DATE in ('2018-02-28','2018-02-21') group by a.BOOK, a.ACCOUNT, a.SPG_DESC, a.ccc_business_area, a.COB_DATE, a.INSURER_RATING, A.VINTAGE, a.COUNTRY_CD_OF_RISK, a.tapscusip