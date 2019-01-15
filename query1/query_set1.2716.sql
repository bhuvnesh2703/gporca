SELECT A.COB_DATE, CASE WHEN PRODUCT_TYPE_CODE IN ('BANKDEBT') THEN 'LOAN' WHEN PRODUCT_TYPE_CODE IN ('BANKDEBT','BOND', 'CASH', 'EQUITY', 'FEE', 'FLOOR', 'FX', 'GVTBOND', 'PREF', 'TRS - BOND', 'TRS - GVTBOND', 'TRD_CLAIM', 'BOND ETF', 'BONDFUT', 'BONDIL', 'CONVRT', 'CLNBOND','MUNI') THEN 'CASH' WHEN PRODUCT_TYPE_CODE IN ('CRDINDEX', 'LOANINDEX', 'MUNICDX', 'CDSOPTIDX') THEN 'INDEX' ELSE 'CDS' END AS SECTYPE, SUM( Coalesce(A.USD_PV10_BENCH,0) ) AS BPV10 FROM CDWUSER.U_DM_FIRMWIDE A WHERE COB_DATE >= '07/01/2016' AND COB_DATE <= '02/28/2018' AND CCC_TAPS_COMPANY in ('0302','0347','0853','4863','4043','6120','6837','6899','4044','0856','5869','7458') AND ( A.CCC_BUSINESS_AREA IN ( 'CREDIT-CORPORATES') OR A.CCC_PRODUCT_LINE IN ('DSP INDEX PRODUCTS TRADING','DSP INDEX PRODUCTS') ) AND NOT ( A.CCC_PRODUCT_LINE IN ('DISTRESSED TRADING') AND A.PRODUCT_TYPE_CODE IN ('GVTBOND','ETF','OPTION','CRDINDEX') ) AND NOT ( CCC_PRODUCT_LINE = 'DISTRESSED TRADING' AND CCC_STRATEGY != 'PAR LOANS TRADING' ) AND NOT ( CCC_PRODUCT_LINE LIKE '%PRIMARY%' ) AND NOT ( PRODUCT_TYPE_CODE IN ('BOND','PREF') AND FID1_SENIORITY in ('AT1','SUBT1','SUBUT2') ) GROUP BY A.COB_DATE, CASE WHEN PRODUCT_TYPE_CODE IN ('BANKDEBT') THEN 'LOAN' WHEN PRODUCT_TYPE_CODE IN ('BANKDEBT','BOND', 'CASH', 'EQUITY', 'FEE', 'FLOOR', 'FX', 'GVTBOND', 'PREF', 'TRS - BOND', 'TRS - GVTBOND', 'TRD_CLAIM', 'BOND ETF', 'BONDFUT', 'BONDIL', 'CONVRT', 'CLNBOND','MUNI') THEN 'CASH' WHEN PRODUCT_TYPE_CODE IN ('CRDINDEX', 'LOANINDEX', 'MUNICDX', 'CDSOPTIDX') THEN 'INDEX' ELSE 'CDS' END