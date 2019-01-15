SELECT COB_DATE, ROW_NUMBER() OVER(PARTITION BY COB_DATE ORDER BY USD_EXPOSURE DESC) AS ROW, COUNTRY_CD_OF_RISK, USD_EXPOSURE, IR_PV01, USD_BPV_10 FROM ( SELECT A.COB_DATE, A.COUNTRY_CD_OF_RISK, SUM(ROUND(COALESCE(A.USD_EXPOSURE,0),5)) AS USD_EXPOSURE, SUM (ROUND(COALESCE(A.USD_IR_UNIFIED_PV01,0),5)) AS IR_PV01, SUM(ROUND(COALESCE(A.USD_PV10_BENCH + A.USD_PV10_SYNTHETIC,0),5)) AS USD_BPV_10 FROM CDWUSER.U_DM_FIRMWIDE A WHERE COB_DATE IN ( '01/31/2018','02/28/2018') AND VAR_EXCL_FL <> 'Y' AND BU_RISK_SYSTEM <> 'PIPELINE_NY' AND A.DIVISION_GROUP = 'ISG CORE' AND CCC_DIVISION NOT IN ('FIC DVA','FID DVA') AND CCC_BUSINESS_AREA NOT IN ('CREDIT DERIVATIVE PROD', 'DSP - CREDIT', 'GLOBAL STRUCT PRODUCTS', 'DERIVS FUNDING') AND A.USD_EXPOSURE IS NOT NULL AND A.COUNTRY_CD_OF_RISK IN ('PRT', 'ESP', 'IRL', 'ITA', 'GRC') GROUP BY A.COB_DATE, A.COUNTRY_CD_OF_RISK ) B