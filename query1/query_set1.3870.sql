SELECT
    A.CCC_PL_REPORTING_REGION,
    A.COB_DATE,
    A.RATING2,
    A.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME,
    ABS (SUM (A.USD_PV10_BENCH)) AS ABS_USD_PV10_BENCH,
    ABS (SUM (A.USD_PV01SPRD)) AS ABS_USD_PV01SPRD
FROM cdwuser.U_DM_CC A
WHERE
    A.CREDIT_TRADING_FLAG = 'Flow Trading' AND
    A.CCC_BUSINESS_AREA IN ('CREDIT-CORPORATES', 'DSP - CREDIT') AND
    A.COB_DATE in ('2018-02-28','2018-02-27') AND
    A.CCC_PRODUCT_LINE <> 'CREDIT CORP MANAGEMENT'
GROUP BY
    A.CCC_PL_REPORTING_REGION,
    A.COB_DATE,
    A.RATING2,
    A.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME