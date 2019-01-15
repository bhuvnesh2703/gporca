SELECT a.COB_Date, a.CCC_BUSINESS_AREA,  SUM(a.USD_NOTIONAL) AS NOTIONAL  FROM cdwuser.U_DM_CVA a WHERE  (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-01') and CCC_PL_REPORTING_REGION in ('EMEA') AND   a.CCC_BUSINESS_AREA IN ('MS CVA MNE - FID', 'MS CVA MNE - COMMOD')  AND a.IS_HEDGE_INSTRUMENT = 'Y' AND a.USD_NOTIONAL IS NOT NULL AND a.BU_RISK_SYSTEM LIKE 'C1%' and a.ccc_product_line not in ('CREDIT LOAN PORTFOLIO','CMD STRUCTURED FINANCE') GROUP BY a.COB_Date, a.CCC_BUSINESS_AREA