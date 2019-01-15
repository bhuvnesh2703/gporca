WITH x AS ( SELECT UNDERLIER_TICK || '.' || UNDERLIER_EXCH AS Underlier, SUM (COALESCE (a.USD_EQ_PARTIAL_KAPPA, 0))/1000 AS vega FROM CDWUSER.U_EQ_MSR a WHERE a.COB_DATE = '2018-02-28' AND a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND a.CCC_TAPS_COMPANY = '0302' AND a.EXECUTIVE_MODEL IN ('VARIANCESWAP', 'CAPPEDVARIANCESWAP', 'VOLATILITYSWAP', 'CAPPEDVOLATILITYSWAP') GROUP BY UNDERLIER_TICK || '.' || UNDERLIER_EXCH ORDER BY ABS(SUM (COALESCE (a.USD_EQ_PARTIAL_KAPPA, 0))) DESC FETCH FIRST 10 ROWS ONLY ) SELECT a.RISK_MANAGER_LOCATION, UNDERLIER_TICK || '.' || UNDERLIER_EXCH AS Underlier, SUM (COALESCE (a.USD_EQ_PARTIAL_KAPPA, 0))/1000 AS vega FROM CDWUSER.U_EQ_MSR a WHERE a.COB_DATE = '2018-02-28' AND a.CCC_DIVISION = 'INSTITUTIONAL EQUITY DIVISION' AND a.CCC_TAPS_COMPANY = '0302' AND a.EXECUTIVE_MODEL IN ('VARIANCESWAP', 'CAPPEDVARIANCESWAP', 'VOLATILITYSWAP', 'CAPPEDVOLATILITYSWAP') AND a.UNDERLIER_TICK || '.' || a.UNDERLIER_EXCH IN (SELECT DISTINCT UNDERLIER FROM X) GROUP BY a.RISK_MANAGER_LOCATION, UNDERLIER_TICK || '.' || UNDERLIER_EXCH