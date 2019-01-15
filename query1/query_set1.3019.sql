SELECT SUM ((USD_FX) :: numeric(15,5)) AS FX, cob_date, book, currency_of_measure, vertical_system, CCC_PRODUCT_LINE, CCC_STRATEGY, ccc_business_area, a.ccc_division FROM cdwuser.U_FX_MSR a WHERE cob_date IN ( '2018-02-28', '2018-01-31', '2017-12-29', '2017-11-30', '2018-01-30', '2017-10-31', '2017-09-29' ) AND CCC_TAPS_COMPANY IN ('1633') AND a.ccc_division IN ('FIXED INCOME DIVISION') AND a.CCC_PL_REPORTING_REGION not in ('AMERICAS') AND usd_fx <> 0 AND currency_of_measure IN ('SAR', 'DKK', 'HKD', 'AED', 'QAR', 'CNY', 'CNH') AND ccc_banking_trading_localreg = 'TRADING' AND ccc_business_area NOT IN ('CPM', 'DSP - CREDIT', 'LENDING', 'OTC CLIENT CLEARING') AND (A.PRODUCT_TYPE_CODE NOT IN ('FXOPT') OR BOOK IN ('BASKET', 'BASKET HEDGES', 'CEEMEA MULTICCY', 'CORRELATION SWAP', 'CORRELATION SWAP 2', 'CORRELATION SWAP 3', 'CORRELATION SWAP 4', 'DUAL CURRENCY', 'MULTICCY SPEC')) GROUP BY cob_date, book, currency_of_measure, CCC_STRATEGY, CCC_PRODUCT_LINE, vertical_system, ccc_business_area, a.ccc_division