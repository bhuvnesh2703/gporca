SELECT
    A.COB_DATE,
    CASE WHEN A.CCC_PRODUCT_LINE IN ('INV GRADE TRADING - EU', 'HIGH YIELD - EU') THEN 'EUROPEAN CREDIT FLOW' ELSE A.CCC_PRODUCT_LINE END AS CCC_PRODUCT_LINE,
    A.CCC_STRATEGY,
    A.PNL_LEVEL_1,
    A.PNL_LEVEL_2,
    A.PRODUCT_TYPE,
    A.BOOK, A.CCC_TRADE_STRATEGY, 
    CASE WHEN A.PRODUCT_TYPE IN ('BANKDEBT') THEN 'LOAN' WHEN PRODUCT_TYPE IN ('BOND', 'FRN', 'PREF', 'TRD_CLAIM', 'GVTBOND', 'CLNCLN', 
                                                                                   'BONDFUT', 'TRS - BOND', 'TRS - GVTBOND', 'BOND ETF', 
                                                                                   'BONDIL', 'CONVRT', 'CLNBOND') THEN 'BOND' WHEN 
        PRODUCT_TYPE IN ('EQUITY', 'STOCK', 'WARRNT', 'ASCOT', 'ADR', 'FUND') THEN 'EQUITY' WHEN PRODUCT_TYPE IN ('CANCDFS', 'DEFSWAP', 
                                                                                                                      'FEE', 'LOANCDS', 
                                                                                                                      'TRRSWAP', 
                                                                                                                      'MUNICDS', 
                                                                                                                      'CLNDEFSWAP') THEN 
        'CDS' WHEN PRODUCT_TYPE IN ('CRDINDEX', 'LOANINDEX', 'MUNICDX', 'CDSOPTIDX') THEN 'INDEX'
    ELSE 'OTHER' END AS PRODUCT_TYPE_GROUP,
    SUM (A.DAILY_PNL) AS DAILY_PNL
FROM DWUSER.U_RISK_PNL A
WHERE
    (A.COB_DATE <= '2018-02-28' AND A.COB_DATE >='2018-02-01') AND
    A.CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' AND
    A.ACCOUNT_PURPOSE <> 'J' AND 
    A.PNL_GROUP IN ('ATTRIBUTION', 'OTHER')
GROUP BY
    A.COB_DATE,
    CASE WHEN A.CCC_PRODUCT_LINE IN ('INV GRADE TRADING - EU', 'HIGH YIELD - EU') THEN 'EUROPEAN CREDIT FLOW' ELSE A.CCC_PRODUCT_LINE END,
    A.CCC_STRATEGY,
    A.PNL_LEVEL_1,
    A.PNL_LEVEL_2,
    A.PRODUCT_TYPE,
    A.BOOK, A.CCC_TRADE_STRATEGY, 
    CASE WHEN PRODUCT_TYPE IN ('BANKDEBT') THEN 'LOAN' WHEN PRODUCT_TYPE IN ('BOND', 'FRN', 'PREF', 'TRD_CLAIM', 'GVTBOND', 'CLNCLN', 
                                                                                 'BONDFUT', 'TRS - BOND', 'TRS - GVTBOND', 'BOND ETF', 
                                                                                 'BONDIL', 'CONVRT', 'CLNBOND') THEN 'BOND' WHEN 
        PRODUCT_TYPE IN ('EQUITY', 'STOCK', 'WARRNT', 'ASCOT', 'ADR', 'FUND') THEN 'EQUITY' WHEN PRODUCT_TYPE IN ('CANCDFS', 'DEFSWAP', 
                                                                                                                      'FEE', 'LOANCDS', 
                                                                                                                      'TRRSWAP', 
                                                                                                                      'MUNICDS', 
                                                                                                                      'CLNDEFSWAP') THEN 
        'CDS' WHEN PRODUCT_TYPE IN ('CRDINDEX', 'LOANINDEX', 'MUNICDX', 'CDSOPTIDX') THEN 'INDEX'
    ELSE 'OTHER' END