SELECT
    A.COB_DATE,
    CASE WHEN A.CCC_PRODUCT_LINE IN ('INV GRADE TRADING - EU', 'HIGH YIELD - EU') THEN 'EUROPEAN CREDIT FLOW' ELSE A.CCC_PRODUCT_LINE END AS CCC_PRODUCT_LINE,
    A.CCC_STRATEGY,
    A.PNL_LEVEL_1,
    A.PNL_LEVEL_2, A.PNL_LEVEL_4,
    A.PRODUCT_TYPE,
    A.BOOK, A.CCC_TRADE_STRATEGY, 
    SUM (A.DAILY_PNL) AS DAILY_PNL
FROM DWUSER.U_RISK_PNL A
WHERE
    (A.COB_DATE <= '2018-02-28' AND A.COB_DATE >='2018-02-01') AND
    A.CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' AND
    A.ACCOUNT_PURPOSE <> 'J' AND 
    A.PNL_GROUP IN ('ATTRIBUTION', 'OTHER') AND
    A.PNL_LEVEL_1 = 'ATTRIBUTION' AND
    A.PNL_LEVEL_2 = 'MARKET MOVEMENT' AND
    A.PNL_LEVEL_3 <> 'OTHER MM'
GROUP BY
    A.COB_DATE,
    CASE WHEN A.CCC_PRODUCT_LINE IN ('INV GRADE TRADING - EU', 'HIGH YIELD - EU') THEN 'EUROPEAN CREDIT FLOW' ELSE A.CCC_PRODUCT_LINE END,
    A.CCC_STRATEGY,
    A.PNL_LEVEL_1,
    A.PNL_LEVEL_2, A.PNL_LEVEL_4,
    A.PRODUCT_TYPE,
    A.BOOK, A.CCC_TRADE_STRATEGY