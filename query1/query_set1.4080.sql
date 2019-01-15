SELECT
    A.COB_DATE,
    CASE WHEN A.PAYOFF_MODEL = 'BONDSETCOCO' THEN 'COCO' ELSE A.FID1_SENIORITY END AS SENIORITY_CATEGORY,
    SUM (a.USD_NET_EXPOSURE) AS netexp
FROM cdwuser.U_DM_CC A
WHERE
    a.COB_DATE IN (
'2017-10-31',
'2017-11-30',
'2017-12-29',
'2018-01-31',
'2018-02-28')
 AND
    A.CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' AND
    A.CREDIT_TRADING_FLAG = 'Junior Subs'
GROUP BY
    A.COB_DATE, 
    CASE WHEN A.PAYOFF_MODEL = 'BONDSETCOCO' THEN 'COCO' ELSE A.FID1_SENIORITY END