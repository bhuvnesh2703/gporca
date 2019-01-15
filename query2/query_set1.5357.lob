SELECT
    a.COB_DATE,
    CCC_PL_REPORTING_REGION,
    CASE WHEN SPG_DESC in ('CORPORATE CDO DEFAULT SWAP') THEN 'Synthetic' 
    ELSE 'CASH' END AS CASH_SYNTHETIC,
    CASE WHEN INSURER_RATING in ('AAA', 'AM', 'AS') then 'AAA'
    when INSURER_RATING in ('AJ', 'AA') then 'AA'
    when INSURER_RATING in ('A') then 'A'
    when INSURER_RATING in ('BBB', 'BBB-') then 'BBB'
    else 'BB' end AS GROUPED_INSURER_RATING,
    CASE WHEN VINTAGE in ('10-1','10-2','10-3','10-4','11-1','11-2','11-3','11-4','12-1','12-2','12-3','12-4') then '10-12'
    when VINTAGE in ('13-1','13-2','13-3','13-4') then '13'
    when VINTAGE in ('14-1','14-2','14-3','14-4') then '14'
    when VINTAGE in ('15-1','15-2','15-3','15-4') then '15'
    when VINTAGE in ('16-1','16-2','16-3','16-4') then '16'
    when VINTAGE in ('17-1','17-2','17-3','17-4') then '17'
    else 'pre10' end AS GROUPED_YEARLY_VINTAGE,
    GROUPED_TRADING_LENDING,
    SUM (a.USD_EXPOSURE) AS NET_EXPOSURE,
    SUM (a.USD_PV01SPRD) AS SPV01
FROM cdwuser.U_DM_SPG a
WHERE
    a.COB_DATE IN 
    ('2018-02-28', '2018-02-27')
    AND a.CCC_BUSINESS_AREA = 'SECURITIZED PRODUCTS GRP'    
    AND GROUPED_TRADING_LENDING = 'CLO'
GROUP BY
    a.COB_DATE,
    CCC_PL_REPORTING_REGION,
    CASE WHEN SPG_DESC in ('CORPORATE CDO DEFAULT SWAP') THEN 'Synthetic' 
    ELSE 'CASH' END,
    CASE WHEN INSURER_RATING in ('AAA', 'AM', 'AS') then 'AAA'
    when INSURER_RATING in ('AJ', 'AA') then 'AA'
    when INSURER_RATING in ('A') then 'A'
    when INSURER_RATING in ('BBB', 'BBB-') then 'BBB'
    else 'BB' end,
    CASE WHEN VINTAGE in ('10-1','10-2','10-3','10-4','11-1','11-2','11-3','11-4','12-1','12-2','12-3','12-4') then '10-12'
    when VINTAGE in ('13-1','13-2','13-3','13-4') then '13'
    when VINTAGE in ('14-1','14-2','14-3','14-4') then '14'
    when VINTAGE in ('15-1','15-2','15-3','15-4') then '15'
    when VINTAGE in ('16-1','16-2','16-3','16-4') then '16'
    when VINTAGE in ('17-1','17-2','17-3','17-4') then '17'
    else 'pre10' end,
    GROUPED_TRADING_LENDING