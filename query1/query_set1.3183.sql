SELECT
    a.COB_DATE,
    spg_desc,
    CASE WHEN INSURER_RATING in ('AAA','AM','AS') then 'AAA/AM/AS'
    when INSURER_RATING in ('AJ') then 'AJ'
    when INSURER_RATING in ('AA') then 'AA'
    when INSURER_RATING in ('A') then 'A'
    when INSURER_RATING in ('BBB', 'BBB-') then 'BBB'
    else 'BB' end AS GROUPED_INSURER_RATING,
    CASE WHEN SPG_DESC in ('CMBS CDO','CMBS CREDIT BASKET') then 'CDO'
    when SPG_DESC in ('CMBS IO') then 'IO'
    when SPG_DESC in ('CMBS IO REREMIC', 'CMBS SECURITY REREMIC') then 'Reremics'
    when SPG_DESC in ('CMBS MEZZANINE LOAN') then 'Mezz Loan'
    when SPG_DESC in ('CMBS MEZZANINE SECURITY', 'CMBS SECURITY') and ACCOUNT = '072005JZ5' then 'SASB'    
    when SPG_DESC in ('CMBS MEZZANINE SECURITY', 'CMBS SECURITY') then 'Conduit'    
    when SPG_DESC in ('CMBS DEFAULT SWAP') then 'CDS'
    when SPG_DESC in ('CMBS INDEX') then 'Index' end AS GROUPED_CMBS,
    CASE when VINTAGE in ('06-1','06-2','06-3','06-4') then '06'
    when VINTAGE in ('07-1','07-2','07-3','07-4') then '07'
    when VINTAGE in ('08-1','08-2','08-3','08-4') then '08'
    when VINTAGE in ('09-1','09-2','09-3','09-4') then '09'
    when VINTAGE in ('10-1','10-2','10-3','10-4','11-1','11-2','11-3','11-4','12-1','12-2','12-3','12-4') then '10-12'
    when VINTAGE in ('13-1','13-2','13-3','13-4') then '13'
    when VINTAGE in ('14-1','14-2','14-3','14-4') then '14'
    when VINTAGE in ('15-1','15-2','15-3','15-4') then '15'
    when VINTAGE in ('16-1','16-2','16-3','16-4') then '16'
    when VINTAGE in ('17-1','17-2','17-3','17-4') then '17'
    else 'pre06' end AS GROUPED_YEARLY_VINTAGE,
    GROUPED_TRADING_LENDING,
    SUM(a.USD_NOTIONAL) AS NOTIONAL,
    SUM (a.USD_EXPOSURE) AS NET_EXPOSURE,
    SUM (a.USD_PV01SPRD) AS SPV01
FROM cdwuser.U_DM_SPG a
WHERE
    a.COB_DATE IN 
    ('2018-02-28', '2018-02-27')
    AND a.CCC_BUSINESS_AREA = 'SECURITIZED PRODUCTS GRP'    
    AND NOT a.CCC_PRODUCT_LINE IN ('CRE LENDING SEC/HFS', 'CREL BANK HFI', 'CRE LENDING', 'WAREHOUSE')
    AND NOT (a.SPG_DESC = 'CMBS INDEX' AND a.CCC_PRODUCT_LINE IN ('CRE LENDING SEC/HFS', 'CREL BANK HFI', 'CRE LENDING', 'SPG MANAGEMENT'))
    AND CCC_PL_REPORTING_REGION = 'AMERICAS'
    AND GROUPED_TRADING_LENDING = 'CMBS'
GROUP BY
    a.COB_DATE,
    spg_desc,
    CASE WHEN INSURER_RATING in ('AAA','AM','AS') then 'AAA/AM/AS'
    when INSURER_RATING in ('AJ') then 'AJ'
    when INSURER_RATING in ('AA') then 'AA'
    when INSURER_RATING in ('A') then 'A'
    when INSURER_RATING in ('BBB', 'BBB-') then 'BBB'
    else 'BB' end,
    CASE WHEN SPG_DESC in ('CMBS CDO','CMBS CREDIT BASKET') then 'CDO'
    when SPG_DESC in ('CMBS IO') then 'IO'
    when SPG_DESC in ('CMBS IO REREMIC', 'CMBS SECURITY REREMIC') then 'Reremics'
    when SPG_DESC in ('CMBS MEZZANINE LOAN') then 'Mezz Loan'
    when SPG_DESC in ('CMBS MEZZANINE SECURITY', 'CMBS SECURITY') and ACCOUNT = '072005JZ5' then 'SASB'    
    when SPG_DESC in ('CMBS MEZZANINE SECURITY', 'CMBS SECURITY') then 'Conduit'    
    when SPG_DESC in ('CMBS DEFAULT SWAP') then 'CDS'
    when SPG_DESC in ('CMBS INDEX') then 'Index' end,
    CASE when VINTAGE in ('06-1','06-2','06-3','06-4') then '06'
    when VINTAGE in ('07-1','07-2','07-3','07-4') then '07'
    when VINTAGE in ('08-1','08-2','08-3','08-4') then '08'
    when VINTAGE in ('09-1','09-2','09-3','09-4') then '09'
    when VINTAGE in ('10-1','10-2','10-3','10-4','11-1','11-2','11-3','11-4','12-1','12-2','12-3','12-4') then '10-12'
    when VINTAGE in ('13-1','13-2','13-3','13-4') then '13'
    when VINTAGE in ('14-1','14-2','14-3','14-4') then '14'
    when VINTAGE in ('15-1','15-2','15-3','15-4') then '15'
    when VINTAGE in ('16-1','16-2','16-3','16-4') then '16'
    when VINTAGE in ('17-1','17-2','17-3','17-4') then '17'
    else 'pre06' end,
    GROUPED_TRADING_LENDING