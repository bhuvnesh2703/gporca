select
    cob_date,
    CCC_PL_REPORTING_REGION,
    ccc_product_line,
    spg_desc,
    INSURER_RATING,    
    
    Case 
    WHEN INSURER_RATING in ('AAA','AM','AS') THEN 'Senior'
    Else 'Mezz'
    End as Seniority,
    
    Case
    WHEN SPG_DESC in ('CMBS DEFAULT SWAP', 'CMBS INDEX') 
    THEN (sum(coalesce(a.SLIDE_CMBX_MIN_05PCT_USD,0)) + sum(coalesce(a.SLIDE_SPGCC_MIN_5PCT_USD,0)))
    ELSE sum(a.SLIDE_CMBX_MIN_05PCT_USD)
    END as CMBX_MIN_05,
    
    Case
    WHEN SPG_DESC in ('CMBS DEFAULT SWAP', 'CMBS INDEX') 
    THEN (sum(coalesce(a.SLIDE_CMBX_MIN_10PCT_USD,0)) + sum(coalesce(a.SLIDE_SPGCC_MIN_10PCT_USD,0)))
    ELSE sum(a.SLIDE_CMBX_MIN_10PCT_USD)
    END as CMBX_MIN_10,

    Case
    WHEN SPG_DESC in ('CMBS DEFAULT SWAP', 'CMBS INDEX') 
    THEN (sum(coalesce(a.SLIDE_CMBX_MIN_20PCT_USD,0)) + sum(coalesce(a.SLIDE_SPGCC_MIN_20PCT_USD,0)))
    ELSE sum(a.SLIDE_CMBX_MIN_20PCT_USD) 
    END as CMBX_MIN_20, 
    
    Case
    WHEN SPG_DESC in ('CMBS DEFAULT SWAP', 'CMBS INDEX') 
    THEN (sum(coalesce(a.SLIDE_CMBX_MIN_30PCT_USD,0)) + sum(coalesce(a.SLIDE_SPGCC_MIN_30PCT_USD,0)))
    ELSE sum(a.SLIDE_CMBX_MIN_30PCT_USD)
    END as CMBX_MIN_30,  

    Case
    WHEN SPG_DESC in ('CMBS DEFAULT SWAP', 'CMBS INDEX') 
    THEN (sum(coalesce(a.SLIDE_CMBX_PLS_05PCT_USD,0)) + sum(coalesce(a.SLIDE_SPGCC_PLS_5PCT_USD,0)))
    ELSE sum(a.SLIDE_CMBX_PLS_05PCT_USD)
    END as CMBX_PLS_05, 
    
    Case
    WHEN SPG_DESC in ('CMBS DEFAULT SWAP', 'CMBS INDEX') 
    THEN (sum(coalesce(a.SLIDE_CMBX_PLS_10PCT_USD,0)) + sum(coalesce(a.SLIDE_SPGCC_PLS_10PCT_USD,0)))
    ELSE sum(a.SLIDE_CMBX_PLS_10PCT_USD)
    END as CMBX_PLS_10, 
    
    Case
    WHEN SPG_DESC in ('CMBS DEFAULT SWAP', 'CMBS INDEX') 
    THEN (sum(coalesce(a.SLIDE_CMBX_PLS_20PCT_USD,0)) + sum(coalesce(a.SLIDE_SPGCC_PLS_20PCT_USD,0)))
    ELSE sum(a.SLIDE_CMBX_PLS_20PCT_USD)
    END as CMBX_PLS_20, 
    
    Case
    WHEN SPG_DESC in ('CMBS DEFAULT SWAP', 'CMBS INDEX') 
    THEN (sum(coalesce(a.SLIDE_CMBX_PLS_30PCT_USD,0)) + sum(coalesce(a.SLIDE_SPGCC_PLS_30PCT_USD,0)))
    ELSE sum(a.SLIDE_CMBX_PLS_30PCT_USD)
    END as CMBX_PLS_30,
    
    CASE WHEN SPG_DESC in ('CMBS CDO','CMBS CREDIT BASKET') then 'CDO'
    when SPG_DESC in ('CMBS IO') then 'IO'
    when SPG_DESC in ('CMBS IO REREMIC', 'CMBS SECURITY REREMIC') then 'Reremics'
    when SPG_DESC in ('CMBS MEZZANINE LOAN') then 'Mezz Loan'
    when SPG_DESC in ('CMBS MEZZANINE SECURITY', 'CMBS SECURITY') and ACCOUNT = '072005JZ5' then 'SASB'    
    when SPG_DESC in ('CMBS MEZZANINE SECURITY', 'CMBS SECURITY') then 'Conduit'    
    when SPG_DESC in ('CMBS DEFAULT SWAP', 'CMBS INDEX') then 'Synthetic' 
    else 'OTHERS'
    end AS GROUPED_CMBS,
    
    CASE WHEN VINTAGE in ('10-1','10-2','10-3','10-4','11-1','11-2','11-3','11-4','12-1','12-2','12-3','12-4') then '2012'
    when VINTAGE in ('13-1','13-2','13-3','13-4') then '2013'
    when VINTAGE in ('14-1','14-2','14-3','14-4') then '2014'
    when VINTAGE in ('15-1','15-2','15-3','15-4') then '2015'
    when VINTAGE in ('16-1','16-2','16-3','16-4') then '2016'
    else 'pre2010' 
    end AS Vintage    
    
    FROM cdwuser.U_SP_MSR a
    WHERE
    a.COB_DATE IN 
    ('2018-02-28', '2018-01-31')
    AND a.CCC_BUSINESS_AREA = 'SECURITIZED PRODUCTS GRP'    
    AND NOT a.CCC_PRODUCT_LINE IN ('CRE LENDING SEC/HFS', 'CREL BANK HFI', 'CRE LENDING', 'WAREHOUSE')
    AND NOT (a.SPG_DESC = 'CMBS INDEX' AND a.CCC_PRODUCT_LINE IN ('CRE LENDING SEC/HFS', 'CREL BANK HFI', 'CRE LENDING', 'SPG MANAGEMENT'))
    AND CCC_PL_REPORTING_REGION = 'AMERICAS'
    AND spg_desc like '%CMBS%' 
    AND spg_desc not in ('CMBS LOAN', 'CMBS LOAN IO')
    
    group by
    cob_date,
    CCC_PL_REPORTING_REGION,
    ccc_product_line,
    spg_desc,
    INSURER_RATING,
    
    Case 
    WHEN INSURER_RATING in ('AAA','AM','AS') THEN 'Senior'
    Else 'Mezz'
    End,
    
    CASE WHEN SPG_DESC in ('CMBS CDO','CMBS CREDIT BASKET') then 'CDO'
    when SPG_DESC in ('CMBS IO') then 'IO'
    when SPG_DESC in ('CMBS IO REREMIC', 'CMBS SECURITY REREMIC') then 'Reremics'
    when SPG_DESC in ('CMBS MEZZANINE LOAN') then 'Mezz Loan'
    when SPG_DESC in ('CMBS MEZZANINE SECURITY', 'CMBS SECURITY') and ACCOUNT = '072005JZ5' then 'SASB'    
    when SPG_DESC in ('CMBS MEZZANINE SECURITY', 'CMBS SECURITY') then 'Conduit'    
    when SPG_DESC in ('CMBS DEFAULT SWAP', 'CMBS INDEX') then 'Synthetic' 
    else 'OTHERS'
    end,
    
    CASE WHEN VINTAGE in ('10-1','10-2','10-3','10-4','11-1','11-2','11-3','11-4','12-1','12-2','12-3','12-4') then '2012'
    when VINTAGE in ('13-1','13-2','13-3','13-4') then '2013'
    when VINTAGE in ('14-1','14-2','14-3','14-4') then '2014'
    when VINTAGE in ('15-1','15-2','15-3','15-4') then '2015'
    when VINTAGE in ('16-1','16-2','16-3','16-4') then '2016'
    else 'pre2010' 
    end