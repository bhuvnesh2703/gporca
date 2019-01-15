SELECT
    a.COB_DATE,
CASE 
 when SPG_PRODUCT_TYPE IN ('RESI- NON AGENCY CRT') then 'RMBS'
 when spg_desc in ('WAREHOUSE CLO') then 'Warehouse - FV'
 when spg_desc like 'WAREHOUSE%' then 'Warehouse - HFI' 
 when SPG_DESC ='OTHER RMBS' or SPG_DESC like '%AGENCY%' or spg_desc like '%POOL%' or spg_desc like '%TBA%' or spg_desc like '%RMBS IOS%' or spg_desc like '%RMBS MBX%' or spg_desc like '%RMBS MBS%' then 'Agency' 
 when spg_desc like '%CMBS%' and spg_desc not in ('CMBS LOAN', 'CMBS LOAN IO') then 'CMBS' 
 when spg_desc like '%RMBS%' and spg_desc not like '%AGENCY%' and CCC_PRODUCT_LINE not in ('WAREHOUSE', 'AGENCY MORTGAGE TRADING') then 'RMBS' 
 when ccc_product_line in ('CRE LEND SECURITIZATION', 'CRE LEND - BANK HFI/HFS', 'CRE LENDING','CRE LENDING SEC/HFS','CREL BANK HFI') and book like '%HFI' and spg_desc in ('CMBS LOAN', 'CMBS LOAN IO') then 'CRE Loan - HFI' 
 when ccc_product_line in ('CRE LEND SECURITIZATION', 'CRE LEND - BANK HFI/HFS', 'CRE LENDING','CRE LENDING SEC/HFS','CREL BANK HFI') and book in ('CRE_LENDING_EU', 'CRE_LENDING') and spg_desc in ('CMBS LOAN', 'CMBS LOAN IO') then 'CRE Loan - FV' 
 when ccc_product_line in ('CRE LEND SECURITIZATION', 'CRE LEND - BANK HFI/HFS', 'CRE LENDING','CRE LENDING SEC/HFS','CREL BANK HFI') and book in ('CRE_LENDING_EU_HFS', 'CRE_LENDING_HFS') and spg_desc in ('CMBS LOAN', 'CMBS LOAN IO') then 'CRE Loan - HFS' 
 when spg_desc in ('ABS STUDENT SECURITY') then 'ABS Agency' 
 when spg_desc like 'ABS%' then 'ABS' 
 when spg_desc like '%CORPORATE CDO%' or spg_desc like '%CORPORATE CLO%' then 'CLO' 
 when spg_desc like '%CORPORATE INDEX%' or spg_desc like '%CORPORATE DEFAULT SWAP%' then 'Corporate Hedge' 
 when spg_desc like '%GOVERNMENT%' or spg_desc like '%RATE%' or spg_desc in ('SWAP','SWAPS') or PRODUCT_TYPE_CODE in ('BERMUDAN_SWAPTION_VA','BONDFUTOPT','MISC') then 'Rate Hedge' 
 WHEN SPG_DESC = 'OTHER' AND PRODUCT_TYPE_CODE = 'AGN' THEN 'Agency Debentures'
Else 'OTHER'  end as GROUPED_TRADING_LENDING,
    SUM (a.USD_EXPOSURE) AS NET_EXPOSURE
FROM cdwuser.U_DM_SPG a
WHERE
    a.COB_DATE IN 
    ('2018-02-28', '2018-02-21', '2018-01-31', '2017-12-29', '2017-09-29')
    AND a.CCC_BUSINESS_AREA = 'SECURITIZED PRODUCTS GRP'
    AND NOT a.CCC_PRODUCT_LINE IN ('CRE LENDING SEC/HFS', 'CREL BANK HFI', 'CRE LENDING', 'WAREHOUSE')
    AND NOT (a.SPG_DESC = 'CMBS INDEX' AND
         a.CCC_PRODUCT_LINE IN ('CRE LENDING SEC/HFS', 'CREL BANK HFI', 'CRE LENDING', 'SPG MANAGEMENT'))
GROUP BY
    a.COB_DATE,
CASE 
 when SPG_PRODUCT_TYPE IN ('RESI- NON AGENCY CRT') then 'RMBS'
 when spg_desc in ('WAREHOUSE CLO') then 'Warehouse - FV'
 when spg_desc like 'WAREHOUSE%' then 'Warehouse - HFI' 
 when SPG_DESC ='OTHER RMBS' or SPG_DESC like '%AGENCY%' or spg_desc like '%POOL%' or spg_desc like '%TBA%' or spg_desc like '%RMBS IOS%' or spg_desc like '%RMBS MBX%' or spg_desc like '%RMBS MBS%' then 'Agency' 
 when spg_desc like '%CMBS%' and spg_desc not in ('CMBS LOAN', 'CMBS LOAN IO') then 'CMBS' 
 when spg_desc like '%RMBS%' and spg_desc not like '%AGENCY%' and CCC_PRODUCT_LINE not in ('WAREHOUSE', 'AGENCY MORTGAGE TRADING') then 'RMBS' 
 when ccc_product_line in ('CRE LEND SECURITIZATION', 'CRE LEND - BANK HFI/HFS', 'CRE LENDING','CRE LENDING SEC/HFS','CREL BANK HFI') and book like '%HFI' and spg_desc in ('CMBS LOAN', 'CMBS LOAN IO') then 'CRE Loan - HFI' 
 when ccc_product_line in ('CRE LEND SECURITIZATION', 'CRE LEND - BANK HFI/HFS', 'CRE LENDING','CRE LENDING SEC/HFS','CREL BANK HFI') and book in ('CRE_LENDING_EU', 'CRE_LENDING') and spg_desc in ('CMBS LOAN', 'CMBS LOAN IO') then 'CRE Loan - FV' 
 when ccc_product_line in ('CRE LEND SECURITIZATION', 'CRE LEND - BANK HFI/HFS', 'CRE LENDING','CRE LENDING SEC/HFS','CREL BANK HFI') and book in ('CRE_LENDING_EU_HFS', 'CRE_LENDING_HFS') and spg_desc in ('CMBS LOAN', 'CMBS LOAN IO') then 'CRE Loan - HFS' 
 when spg_desc in ('ABS STUDENT SECURITY') then 'ABS Agency' 
 when spg_desc like 'ABS%' then 'ABS' 
 when spg_desc like '%CORPORATE CDO%' or spg_desc like '%CORPORATE CLO%' then 'CLO' 
 when spg_desc like '%CORPORATE INDEX%' or spg_desc like '%CORPORATE DEFAULT SWAP%' then 'Corporate Hedge' 
 when spg_desc like '%GOVERNMENT%' or spg_desc like '%RATE%' or spg_desc in ('SWAP','SWAPS') or PRODUCT_TYPE_CODE in ('BERMUDAN_SWAPTION_VA','BONDFUTOPT','MISC') then 'Rate Hedge' 
 WHEN SPG_DESC = 'OTHER' AND PRODUCT_TYPE_CODE = 'AGN' THEN 'Agency Debentures'
Else 'OTHER'  end