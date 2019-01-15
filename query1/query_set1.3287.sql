select COB_DATE,CCC_BUSINESS_AREA, CCC_BANKING_TRADING, spg_desc, ccc_pl_reporting_region,ccc_product_line,CCC_strategy, insurer_rating, vintage, account,  SUM(abs(USD_EXPOSURE)) AS GROSS_EXPO, tapscusip,
 (case when spg_desc in ('RMBS NON CONFORMING DEFAULT SWAP', 'RMBS SD DEFAULT SWAP', 'RMBS SD LOAN', 'RMBS SD PREPAYMENT PENALTY', 'RMBS SD RESIDUAL', 'RMBS SD SECURITY', 'RMBS SECOND DEFAULT SWAP', 'RMBS SECOND LOAN', 'RMBS SECOND RESIDUAL', 'RMBS SECOND SECURITY', 'RMBS SD DEFAULT SWAP', 'RMBS SD SECURITY ', 'RMBS ALTA DEFAULT SWAP', 'RMBS ALTA IO',  'RMBS ALTA PREPAYMENT PENALTY', 'RMBS ALTA RESIDUAL', 'RMBS ALTA SECURITY', 'RMBS OPTION ARM SECURITY', 'RMBS CDO', 'RMBS CDO EQUITY', 
 'RMBS CDO PREFERRED', 'RMBS NIMS', 'RMBS POST NIM', 'RMBS SUB PRIME FIRST PAY', 'RMBS SUB PRIME IO', 'RMBS SUB PRIME LOAN', 'RMBS SUB PRIME RESIDUAL', 'RMBS SUB PRIME SECOND PAY', 'RMBS SUB PRIME SECURITY', 'RMBS SUPER SENIOR', 'RMBS SUPER SENIOR TRR', 'WAREHOUSE SUB PRIME RMBS LOAN', 'RMBS ABSPOKE', 'RMBS CDO DEFAULT SWAP',  'RMBS DEFAULT SWAP', 'RMBS INDEX TRANCHE', 'RMBS SUB PRIME INDEX',  'RMBS ALTA REREMIC', 'RMBS SUB PRIME REREMIC','RPX', 'RMBS HELOC SECURITY' , 'RMBS PRIME DEFAULT SWAP','RMBS PRIME IO', 'RMBS PRIME LOAN', 'RMBS PRIME RESIDUAL', 'RMBS PRIME SECURITY', 'RMBS PRIME INDEX', 'RMBS PRIME REREMIC') then 'Residential' 
 when spg_desc like ('ABS%') and spg_desc not in ('ABS DEFAULT SWAP', 'ABS FRANCHISE SECURITY') then 'Other ABS' when spg_desc in ('CMBS LOAN', 'CMBS MEZZANINE LOAN')   then 'CRE Loans' when spg_desc like('CMBS%') and spg_desc not  in ('CMBS LOAN', 'CMBS MEZZANINE LOAN')  then 'CMBS Ex. CRE Loans'  when spg_desc in ('CORPORATE CLO', 'CORPORATE CLO TRUPS', 'CORPORATE CDO', 'CORPORATE CDO DEFAULT SWAP', 'CORPORATE CDO EQUITY', 'CORPORATE CDO PREFERRED', 'CORPORATE AND CDS') then 'CLOs' when spg_desc in ('CORPORATE BONDS', 'CORPORATE DEFAULT SWAP', 'CORPORATE INDEX') then 'Corporate Hedges' end) as Core_Trading_Classification,
 (CASE WHEN account like ('07%') then 'cash' when account like ('08%') then 'Synthetic' else 'others' end) as  Cash_Synthetic,
 (case when spg_desc in ('RMBS PRIME IO', 'RMBS PRIME LOAN', 'RMBS PRIME RESIDUAL', 'RMBS PRIME SECURITY',  'RMBS PRIME REREMIC') and ccc_pl_reporting_region in ('AMERICAS') then 'Prime' else 'Ex_Prime' end)as Resi_Core_Trading_Classification,
 (case when account like ('08%') and  ccc_pl_reporting_region in ('AMERICAS') and spg_desc in ('CMBS CDO', 'CMBS CREDIT BASKET','RMBS CDO','RMBS CDO PREFERRED') THEN 'BBB' WHEN insurer_rating in ('PENAAA','AAA','AM','AJ') THEN 'AAA' else insurer_rating end)as adjusted_rating, 
 CASE WHEN ccc_business_area = 'SECURITIZED PRODUCTS GRP' and ccc_product_line <> 'WAREHOUSE' and account NOT IN ('07200BKA5',' 0730011W9','07200BKB3','07200CC46','07200CC53') then 'TRADING' else CCC_BANKING_TRADING end as capital_book_patched 
 From DWUSER.U_CR_MSR
 where(CCC_product_line in ('CREDIT-SECURITIZED PRODS','SECURITIZED PRODUCTS GRP') or ccc_business_area in ('CREDIT-SECURITIZED PRODS','SECURITIZED PRODUCTS GRP')) 
 AND COB_DATE IN ('2018-02-28', '2018-01-31', '2017-12-29', '2017-09-29', '2017-06-30', '2017-03-31') 
 GROUP by COB_DATE, spg_desc, ccc_pl_reporting_region, ccc_product_line,CCC_BUSINESS_AREA, CCC_BANKING_TRADING,CCC_strategy,insurer_rating, vintage, account, tapscusip