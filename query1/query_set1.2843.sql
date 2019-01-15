select cob_date,CASE
WHEN MRD_RATING IN ('AAA', 'AA', 'A', 'BBB') THEN 'IG'
ELSE 'NIG' END AS RATING2, case when ccc_division in ('INSTITUTIONAL EQUITY DIVISION') then 'IED' when (ccc_division in ('FIXED INCOME DIVISION') and ccc_business_area in ('FXEM MACRO TRADING','EM CREDIT TRADING')) then 'FXEM' when (ccc_division in ('FIXED INCOME DIVISION') and ccc_business_area in ('LIQUID FLOW RATES','STRUCTURED RATES')) then 'IR' when (ccc_division in ('FIXED INCOME DIVISION') and ccc_business_area in ('CREDIT-CORPORATES','SECURITIZED PRODUCTS GRP')) then 'CC-SPG' when (ccc_division in ('FIXED INCOME DIVISION') and ccc_business_area in ('DSP - CREDIT')) then 'DSP Cr' when ccc_division in ('FIXED INCOME DIVISION') then 'Others' else 'Other' end as hierarchy, case when product_type_code in ('BOND','GVTBOND','GVTBONDIL','FRN','BANKDEBT','AGN','CD', 'BONDIL') then 'Cash' when product_type_code like 'DEFSWAP' then 'CDS' when product_type_code like '%INDEX%' then 'Index Outright' when product_type_code like '%OPTIDX%' then 'Index Options' when product_type_code in ('BONDFUT','BONDFUTOPT','GOVTBONDOPT','SPRDOPT', 'TRRSWAP', 'ZCS') then 'Derivatives' when product_type_code like '%TRS%' then 'Derivatives' else 'N/A' end as product, ISSUER_COUNTRY_CODE, sum(usd_pv10_bench) as pv10, sum(usd_net_exposure)/1000 as netexp from cdwuser.U_DM_CC a where a.COB_DATE in ('2018-02-28','2018-02-27') and ((a.USD_NET_EXPOSURE <> 0 and a.USD_NET_EXPOSURE is not null) OR (a.USD_PV10_BENCH <> 0 and a.USD_PV10_BENCH is not null)) AND a.CCC_TAPS_COMPANY ='0302' AND a.CCC_BUSINESS_AREA NOT IN ('NON CORE','CPM','LENDING') AND a.CCC_BANKING_TRADING = 'TRADING' AND (a.CCC_PRODUCT_LINE NOT IN ('DISTRESSED TRADING') OR a.PRODUCT_TYPE_CODE IN ('CDSOPTIDX','CRDINDEX','LOANINDEX')) AND ((a.FID1_SENIORITY NOT IN ('AT1','SUBT1','SUBUT2') OR a.ISSUER_COUNTRY_CODE NOT IN ('DNK','BMU','CYM','AUS','AUT','BEL','CAN','CHE','DEU','ESP','FIN','FRA','GBR','GRC','IRL','ITA','JPN','LUX','NLD','NOR','NZL','PRT','SWE','USA','JEY','GGY','SUP','XS','XCI','VGB','CYP')) OR a.PRODUCT_TYPE_CODE NOT IN ('BOND','BONDFUT','BONDFUTOPT','BONDIL','BONDOPT','FRN','PREF')) AND a.VERTICAL_SYSTEM NOT LIKE '%SPG%' group by cob_date, case when ccc_division in ('INSTITUTIONAL EQUITY DIVISION') then 'IED' when (ccc_division in ('FIXED INCOME DIVISION') and ccc_business_area in ('FXEM MACRO TRADING','EM CREDIT TRADING')) then 'FXEM' when (ccc_division in ('FIXED INCOME DIVISION') and ccc_business_area in ('LIQUID FLOW RATES','STRUCTURED RATES')) then 'IR' when (ccc_division in ('FIXED INCOME DIVISION') and ccc_business_area in ('CREDIT-CORPORATES','SECURITIZED PRODUCTS GRP')) then 'CC-SPG' when (ccc_division in ('FIXED INCOME DIVISION') and ccc_business_area in ('DSP - CREDIT')) then 'DSP Cr' when ccc_division in ('FIXED INCOME DIVISION') then 'Others' else 'Other' end, case when product_type_code in ('BOND','GVTBOND','GVTBONDIL','FRN','BANKDEBT','AGN','CD', 'BONDIL') then 'Cash' when product_type_code like 'DEFSWAP' then 'CDS' when product_type_code like '%INDEX%' then 'Index Outright' when product_type_code like '%OPTIDX%' then 'Index Options' when product_type_code in ('BONDFUT','BONDFUTOPT','GOVTBONDOPT','SPRDOPT', 'TRRSWAP', 'ZCS') then 'Derivatives' when product_type_code like '%TRS%' then 'Derivatives' else 'N/A' end, ISSUER_COUNTRY_CODE,CASE
WHEN MRD_RATING IN ('AAA', 'AA', 'A', 'BBB') THEN 'IG'
ELSE 'NIG' END