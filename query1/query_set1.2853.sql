With dates(dates_mrdw, dates_name) as (values ('2018-02-28','COB_ONE'), ('2018-02-27','COB_TWO'), ('2018-01-31','COB_THREE'), ('2017-12-29','COB_FOUR'), ('2017-11-30','COB_FIVE') ), x as ( SELECT a.COB_DATE, a.CCC_DIVISION, a.CCC_BUSINESS_AREA, a.CCC_PRODUCT_LINE, a.PRODUCT_TYPE_CODE, a.BOOK, CASE WHEN (a.CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' AND a.PRODUCT_TYPE_CODE IN ('BOND','GVTBOND','GVTBONDIL','FRN','BANKDEBT','AGN','CD', 'BONDIL') AND a.MRD_RATING IN ('AAA','AA','A','BBB')) THEN 'Credit Flow Cash IG' WHEN (a.CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' AND a.PRODUCT_TYPE_CODE IN ('BOND','GVTBOND','GVTBONDIL','FRN','BANKDEBT','AGN','CD', 'BONDIL') AND a.MRD_RATING NOT IN ('AAA','AA','A','BBB')) THEN 'Credit Flow Cash HY' WHEN (a.CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' AND a.PRODUCT_TYPE_CODE IN ('DEFSWAP')) THEN 'Credit Flow CDS' WHEN (a.CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' AND a.PRODUCT_TYPE_CODE NOT IN ('BOND','GVTBOND','GVTBONDIL','FRN','BANKDEBT','AGN','CD', 'BONDIL','DEFSWAP')) THEN 'Credit Flow Derivatives' WHEN (a.CCC_BUSINESS_AREA = 'DSP - CREDIT') THEN 'DSP Credit' WHEN (a.CCC_BUSINESS_AREA NOT IN ('CREDIT-CORPORATES','DSP - CREDIT')) THEN 'SPG & Others' END AS FLAG_CORPDM, CASE WHEN a.CCC_PRODUCT_LINE IN ('PRIMARY - IG BONDS','PRIMARY - NIG BONDS') THEN 'Primary' WHEN (a.CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' AND a.CCC_PL_REPORTING_REGION = 'EMEA') THEN 'Credit Flow EMEA' WHEN (a.CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' AND a.CCC_PL_REPORTING_REGION = 'ASIA PACIFIC') THEN 'Credit Flow Asia' WHEN a.CCC_BUSINESS_AREA IN ('EM CREDIT TRADING','FXEM MACRO TRADING') THEN 'EM Credit and FX' WHEN a.CCC_BUSINESS_AREA IN ('LIQUID FLOW RATES','STRUCTURED RATES') THEN 'Rates Liquid/Structured' WHEN a.CCC_BUSINESS_AREA IN ('DSP - CREDIT','SECURITIZED PRODUCTS GRP') THEN 'Credit Derivatives & SPG' ELSE 'Other' END AS FLAG_BU, CASE WHEN a.ISSUER_COUNTRY_CODE IN ('XS') THEN 'Supranational' WHEN a.ISSUER_COUNTRY_CODE IN ('ALB','AND','AUT','BLR','BEL','BIH','BGR','CYP','HRV','CZE','DNK','EST','FRO','FIN','FRA','GEO','DEU','GRC','GGY','VAT','HUN','ISL','IRL','IMN','ITA','JEY','LVA','LIE','LTU','LUX','MKD','MLT','MDA','MCO','MNE','NLD','NOR','POL','PRT','ROU','RUS','SMR','SRB','SVK','SVN','ESP','SJM','SWE','CHE','UKR') THEN 'Europe' WHEN a.ISSUER_COUNTRY_CODE IN ('GBR') THEN 'UK' WHEN a.ISSUER_COUNTRY_CODE IN ('USA','CAN') THEN 'NAM' WHEN a.ISSUER_COUNTRY_CODE IN ('AIA','ATG','ARG','ABW','BHS','BRB','BLZ','BMU','BOL','BRA','VGB','CYM','CHL','COL','CRI','CUB','DMA','DOM','ECU','SLV','GUF','GRL','GRD','GLP','GTM','GUY','HTI','HND','JAM','MTQ','MEX','MSR','ANT','NIC','PAN','PRY','PER','PRI','KNA','LCA','SPM','VCT','SGS','SUR','TTO','TCA','URY','VEN','VIR') THEN 'LATAM' WHEN a.ISSUER_COUNTRY_CODE IN ('DZA','AGO','BEN','BWA','BFA','BDI','CMR','CPV','CAF','TCD','COM','COD','CIV','DJI','EGY','GNQ','ERI','ETH','GAB','GMB','GHA','GIB','GIN','GNB','KEN','LSO','LBR','LBY','MDG','MWI','MLI','MRT','MUS','MYT','MAR','MOZ','NAM','NER','NGA','REU','RWA','BLM','SHN','MAF','STP','SEN','SYC','SLE','SOM','ZAF','SSD','SDN','SWZ','TZA','TGO','TUN','UGA','ESH','ZMB','ZWE') THEN 'Africa' WHEN a.ISSUER_COUNTRY_CODE IN ('AFG','ALA','ARM','AZE','BHR','BGD','BTN','IOT','BRN','KHM','CHN','HKG','CXR','CCK','FLK','GUM','IND','IDN','IRN','IRQ','ISR','JPN','JOR','KAZ','PRK','KOR','KWT','KGZ','LAO','LBN','MYS','MDV','MNG','MMR','NPL','OMN','PAK','PSE','PHL','QAT','SAU','SGP','LKA','SYR','TWN','TJK','THA','TUR','TKM','ARE','UMI','UZB','VNM','YEM') THEN 'Asia' WHEN a.ISSUER_COUNTRY_CODE IN ('ASM','ATA','AUS','BVT','COK','FJI','PYF','ATF','HMD','KIR','MHL','FSM','NRU','NCL','NZL','NIU','NFK','MNP','PLW','PNG','PCN','WSM','SLB','TLS','TKL','TON','TUV','VUT','WLF') THEN 'Pacific' ELSE 'Other' END AS FLAG_REGION, CASE WHEN (a.GICS_LEVEL_1_NAME ='GOVERNMENT' AND a.FID1_INDUSTRY_NAME_LEVEL1 in ('SOVEREIGN', 'GOVERNMENT SPONSORED') AND a.ISSUER_COUNTRY_CODE NOT IN ('DNK','BMU','CYM','AUS','AUT','BEL','CAN','CHE','DEU','ESP','FIN','FRA','GBR','GRC','IRL','ITA','JPN','LUX','NLD','NOR','NZL','PRT','SWE','USA','JEY','GGY','SUP','XS','XCI','VGB','CYP')) THEN 'Govt EM' WHEN (a.GICS_LEVEL_1_NAME ='GOVERNMENT' AND a.FID1_INDUSTRY_NAME_LEVEL1 in ('SOVEREIGN', 'GOVERNMENT SPONSORED') AND a.ISSUER_COUNTRY_CODE IN ('DNK','BMU','CYM','AUS','AUT','BEL','CAN','CHE','DEU','ESP','FIN','FRA','GBR','GRC','IRL','ITA','JPN','LUX','NLD','NOR','NZL','PRT','SWE','USA','JEY','GGY','SUP','XS','XCI','VGB','CYP')) THEN 'Govt DM' WHEN a.ISSUER_COUNTRY_CODE IN ('DNK','BMU','CYM','AUS','AUT','BEL','CAN','CHE','DEU','ESP','FIN','FRA','GBR','GRC','IRL','ITA','JPN','LUX','NLD','NOR','NZL','PRT','SWE','USA','JEY','GGY','SUP','XS','XCI','VGB','CYP') THEN 'Corp DM' WHEN a.ISSUER_COUNTRY_CODE NOT IN ('DNK','BMU','CYM','AUS','AUT','BEL','CAN','CHE','DEU','ESP','FIN','FRA','GBR','GRC','IRL','ITA','JPN','LUX','NLD','NOR','NZL','PRT','SWE','USA','JEY','GGY','SUP','XS','XCI','VGB','CYP') THEN 'Corp EM' ELSE 'Other' END AS FLAG_DMEM, CASE WHEN a.CURRENCY_OF_MEASURE NOT IN ('EUR', 'GBP', 'USD') THEN 'OTHERS' ELSE a.CURRENCY_OF_MEASURE END AS FLAG_FX, CASE WHEN (GICS_LEVEL_1_NAME = 'FINANCIALS' OR (GICS_LEVEL_1_NAME IS NULL AND FID1_INDUSTRY_NAME_LEVEL1='CORPORATES: FINANCIALS')) THEN 'FINANCIALS' ELSE 'NON-FINANCIALS' end as FLAG_INDUSTRY, CASE WHEN GICS_LEVEL_1_NAME = 'FINANCIALS' THEN a.GICS_LEVEL_2_NAME ELSE a.GICS_LEVEL_1_NAME END AS FLAG_SECTOR, CASE WHEN a.FID1_SENIORITY IN ('AT1', 'SUBT1', 'SUBUT2') THEN 'JUNIOR' ELSE 'SENIOR' END AS FLAG_SENIORITY, CASE WHEN a.MRD_RATING IN ('AAA', 'AA', 'A', 'BBB') THEN 'IG' ELSE 'HY' END AS FLAG_INVESTMENT_GRADE, CASE WHEN a.PRODUCT_TYPE_CODE IN ('BOND','FRN','GVTBOND','AGN','GVTBONDIL','BONDIL','CD') THEN 'CASH' ELSE 'DERIVATIVES' END AS FLAG_CASH_DER, CASE WHEN (a.PRODUCT_TYPE_CODE IN ('CDSOPTIDX') AND a.REFERENCE_INDEX_ENTITY_NAME LIKE '%SENIOR FINANCIALS%') THEN 'CR Option - iTraxx Snr Fin' WHEN (a.PRODUCT_TYPE_CODE IN ('CDSOPTIDX') AND a.REFERENCE_INDEX_ENTITY_NAME LIKE '%SUB FINANCIALS%') THEN 'CR Option - iTraxx Sub Fin' WHEN (a.PRODUCT_TYPE_CODE IN ('CDSOPTIDX') AND a.REFERENCE_INDEX_ENTITY_NAME NOT LIKE '%SENIOR FINANCIALS%' AND a.REFERENCE_INDEX_ENTITY_NAME NOT LIKE '%SENIOR FINANCIALS%') THEN 'CR Option' || ' - ' || a.CCAR_INDEX_CLASS WHEN (a.PRODUCT_TYPE_CODE IN ('CRDINDEX') AND a.REFERENCE_INDEX_ENTITY_NAME LIKE '%SENIOR FINANCIALS%') THEN 'CR Index - iTraxx Snr Fin' WHEN (a.PRODUCT_TYPE_CODE IN ('CRDINDEX') AND a.REFERENCE_INDEX_ENTITY_NAME LIKE '%SUB FINANCIALS%') THEN 'CR Index - iTraxx Sub Fin' WHEN (a.PRODUCT_TYPE_CODE IN ('CRDINDEX') AND a.REFERENCE_INDEX_ENTITY_NAME NOT LIKE '%SENIOR FINANCIALS%' AND a.REFERENCE_INDEX_ENTITY_NAME NOT LIKE '%SENIOR FINANCIALS%') THEN 'CR Index' || ' - ' || a.CCAR_INDEX_CLASS ELSE a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME END AS POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, sum(coalesce(a.USD_PV10_BENCH,0)) as USD_PV10_BENCH FROM CDWUSER.U_EXP_MSR a WHERE a.COB_DATE in ('2018-02-28','2018-02-27','2018-01-31','2017-12-29','2017-11-30') AND a.CCC_TAPS_COMPANY ='0302' AND a.CCC_DIVISION IN ('FIXED INCOME DIVISION') AND a.CCC_BUSINESS_AREA NOT IN ('NON CORE','CPM','LENDING') AND a.CCC_BANKING_TRADING = 'TRADING' AND (a.CCC_PRODUCT_LINE NOT IN ('DISTRESSED TRADING') OR a.PRODUCT_TYPE_CODE IN ('CDSOPTIDX','CRDINDEX','LOANINDEX')) AND ((a.FID1_SENIORITY NOT IN ('AT1','SUBT1','SUBUT2') OR ISSUER_COUNTRY_CODE NOT IN ('DNK','BMU','CYM','AUS','AUT','BEL','CAN','CHE','DEU','ESP','FIN','FRA','GBR','GRC','IRL','ITA','JPN','LUX','NLD','NOR','NZL','PRT','SWE','USA','JEY','GGY','SUP','XS','XCI','VGB','CYP')) OR a.PRODUCT_TYPE_CODE NOT IN ('BOND','BONDFUT','BONDFUTOPT','BONDIL','BONDOPT','FRN','PREF')) AND a.VERTICAL_SYSTEM NOT LIKE '%SPG%' AND a.USD_PV10_BENCH <> 0 AND (a.FID1_INDUSTRY_NAME_LEVEL1 not in ('SOVEREIGN', 'GOVERNMENT SPONSORED') AND a.ISSUER_COUNTRY_CODE IN ('DNK','BMU','CYM','AUS','AUT','BEL','CAN','CHE','DEU','ESP','FIN','FRA','GBR','GRC','IRL','ITA','JPN','LUX','NLD','NOR','NZL','PRT','SWE','USA','JEY','GGY','SUP','XS','XCI','VGB','CYP')) AND book not in ('OBTRS') GROUP BY a.COB_DATE, a.CCC_DIVISION, a.CCC_BUSINESS_AREA, a.CCC_PRODUCT_LINE, a.PRODUCT_TYPE_CODE, a.BOOK, CASE WHEN (a.CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' AND a.PRODUCT_TYPE_CODE IN ('BOND','GVTBOND','GVTBONDIL','FRN','BANKDEBT','AGN','CD', 'BONDIL') AND a.MRD_RATING IN ('AAA','AA','A','BBB')) THEN 'Credit Flow Cash IG' WHEN (a.CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' AND a.PRODUCT_TYPE_CODE IN ('BOND','GVTBOND','GVTBONDIL','FRN','BANKDEBT','AGN','CD', 'BONDIL') AND a.MRD_RATING NOT IN ('AAA','AA','A','BBB')) THEN 'Credit Flow Cash HY' WHEN (a.CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' AND a.PRODUCT_TYPE_CODE IN ('DEFSWAP')) THEN 'Credit Flow CDS' WHEN (a.CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' AND a.PRODUCT_TYPE_CODE NOT IN ('BOND','GVTBOND','GVTBONDIL','FRN','BANKDEBT','AGN','CD', 'BONDIL','DEFSWAP')) THEN 'Credit Flow Derivatives' WHEN (a.CCC_BUSINESS_AREA = 'DSP - CREDIT') THEN 'DSP Credit' WHEN (a.CCC_BUSINESS_AREA NOT IN ('CREDIT-CORPORATES','DSP - CREDIT')) THEN 'SPG & Others' END, CASE WHEN a.CCC_PRODUCT_LINE IN ('PRIMARY - IG BONDS','PRIMARY - NIG BONDS') THEN 'Primary' WHEN (a.CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' AND a.CCC_PL_REPORTING_REGION = 'EMEA') THEN 'Credit Flow EMEA' WHEN (a.CCC_BUSINESS_AREA = 'CREDIT-CORPORATES' AND a.CCC_PL_REPORTING_REGION = 'ASIA PACIFIC') THEN 'Credit Flow Asia' WHEN a.CCC_BUSINESS_AREA IN ('EM CREDIT TRADING','FXEM MACRO TRADING') THEN 'EM Credit and FX' WHEN a.CCC_BUSINESS_AREA IN ('LIQUID FLOW RATES','STRUCTURED RATES') THEN 'Rates Liquid/Structured' WHEN a.CCC_BUSINESS_AREA IN ('DSP - CREDIT','SECURITIZED PRODUCTS GRP') THEN 'Credit Derivatives & SPG' ELSE 'Other' END, CASE WHEN a.ISSUER_COUNTRY_CODE IN ('XS') THEN 'Supranational' WHEN a.ISSUER_COUNTRY_CODE IN ('ALB','AND','AUT','BLR','BEL','BIH','BGR','CYP','HRV','CZE','DNK','EST','FRO','FIN','FRA','GEO','DEU','GRC','GGY','VAT','HUN','ISL','IRL','IMN','ITA','JEY','LVA','LIE','LTU','LUX','MKD','MLT','MDA','MCO','MNE','NLD','NOR','POL','PRT','ROU','RUS','SMR','SRB','SVK','SVN','ESP','SJM','SWE','CHE','UKR') THEN 'Europe' WHEN a.ISSUER_COUNTRY_CODE IN ('GBR') THEN 'UK' WHEN a.ISSUER_COUNTRY_CODE IN ('USA','CAN') THEN 'NAM' WHEN a.ISSUER_COUNTRY_CODE IN ('AIA','ATG','ARG','ABW','BHS','BRB','BLZ','BMU','BOL','BRA','VGB','CYM','CHL','COL','CRI','CUB','DMA','DOM','ECU','SLV','GUF','GRL','GRD','GLP','GTM','GUY','HTI','HND','JAM','MTQ','MEX','MSR','ANT','NIC','PAN','PRY','PER','PRI','KNA','LCA','SPM','VCT','SGS','SUR','TTO','TCA','URY','VEN','VIR') THEN 'LATAM' WHEN a.ISSUER_COUNTRY_CODE IN ('DZA','AGO','BEN','BWA','BFA','BDI','CMR','CPV','CAF','TCD','COM','COD','CIV','DJI','EGY','GNQ','ERI','ETH','GAB','GMB','GHA','GIB','GIN','GNB','KEN','LSO','LBR','LBY','MDG','MWI','MLI','MRT','MUS','MYT','MAR','MOZ','NAM','NER','NGA','REU','RWA','BLM','SHN','MAF','STP','SEN','SYC','SLE','SOM','ZAF','SSD','SDN','SWZ','TZA','TGO','TUN','UGA','ESH','ZMB','ZWE') THEN 'Africa' WHEN a.ISSUER_COUNTRY_CODE IN ('AFG','ALA','ARM','AZE','BHR','BGD','BTN','IOT','BRN','KHM','CHN','HKG','CXR','CCK','FLK','GUM','IND','IDN','IRN','IRQ','ISR','JPN','JOR','KAZ','PRK','KOR','KWT','KGZ','LAO','LBN','MYS','MDV','MNG','MMR','NPL','OMN','PAK','PSE','PHL','QAT','SAU','SGP','LKA','SYR','TWN','TJK','THA','TUR','TKM','ARE','UMI','UZB','VNM','YEM') THEN 'Asia' WHEN a.ISSUER_COUNTRY_CODE IN ('ASM','ATA','AUS','BVT','COK','FJI','PYF','ATF','HMD','KIR','MHL','FSM','NRU','NCL','NZL','NIU','NFK','MNP','PLW','PNG','PCN','WSM','SLB','TLS','TKL','TON','TUV','VUT','WLF') THEN 'Pacific' ELSE 'Other' END, CASE WHEN (a.GICS_LEVEL_1_NAME ='GOVERNMENT' AND a.FID1_INDUSTRY_NAME_LEVEL1 in ('SOVEREIGN', 'GOVERNMENT SPONSORED') AND a.ISSUER_COUNTRY_CODE NOT IN ('DNK','BMU','CYM','AUS','AUT','BEL','CAN','CHE','DEU','ESP','FIN','FRA','GBR','GRC','IRL','ITA','JPN','LUX','NLD','NOR','NZL','PRT','SWE','USA','JEY','GGY','SUP','XS','XCI','VGB','CYP')) THEN 'Govt EM' WHEN (a.GICS_LEVEL_1_NAME ='GOVERNMENT' AND a.FID1_INDUSTRY_NAME_LEVEL1 in ('SOVEREIGN', 'GOVERNMENT SPONSORED') AND a.ISSUER_COUNTRY_CODE IN ('DNK','BMU','CYM','AUS','AUT','BEL','CAN','CHE','DEU','ESP','FIN','FRA','GBR','GRC','IRL','ITA','JPN','LUX','NLD','NOR','NZL','PRT','SWE','USA','JEY','GGY','SUP','XS','XCI','VGB','CYP')) THEN 'Govt DM' WHEN a.ISSUER_COUNTRY_CODE IN ('DNK','BMU','CYM','AUS','AUT','BEL','CAN','CHE','DEU','ESP','FIN','FRA','GBR','GRC','IRL','ITA','JPN','LUX','NLD','NOR','NZL','PRT','SWE','USA','JEY','GGY','SUP','XS','XCI','VGB','CYP') THEN 'Corp DM' WHEN a.ISSUER_COUNTRY_CODE NOT IN ('DNK','BMU','CYM','AUS','AUT','BEL','CAN','CHE','DEU','ESP','FIN','FRA','GBR','GRC','IRL','ITA','JPN','LUX','NLD','NOR','NZL','PRT','SWE','USA','JEY','GGY','SUP','XS','XCI','VGB','CYP') THEN 'Corp EM' ELSE 'Other' END, CASE WHEN a.CURRENCY_OF_MEASURE NOT IN ('EUR', 'GBP', 'USD') THEN 'OTHERS' ELSE a.CURRENCY_OF_MEASURE END, CASE WHEN (GICS_LEVEL_1_NAME = 'FINANCIALS' OR (GICS_LEVEL_1_NAME IS NULL AND FID1_INDUSTRY_NAME_LEVEL1='CORPORATES: FINANCIALS')) THEN 'FINANCIALS' ELSE 'NON-FINANCIALS' END, CASE WHEN GICS_LEVEL_1_NAME = 'FINANCIALS' THEN a.GICS_LEVEL_2_NAME ELSE a.GICS_LEVEL_1_NAME END, CASE WHEN a.FID1_SENIORITY IN ('AT1', 'SUBT1', 'SUBUT2') THEN 'JUNIOR' ELSE 'SENIOR' END, CASE WHEN a.MRD_RATING IN ('AAA', 'AA', 'A', 'BBB') THEN 'IG' ELSE 'HY' END, CASE WHEN a.PRODUCT_TYPE_CODE IN ('BOND','FRN','GVTBOND','AGN','GVTBONDIL','BONDIL','CD') THEN 'CASH' ELSE 'DERIVATIVES' END, CASE WHEN (a.PRODUCT_TYPE_CODE IN ('CDSOPTIDX') AND a.REFERENCE_INDEX_ENTITY_NAME LIKE '%SENIOR FINANCIALS%') THEN 'CR Option - iTraxx Snr Fin' WHEN (a.PRODUCT_TYPE_CODE IN ('CDSOPTIDX') AND a.REFERENCE_INDEX_ENTITY_NAME LIKE '%SUB FINANCIALS%') THEN 'CR Option - iTraxx Sub Fin' WHEN (a.PRODUCT_TYPE_CODE IN ('CDSOPTIDX') AND a.REFERENCE_INDEX_ENTITY_NAME NOT LIKE '%SENIOR FINANCIALS%' AND a.REFERENCE_INDEX_ENTITY_NAME NOT LIKE '%SENIOR FINANCIALS%') THEN 'CR Option' || ' - ' || a.CCAR_INDEX_CLASS WHEN (a.PRODUCT_TYPE_CODE IN ('CRDINDEX') AND a.REFERENCE_INDEX_ENTITY_NAME LIKE '%SENIOR FINANCIALS%') THEN 'CR Index - iTraxx Snr Fin' WHEN (a.PRODUCT_TYPE_CODE IN ('CRDINDEX') AND a.REFERENCE_INDEX_ENTITY_NAME LIKE '%SUB FINANCIALS%') THEN 'CR Index - iTraxx Sub Fin' WHEN (a.PRODUCT_TYPE_CODE IN ('CRDINDEX') AND a.REFERENCE_INDEX_ENTITY_NAME NOT LIKE '%SENIOR FINANCIALS%' AND a.REFERENCE_INDEX_ENTITY_NAME NOT LIKE '%SENIOR FINANCIALS%') THEN 'CR Index' || ' - ' || a.CCAR_INDEX_CLASS ELSE a.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME END ), y as ( SELECT x.COB_DATE, x.FLAG_CORPDM, x.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, sum(x.USD_PV10_BENCH) as USD_PV10_BENCH FROM x GROUP BY x.COB_DATE, x.FLAG_CORPDM, x.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME ), z as ( SELECT dates.dates_name, y.COB_DATE, y.FLAG_CORPDM, CASE WHEN dates.dates_name = 'COB_ONE' THEN USD_PV10_BENCH WHEN dates.dates_name = 'COB_TWO' THEN -USD_PV10_BENCH ELSE 0 END AS CHANGE_ONE, CASE WHEN dates.dates_name = 'COB_TWO' THEN USD_PV10_BENCH WHEN dates.dates_name = 'COB_THREE' THEN -USD_PV10_BENCH ELSE 0 END AS CHANGE_TWO, CASE WHEN dates.dates_name = 'COB_THREE' THEN USD_PV10_BENCH WHEN dates.dates_name = 'COB_FOUR' THEN -USD_PV10_BENCH ELSE 0 END AS CHANGE_THREE, CASE WHEN dates.dates_name = 'COB_FOUR' THEN USD_PV10_BENCH WHEN dates.dates_name = 'COB_FIVE' THEN -USD_PV10_BENCH ELSE 0 END AS CHANGE_FOUR, y.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, y.USD_PV10_BENCH FROM y JOIN dates ON y.COB_DATE = dates.dates_mrdw ), w as ( SELECT z.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, z.FLAG_CORPDM, sum(CHANGE_ONE) as CHANGE_ONE, sum(CHANGE_TWO) as CHANGE_TWO, sum(CHANGE_THREE) as CHANGE_THREE, sum(CHANGE_FOUR) as CHANGE_FOUR FROM z GROUP BY z.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, z.FLAG_CORPDM ), j as ( SELECT w.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, w.FLAG_CORPDM, RANK() OVER (PARTITION BY w.FLAG_CORPDM ORDER BY ABS(w.CHANGE_ONE) DESC) AS RANK_ONE, w.CHANGE_ONE, RANK() OVER (PARTITION BY w.FLAG_CORPDM ORDER BY ABS(w.CHANGE_TWO) DESC) AS RANK_TWO, w.CHANGE_TWO, RANK() OVER (PARTITION BY w.FLAG_CORPDM ORDER BY ABS(w.CHANGE_THREE) DESC) AS RANK_THREE, w.CHANGE_THREE, RANK() OVER (PARTITION BY w.FLAG_CORPDM ORDER BY ABS(w.CHANGE_FOUR) DESC) AS RANK_FOUR, w.CHANGE_FOUR FROM w ) SELECT RANK_ONE || '.' || FLAG_CORPDM as ID_ONE, CHANGE_ONE, RANK_TWO || '.' || FLAG_CORPDM as ID_TWO, CHANGE_TWO, RANK_THREE || '.' || FLAG_CORPDM as ID_THREE, CHANGE_THREE, RANK_FOUR || '.' || FLAG_CORPDM as ID_FOUR, CHANGE_FOUR, POSITION_ULT_ISSUER_PARTY_DARWIN_NAME FROM j WHERE (RANK_ONE <= 5 OR RANK_TWO <= 5 OR RANK_THREE <= 5 OR RANK_FOUR <= 5)