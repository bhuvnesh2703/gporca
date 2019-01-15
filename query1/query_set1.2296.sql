with src as ( select cob_date, ccc_business_area, ultimate_code, issuer_or_sec_code, max(ultimate_name) as ultimate_name, max(issuer_or_sec_desc) as issuer_or_sec_desc, change_multiplier, cob_multiplier, sum(DELTA_USD) as DELTA_USD from( select distinct cob_date, ccc_business_area, ISSUER_PARTY_DARWIN_ID as ultimate_code, child_issuer_party_darwin_id as issuer_or_sec_code, max(case when ISSUER_PARTY_DARWIN_NAME = 'UNDEFINED' then '-' else coalesce(ISSUER_PARTY_DARWIN_NAME, '-') end) as ultimate_name, max(case when child_issuer_party_darwin_name = 'UNDEFINED' then case when product_description_decomp = 'UNDEEFINED' then '-' else coalesce(product_description_decomp, '-') end else coalesce(child_issuer_party_darwin_name, '-') end) AS issuer_or_sec_desc, case when cob_date = '2018-02-28' then 1 else 0 end as cob_multiplier, case when cob_date = '2018-02-28' then 1 else -1 end as change_multiplier, sum(USD_EQ_DELTA_DECOMP) as DELTA_USD from CDWUSER.U_DECOMP_MSR WHERE cob_date in ('2018-02-28', '2018-02-27') AND PARENT_LEGAL_ENTITY = '0302(G)' AND ccc_business_area not in ('PDT DORMANT','PROCESS DRIVEN TRADING') AND ccc_strategy <> 'CORE PRIMARY' and ccc_product_line <> 'CORE PRIMARY' AND feed_source_id = 301 AND VAR_EXCL_FL <> 'Y' AND COALESCE (PRODUCT_TYPE_CODE_DECOMP, '') <> 'COMM' AND COALESCE (CASH_ISSUE_TYPE, '') <> 'COMM' AND ccc_banking_trading = 'TRADING' GROUP BY cob_date, ccc_business_area, ISSUER_PARTY_DARWIN_ID, child_issuer_party_darwin_id /*ULT_ISSUER_PARTY_DARWIN_NAME, issuer_party_darwin_name*/ ) P group by cob_date, ccc_business_area, ultimate_code, issuer_or_sec_code, /*ultimate_name, issuer_or_sec_desc,*/ change_multiplier, cob_multiplier ) , rank_data as ( SELECT I.*, rank() over (order by abs(DELTA_USD_COB) desc, rownum ) as rank_delta, rank() over (order by abs(DELTA_USD_CHANGE) desc, rownum) as rank_delta_chnage FROM ( SELECT max(ultimate_name) as ultimate_name, max(issuer_or_sec_desc) as issuer_or_sec_desc, ROW_NUMBER() OVER() AS ROWNUM, SUM(DELTA_USD * cob_multiplier) AS DELTA_USD_COB, SUM(DELTA_USD * change_multiplier) AS DELTA_USD_CHANGE FROM src GROUP BY ultimate_code, issuer_or_sec_code ) I ) SELECT * FROM (SELECT ultimate_name, issuer_or_sec_desc, ccc_business_area, delta_usd_cob, delta_usd_change, rank_delta, ROW_NUMBER() OVER() as rank_delta_chnage FROM( SELECT * FROM (SELECT a.ultimate_name, a.issuer_or_sec_desc, case when ccc_business_area = 'CREDIT-CORPORATES' then 'CREDIT-CORP' when ccc_business_area = 'FXEM CREDIT TRADING' then 'FXEM CREDIT TR' when ccc_business_area = 'FXEM MACRO TRADING' then 'FXEM MACRO TR' else ccc_business_area end as ccc_business_area, a.DELTA_USD_COB, a.DELTA_USD_CHANGE, a.rank_delta, ROW_NUMBER() OVER () AS rank_delta_chnage FROM rank_data a, (SELECT ultimate_name, issuer_or_sec_desc, ccc_business_area FROM( SELECT ultimate_name, issuer_or_sec_desc, ccc_business_area, rank() over (partition by ultimate_name, issuer_or_sec_desc order by abs(DELTA_USD_COB) desc, rownum) as rank_business_area FROM( select max(ultimate_name) as ultimate_name, max(issuer_or_sec_desc) as issuer_or_sec_desc, ccc_business_area, ROW_NUMBER() OVER() AS ROWNUM, sum(DELTA_USD_COB) as DELTA_USD_COB, sum(DELTA_USD_CHANGE) as DELTA_USD_CHANGE from ( SELECT max(ultimate_name) as ultimate_name, max(issuer_or_sec_desc) as issuer_or_sec_desc, ultimate_code, issuer_or_sec_code, ccc_business_area, SUM(DELTA_USD * cob_multiplier) AS DELTA_USD_COB, SUM(DELTA_USD * change_multiplier) AS DELTA_USD_CHANGE FROM src a GROUP BY ultimate_code, issuer_or_sec_code, ccc_business_area ) X group by ultimate_code, issuer_or_sec_code, ccc_business_area ) Y ) Z WHERE rank_business_area = 1) b WHERE a.ultimate_name = b.ultimate_name AND a.issuer_or_sec_desc = b.issuer_or_sec_desc ORDER BY a.rank_delta) P where LEAST(rank_delta, rank_delta_chnage) <= 50 ) a ORDER BY a.rank_delta_chnage) O WHERE LEAST(rank_delta, rank_delta_chnage) <= 50