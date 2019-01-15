with src as ( SELECT K.*, ROW_NUMBER() OVER() AS ROWNUM FROM ( SELECT max(POSITION_ISSUER_PARTY_DARWIN_NAME) as legal_ultimate_name, POSITION_ISSUER_PARTY_DARWIN_ID as legal_ultimate_code, a.mrd_rating as rating, a.fid1_industry_name_level4, a.country_cd_of_risk as country, 0 AS usd_pv01, 0 AS usd_pv01_c, SUM (case when cob_date = '2018-02-28' then usd_pv01sprd else 0 end) AS usd_pv01sprd, SUM (case when cob_date = '2018-02-28' then usd_pv01sprd else -usd_pv01sprd end) AS usd_pv01sprd_c, SUM (case when cob_date = '2018-02-28' then usd_exposure else 0 end) AS usd_net_exposure, SUM (case when cob_date = '2018-02-28' then usd_exposure else -usd_exposure end) AS usd_net_exposure_c FROM CDWUSER.U_CR_MSR a WHERE a.cob_date in ('2018-02-28','2018-02-27') and a.sectype2 NOT IN ('FX', upper('Swaps'), upper('RateFuture')) AND a.product_type_code <> 'CP' AND coalesce(a.POSITION_ISSUER_PARTY_DARWIN_NAME,'UNKNOWN') <> 'UNKNOWN' and a.var_excl_fl <> 'Y' AND PARENT_LEGAL_ENTITY = '0517(G)' AND a.country_cd_of_risk NOT IN ('DNK', 'BMU', 'CYM', 'AUS', 'AUT', 'BEL', 'CAN', 'CHE', 'DEU', 'ESP', 'FIN', 'FRA', 'GBR', 'GRC', 'IRL', 'ITA', 'JPN', 'LUX', 'NLD', 'NOR', 'NZL', 'PRT', 'SWE', 'USA', 'JEY') AND ccc_banking_trading = 'TRADING' GROUP BY a.POSITION_ISSUER_PARTY_DARWIN_ID, a.mrd_rating, a.fid1_industry_name_level4, a.country_cd_of_risk UNION ALL SELECT max(POSITION_ISSUER_PARTY_DARWIN_NAME) as legal_ultimate_name, POSITION_ISSUER_PARTY_DARWIN_ID as legal_ultimate_code, a.mrd_rating as rating, a.fid1_industry_name_level4, a.country_cd_of_risk as country, SUM (case when cob_date = '2018-02-28' then usd_ir_unified_pv01 else 0 end) AS usd_pv01, SUM (case when cob_date = '2018-02-28' then usd_ir_unified_pv01 else -usd_ir_unified_pv01 end) AS usd_pv01_c, 0 AS usd_pv01sprd, 0 AS usd_pv01sprd_c, 0 AS usd_net_exposure, 0 AS usd_net_exposure_c FROM CDWUSER.U_IR_MSR a WHERE a.cob_date in ('2018-02-28','2018-02-27') and a.sectype2 NOT IN ('FX', upper('Swaps'), upper('RateFuture')) AND a.product_type_code <> 'CP' AND coalesce(a.POSITION_ISSUER_PARTY_DARWIN_NAME,'UNKNOWN') <> 'UNKNOWN' and a.var_excl_fl <> 'Y' AND PARENT_LEGAL_ENTITY = '0517(G)' AND a.country_cd_of_risk NOT IN ('DNK', 'BMU', 'CYM', 'AUS', 'AUT', 'BEL', 'CAN', 'CHE', 'DEU', 'ESP', 'FIN', 'FRA', 'GBR', 'GRC', 'IRL', 'ITA', 'JPN', 'LUX', 'NLD', 'NOR', 'NZL', 'PRT', 'SWE', 'USA', 'JEY') AND ccc_banking_trading = 'TRADING' GROUP BY a.POSITION_ISSUER_PARTY_DARWIN_ID, a.mrd_rating, a.fid1_industry_name_level4, a.country_cd_of_risk ) K ), grp as ( SELECT max(legal_ultimate_name) as legal_ultimate_name, legal_ultimate_code, ROW_NUMBER() OVER() AS ROWNUM, SUM (usd_pv01) AS usd_pv01, SUM (usd_pv01_c) AS usd_pv01_c, SUM (usd_pv01sprd) AS usd_pv01sprd, SUM (usd_pv01sprd_c) AS usd_pv01sprd_c, SUM (usd_net_exposure) AS usd_net_exposure, SUM (usd_net_exposure_c) AS usd_net_exposure_c FROM src GROUP BY legal_ultimate_code) , src_rank as ( select s.*, rank() over(partition by legal_ultimate_code order by abs(coalesce(usd_pv01sprd,0)) desc, rownum) as pv01_rank, rank() over(partition by legal_ultimate_code order by abs(coalesce(usd_net_exposure,0)) desc, rownum) as usd_net_exposure_rank from src s), grp_rank as ( select g.*, rank() over(order by abs(coalesce(usd_pv01sprd,0)) desc, rownum) as pv01_rank, rank() over(order by abs(coalesce(usd_net_exposure,0)) desc, rownum) as usd_net_exposure_rank from grp g) select g.legal_ultimate_name, g.legal_ultimate_code, g.pv01_rank, null as usd_net_exposure_rank, s.rating, s.fid1_industry_name_level4, s.country, g.usd_pv01, g.usd_pv01_c, g.usd_pv01sprd, g.usd_pv01sprd_c, g.usd_net_exposure, g.usd_net_exposure_c from (select * from src_rank where pv01_rank = 1) s, grp_rank g where s.legal_ultimate_code=g.legal_ultimate_code and g.pv01_rank <= 30 union select g.legal_ultimate_name, g.legal_ultimate_code, null as pv01_rank, g.usd_net_exposure_rank, s.rating, s.fid1_industry_name_level4, s.country, g.usd_pv01, g.usd_pv01_c, g.usd_pv01sprd, g.usd_pv01sprd_c, g.usd_net_exposure, g.usd_net_exposure_c from (select * from src_rank where usd_net_exposure_rank = 1) s, grp_rank g where s.legal_ultimate_code=g.legal_ultimate_code and g.usd_net_exposure_rank <= 30