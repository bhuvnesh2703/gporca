with src as (select country_cd_of_risk as country, mrd_rating as rating, sum (case when cob_date = '2018-02-28' then usd_pv01sprd else 0 end) as usd_pv01sprd, sum (case when cob_date = '2018-02-28' then usd_pv01sprd else -usd_pv01sprd end) as usd_pv01sprd_chng from CDWUSER.U_CR_MSR WHERE cob_date in ('2018-02-28', '2018-01-31') AND PARENT_LEGAL_ENTITY = '0201(G)' AND VAR_EXCL_FL <> 'Y' GROUP BY country_cd_of_risk, mrd_rating), ranked as (SELECT country, sum(case when rating = 'AAA' then usd_pv01sprd end) as spv01_aaa, sum(case when rating = 'AA' then usd_pv01sprd end) as spv01_aa, sum(case when rating = 'A' then usd_pv01sprd end) as spv01_a, sum(case when rating = 'BBB' then usd_pv01sprd end) as spv01_bbb, sum(case when rating = 'BB' then usd_pv01sprd end) as spv01_bb, sum(case when rating = 'B' then usd_pv01sprd end) as spv01_b, sum(case when rating in ('CCC','CC','C','D') then usd_pv01sprd end) as spv01_b_low, sum(case when coalesce(rating,'NR') not in ('AAA','AA','A', 'BBB','BB','B', 'CCC','CC','C','D') then usd_pv01sprd end) as spv01_nr, sum(usd_pv01sprd) AS pv01sprd, sum(case when rating = 'AAA' then usd_pv01sprd_chng end) as spv01_aaa_c, sum(case when rating = 'AA' then usd_pv01sprd_chng end) as spv01_aa_c, sum(case when rating = 'A' then usd_pv01sprd_chng end) as spv01_a_c, sum(case when rating = 'BBB' then usd_pv01sprd_chng end) as spv01_bbb_c, sum(case when rating = 'BB' then usd_pv01sprd_chng end) as spv01_bb_c, sum(case when rating = 'B' then usd_pv01sprd_chng end) as spv01_b_c, sum(case when rating in ('CCC','CC','C','D') then usd_pv01sprd_chng end) as spv01_b_low_c, sum(case when coalesce(rating,'NR') not in ('AAA','AA','A', 'BBB','BB','B', 'CCC','CC','C','D') then usd_pv01sprd_chng end) as spv01_nr_c, sum(usd_pv01sprd_chng) AS pv01sprd_c, least (rank() over(order by strpos(country,'XS'), abs(sum(coalesce(usd_pv01sprd,0))) desc, country), 30) + sign(strpos(country,'XS')) as rank FROM src GROUP BY country) SELECT rank, case rank when 30 then 'Other' when 31 then 'Multi-Country' else country end as country, sum(spv01_aaa) spv01_aaa, sum(spv01_aaa_c) spv01_aaa_c, sum(spv01_aa) spv01_aa, sum(spv01_aa_c) spv01_aa_c, sum(spv01_a) spv01_a, sum(spv01_a_c) spv01_a_c, sum(spv01_bbb) spv01_bbb, sum(spv01_bbb_c) spv01_bbb_c, sum(spv01_bb) spv01_bb, sum(spv01_bb_c) spv01_bb_c, sum(spv01_b) spv01_b, sum(spv01_b_c) spv01_b_c, sum(spv01_b_low) spv01_b_low, sum(spv01_b_low_c) spv01_b_low_c, sum(spv01_nr) spv01_nr, sum(spv01_nr_c) spv01_nr_c, sum(pv01sprd) pv01sprd, sum(pv01sprd_c) pv01sprd_c FROM ranked t GROUP BY rank, case rank when 30 then 'Other' when 31 then 'Multi-Country' else country end ORDER BY rank