With X as (select upper(e.UNDERLIER_PRODUCT_DESCRIPTION) as Security_description, upper(e.UNDERLIER_TICK) as tick, coalesce(e.USD_EQ_KAPPA,0) as Vega from cdwuser.U_EQ_MSR e where e.COB_DATE = '2018-02-28' and e.DIVISION = 'IED' and silo_src = 'IED' and e.CCC_BANKING_TRADING = 'TRADING' and (upper(e.UNDERLIER_PRODUCT_DESCRIPTION) IN('S&P 500 INDEX', 'EURO STOXX 50') or upper(e.UNDERLIER_TICK) IN('FTSE','GDAXI','N225','HSI','KO201')) ) Select case when security_description = 'S&P 500 INDEX' then 'TotalSPXVega' else 'TotalSX5EVega' end as Concentration, sum(Vega) as Measure from x where security_description in ('S&P 500 INDEX', 'EURO STOXX 50') group by case when security_description = 'S&P 500 INDEX' then 'TotalSPXVega' else 'TotalSX5EVega' end union all Select 'LargeIndexVega' as Concentration, TopVega as Measure from ( select tick, sum(vega) as TopVega from x where tick IN('FTSE','GDAXI','N225') group by tick order by abs(sum(vega)) desc fetch first 1 rows only) a Union All Select 'MediumIndexVega' as Concentration, TopVega as Measure from ( select tick, sum(vega) as TopVega from x where tick IN('HSI','KO201') group by tick order by abs(sum(vega)) desc fetch first 1 rows only) a