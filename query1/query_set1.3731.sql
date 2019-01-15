select 
a.LE_GROUP,
a.CCC_PL_REPORTING_REGION,
a.CCC_DIVISION,
a.CCC_BUSINESS_AREA,
a.COB_DATE,
sum(coalesce(a.USD_FX_WIDE_SKEW,0)) as FX_SKEW,
case when currency_of_measure='USD' then 'USD'
when currency_of_measure='EUR' then 'EUR'
when currency_of_measure='GBP' then 'GBP'
when currency_of_measure in ('CNY', 'CNH') then 'CNY'
when currency_of_measure in ('KRW', 'KRX') then 'KRW'
when currency_of_measure in ('RUB', 'RBX','RU1') then 'RUB'
when currency_of_measure in ('TRY') then 'TRY' 
else 'Others' end as FX_FLAG,
case when (a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD','COMMODS FINANCING') 
OR a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES')) AND a.CCC_PRODUCT_LINE NOT IN ('CREDIT LOAN PORTFOLIO', 'CMD STRUCTURED FINANCE') then 'CVA_Skew'
when a.CCC_BUSINESS_AREA = 'LENDING' then 'LENDING'
 else 'FX_Skew' end as CVA_FL
from cdwuser.U_FX_MSR a
where 
 cob_date IN ('2018-02-28','2018-02-21')
    and a.CCC_DIVISION <> 'INSTITUTIONAL EQUITY DIVISION'
    AND a.USD_FX_WIDE_SKEW <> 0 
    and a.vertical_system like ('%FXOPT%')     AND CCC_DIVISION NOT IN ('FID DVA', 'FIC DVA')  AND CCC_STRATEGY NOT IN ('MS DVA STR NOTES IED') 
group by
a.LE_GROUP,
a.CCC_PL_REPORTING_REGION,
a.CCC_DIVISION,
a.CCC_BUSINESS_AREA,
a.COB_DATE,
case when currency_of_measure='USD' then 'USD'
when currency_of_measure='EUR' then 'EUR'
when currency_of_measure='GBP' then 'GBP'
when currency_of_measure in ('CNY', 'CNH') then 'CNY'
when currency_of_measure in ('KRW', 'KRX') then 'KRW'
when currency_of_measure in ('RUB', 'RBX','RU1') then 'RUB'
when currency_of_measure in ('TRY') then 'TRY' 
else 'Others' end,
case when (a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD','COMMODS FINANCING') 
OR a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES')) AND a.CCC_PRODUCT_LINE NOT IN ('CREDIT LOAN PORTFOLIO', 'CMD STRUCTURED FINANCE') then 'CVA_Skew'
when a.CCC_BUSINESS_AREA = 'LENDING' then 'LENDING'
 else 'FX_Skew' end