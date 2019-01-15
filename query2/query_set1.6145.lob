SELECT  sum(coalesce(z.USD_PV10_Bench,0)+coalesce(z.USD_Credit_PV10PCT,0)) as PV10 ,z.cob_date ,z.country_cd_of_risk,
z.PRODUCT_TYPE_CODE,case 
 when z.vertical_system like '%EQUITY%' then 'EQUITY'
 when z.vertical_system like '%C1%' then 'C1'
 when z.vertical_system like '%STS%' then 'STS' else z.vertical_system end  as VS

 FROM cdwuser.U_EXP_TRENDS Z
where z.COB_DATE in ('2018-02-28','2018-02-21') 
and z.CCC_DIVISION ='INSTITUTIONAL EQUITY DIVISION'
and z.CCC_TAPS_COMPANY in ('8790','7435','7280','7016','6316','6262','5274','4562','4512','4391','4341','4086','1308','0993','0853','0721','0715','0620','0517','0347','0342','0302')
 and z.CCC_PRODUCT_LINE ='CONVERTIBLE PRODUCTS'
and z.country_cd_of_risk in ('CHN','HKG')
group by z.cob_date,z.country_cd_of_risk,
z.PRODUCT_TYPE_CODE ,case 
 when z.vertical_system like '%EQUITY%' then 'EQUITY'
 when z.vertical_system like '%C1%' then 'C1'
 when z.vertical_system like '%STS%' then 'STS' else z.vertical_system end