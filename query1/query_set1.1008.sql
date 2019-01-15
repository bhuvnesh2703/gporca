SELECT     a.COB_DATE, a.CURVE_CURRENCY,     a.curve_name,     SUM (COALESCE (a.USD_BREAKEVEN_KAPPA,0) / 10) AS USD_BREAKEVEN_KAPPA FROM cdwuser.U_EXP_MSR a  WHERE (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-01-01') AND  CCC_PL_REPORTING_REGION in ('EMEA') AND      (a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)', 'CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR      a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING'))       and USD_BREAKEVEN_KAPPA is not null GROUP BY     a.COB_DATE, a.CURVE_CURRENCY,     a.curve_name