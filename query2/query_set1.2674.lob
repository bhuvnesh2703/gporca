select 
a.COB_DATE    /*, sum(a.USD_IR_UNIFIED_PV01) as PV01 */
,sum( ( CASE
              WHEN USD_PV01 IS NULL THEN 0
              ELSE USD_PV01
            END ) + ( CASE
                        WHEN USD_IR_PARTIAL_PV01 IS NULL THEN 0
                                               ELSE USD_IR_PARTIAL_PV01
                      END ) + ( ( CASE
                                    WHEN USD_IR_RHO IS NULL THEN 0
                                     WHEN VERTICAL_SYSTEM LIKE '%EQUITY%' then 0
                                    ELSE USD_IR_RHO
                                  END ) / 10000 )) as PV01

, sign(sum(a.USD_IR_UNIFIED_PV01))*400 as limit

from CDWUSER.U_IR_MSR a

WHERE
            cob_date between ('2018-01-31') and ('2018-02-28')

AND LE_GROUP = 'UK'
AND a.BOOK NOT IN ('SECTD')
AND CCC_STRATEGY NOT IN ('CVA RISK MANAGEMENT','FVA RISK MANAGEMENT','CPM - OTHER','CPM CREDIT',
                'CPM FUNDING','XVA OTHER', 'XVA CREDIT', 'XVA FUNDING', 'MS CVA MNE - DERIVATIVES','MS CVA MPE - DERIVATIVES','EQ XVA HEDGING')
AND CCC_LE_GROUP_BTI='BANKING'
group by a.COB_DATE