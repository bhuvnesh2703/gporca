SELECT     a.COB_DATE,a.CCC_PRODUCT_LINE,     CASE WHEN term_new <= 1.5 THEN '0-1YR' WHEN     term_new > 1.5 AND     term_new <= 6.5     THEN '2-6YR' WHEN     term_new > 6.5 AND     term_new <= 13.5     THEN '7-14YR' WHEN     term_new > 13.5 AND     term_new <= 30.5     THEN '15-30YR' WHEN term_new > 30.5 THEN '30Yr+'     ELSE 'NULL' END AS term_new_group,     CASE      WHEN (a.curve_name like '%_disc' or a.curve_name like '%_3m')THEN 'IR OIS Basis'     WHEN (a.curve_name like '%_1m') THEN 'IR 1M Basis'     WHEN (a.curve_name like '%_6m') THEN 'IR 6M Basis'     WHEN (a.curve_name like '%_12m') THEN 'IR 12M Basis'     WHEN (a.curve_name like 'fx%') THEN 'CCY Basis'     ELSE 'Other Basis' END AS curve_type,     CASE      WHEN a.curve_name IN ('usdn_disc', 'usdn_3m', 'usdnw_3m','usdnw_disc') THEN 'USD OIS Basis'      WHEN a.curve_name IN ('chfnw_3m') THEN 'CHF OIS Basis'     WHEN a.curve_name IN ('jpynw_3m','jpymu_3m') THEN 'JPY OIS Basis'      WHEN a.curve_name IN ('audn_disc','audnw_3m','audnw_disc') THEN 'AUD OIS Basis'     WHEN a.curve_name IN ('eurnw_disc') THEN 'EUR OIS Basis'      WHEN a.curve_name IN ('gbpnw_3m','gbpnw_disc') THEN 'GBP OIS Basis'         WHEN a.curve_name IN ('seknw_3m') THEN 'SEK OIS Basis'         WHEN a.curve_name IN ('cadnw_disc') THEN 'CAD OIS Basis'      WHEN a.curve_name IN ('nzdnw_3m') THEN 'NZD OIS Basis'      WHEN a.curve_name IN ('usdn_1m','usdnw_1m') THEN 'USD 1M Basis'          WHEN a.curve_name IN ('usdnw_6m','usdn_6m') THEN 'USD 6M Basis'             WHEN a.curve_name IN ('usdnw_12m','usdn_12m') THEN 'USD 12M Basis'     WHEN a.curve_name = 'chfnw_1m' THEN 'CHF 1M Basis'     WHEN a.curve_name = 'chfnw_6m' THEN 'CHF 6M Basis'     WHEN a.curve_name = 'chfnw_12m' THEN 'CHF 12M Basis'     WHEN a.curve_name = 'jpynw_1m' THEN 'JPY 1M Basis'             WHEN a.curve_name IN ('jpynw_6m','jpymu_6m') THEN 'JPY 6M Basis'         WHEN a.curve_name IN ('jpymu_12m','jpynw_12m') THEN 'JPY 12M Basis'     WHEN a.curve_name IN ('audn_1m','audnw_1m') THEN 'AUD 1M Basis'     WHEN a.curve_name IN ('audn_6m','audnw_6m') THEN 'AUD 6M Basis'     WHEN a.curve_name = 'eurnw_1m' THEN 'EUR 1M Basis'      WHEN a.curve_name = 'eurnw_6m' THEN 'EUR 6M Basis'       WHEN a.curve_name = 'eurnw_12m' THEN 'EUR 12M Basis'            WHEN a.curve_name = 'gbpnw_1m' THEN 'GBP 1M Basis'       WHEN a.curve_name = 'gbpnw_6m' THEN 'GBP 6M Basis'      WHEN a.curve_name = 'gbpnw_12m' THEN 'GBP 12M Basis'     WHEN a.curve_name = 'dkk2w_6m' THEN 'DKK 6M Basis'     WHEN a.curve_name = 'cadnw_1m' THEN 'CAD 1M Basis'     WHEN (a.curve_name like '%_disc' or a.curve_name like '%_3m')THEN 'Other OIS Basis'     WHEN (a.curve_name like '%_1m') THEN 'Other 1M Basis'     WHEN (a.curve_name like '%_6m') THEN 'Other 6M Basis'     WHEN (a.curve_name like '%_12m') THEN 'Other 12M Basis'     WHEN a.curve_name = 'fxusdgbp' THEN 'GBP CCY Basis'      WHEN a.curve_name = 'fxusdsek' THEN 'SEK CCY Basis'      WHEN a.curve_name = 'fxusdaud' THEN 'AUD CCY Basis'      WHEN a.curve_name = 'fxusdkrw' THEN 'KRW CCY Basis'      WHEN a.curve_name = 'fxusdclp' THEN 'CLP CCY Basis'      WHEN a.curve_name = 'fxusdcad' THEN 'CAD CCY Basis'      WHEN a.curve_name = 'fxusdchf' THEN 'CHF CCY Basis'      WHEN a.curve_name = 'fxusdjpy' THEN 'JPY CCY Basis'      WHEN a.curve_name = 'fxusdeur' THEN 'EUR CCY Basis'      WHEN a.curve_name like 'fx%' THEN 'Other CCY Basis'     ELSE 'Other' END AS basis_type,     CASE      WHEN a.PRODUCT_SUB_TYPE_CODE IN ('CTDVA') THEN 'CTDVA'     WHEN a.PRODUCT_SUB_TYPE_CODE IN ('LVA') THEN 'LVA'     WHEN PRODUCT_SUB_TYPE_CODE IN ('MPE_FVA_RAW', 'MPE_FVA') THEN 'MPE FVA'     WHEN PRODUCT_SUB_TYPE_CODE IN ('MNE_FVA', 'MNE_FVA_NET') THEN 'MNE FVA'     WHEN a.PRODUCT_SUB_TYPE_CODE IN ('MPE_CVA', 'MPE', 'MPE_PROXY', 'MNE_CP') THEN 'MPE CVA'     WHEN PRODUCT_SUB_TYPE_CODE IN ('MNE_CVA', 'MNE') THEN 'MNE CVA'     ELSE 'Hedge' END AS TYPE_FLAG,     SUM (a.USD_IRPV01SPRD) AS pv01 FROM cdwuser.U_IR_MSR_INTRPLT a WHERE (a.COB_DATE = '2018-02-28' or a.COB_DATE = '2018-02-27') AND      (a.CCC_BUSINESS_AREA IN ('CPM TRADING (MPE)','CPM', 'CREDIT', 'MS CVA MNE - FID', 'MS CVA MNE - COMMOD') OR      a.CCC_STRATEGY IN ('MS CVA MPE - DERIVATIVES', 'MS CVA MNE - DERIVATIVES','EQ XVA HEDGING')) AND      a.USD_IRPV01SPRD IS NOT NULL GROUP BY     a.COB_DATE, a.CCC_PRODUCT_LINE,     CASE WHEN term_new <= 1.5 THEN '0-1YR' WHEN     term_new > 1.5 AND     term_new <= 6.5     THEN '2-6YR' WHEN     term_new > 6.5 AND     term_new <= 13.5     THEN '7-14YR' WHEN     term_new > 13.5 AND     term_new <= 30.5     THEN '15-30YR' WHEN term_new > 30.5 THEN '30Yr+'     ELSE 'NULL' END,     CASE      WHEN (a.curve_name like '%_disc' or a.curve_name like '%_3m')THEN 'IR OIS Basis'     WHEN (a.curve_name like '%_1m') THEN 'IR 1M Basis'     WHEN (a.curve_name like '%_6m') THEN 'IR 6M Basis'     WHEN (a.curve_name like '%_12m') THEN 'IR 12M Basis'     WHEN (a.curve_name like 'fx%') THEN 'CCY Basis'     ELSE 'Other Basis' END,     CASE      WHEN a.curve_name IN ('usdn_disc', 'usdn_3m', 'usdnw_3m','usdnw_disc') THEN 'USD OIS Basis'      WHEN a.curve_name IN ('chfnw_3m') THEN 'CHF OIS Basis'     WHEN a.curve_name IN ('jpynw_3m','jpymu_3m') THEN 'JPY OIS Basis'      WHEN a.curve_name IN ('audn_disc','audnw_3m','audnw_disc') THEN 'AUD OIS Basis'     WHEN a.curve_name IN ('eurnw_disc') THEN 'EUR OIS Basis'      WHEN a.curve_name IN ('gbpnw_3m','gbpnw_disc') THEN 'GBP OIS Basis'         WHEN a.curve_name IN ('seknw_3m') THEN 'SEK OIS Basis'         WHEN a.curve_name IN ('cadnw_disc') THEN 'CAD OIS Basis'      WHEN a.curve_name IN ('nzdnw_3m') THEN 'NZD OIS Basis'      WHEN a.curve_name IN ('usdn_1m','usdnw_1m') THEN 'USD 1M Basis'          WHEN a.curve_name IN ('usdnw_6m','usdn_6m') THEN 'USD 6M Basis'             WHEN a.curve_name IN ('usdnw_12m','usdn_12m') THEN 'USD 12M Basis'     WHEN a.curve_name = 'chfnw_1m' THEN 'CHF 1M Basis'     WHEN a.curve_name = 'chfnw_6m' THEN 'CHF 6M Basis'     WHEN a.curve_name = 'chfnw_12m' THEN 'CHF 12M Basis'     WHEN a.curve_name = 'jpynw_1m' THEN 'JPY 1M Basis'             WHEN a.curve_name IN ('jpynw_6m','jpymu_6m') THEN 'JPY 6M Basis'         WHEN a.curve_name IN ('jpymu_12m','jpynw_12m') THEN 'JPY 12M Basis'     WHEN a.curve_name IN ('audn_1m','audnw_1m') THEN 'AUD 1M Basis'     WHEN a.curve_name IN ('audn_6m','audnw_6m') THEN 'AUD 6M Basis'     WHEN a.curve_name = 'eurnw_1m' THEN 'EUR 1M Basis'      WHEN a.curve_name = 'eurnw_6m' THEN 'EUR 6M Basis'       WHEN a.curve_name = 'eurnw_12m' THEN 'EUR 12M Basis'            WHEN a.curve_name = 'gbpnw_1m' THEN 'GBP 1M Basis'       WHEN a.curve_name = 'gbpnw_6m' THEN 'GBP 6M Basis'      WHEN a.curve_name = 'gbpnw_12m' THEN 'GBP 12M Basis'     WHEN a.curve_name = 'dkk2w_6m' THEN 'DKK 6M Basis'     WHEN a.curve_name = 'cadnw_1m' THEN 'CAD 1M Basis'     WHEN (a.curve_name like '%_disc' or a.curve_name like '%_3m')THEN 'Other OIS Basis'     WHEN (a.curve_name like '%_1m') THEN 'Other 1M Basis'     WHEN (a.curve_name like '%_6m') THEN 'Other 6M Basis'     WHEN (a.curve_name like '%_12m') THEN 'Other 12M Basis'     WHEN a.curve_name = 'fxusdgbp' THEN 'GBP CCY Basis'      WHEN a.curve_name = 'fxusdsek' THEN 'SEK CCY Basis'      WHEN a.curve_name = 'fxusdaud' THEN 'AUD CCY Basis'      WHEN a.curve_name = 'fxusdkrw' THEN 'KRW CCY Basis'      WHEN a.curve_name = 'fxusdclp' THEN 'CLP CCY Basis'      WHEN a.curve_name = 'fxusdcad' THEN 'CAD CCY Basis'      WHEN a.curve_name = 'fxusdchf' THEN 'CHF CCY Basis'      WHEN a.curve_name = 'fxusdjpy' THEN 'JPY CCY Basis'      WHEN a.curve_name = 'fxusdeur' THEN 'EUR CCY Basis'      WHEN a.curve_name like 'fx%' THEN 'Other CCY Basis'     ELSE 'Other' END,     CASE      WHEN a.PRODUCT_SUB_TYPE_CODE IN ('CTDVA') THEN 'CTDVA'     WHEN a.PRODUCT_SUB_TYPE_CODE IN ('LVA') THEN 'LVA'     WHEN PRODUCT_SUB_TYPE_CODE IN ('MPE_FVA_RAW', 'MPE_FVA') THEN 'MPE FVA'     WHEN PRODUCT_SUB_TYPE_CODE IN ('MNE_FVA', 'MNE_FVA_NET') THEN 'MNE FVA'     WHEN a.PRODUCT_SUB_TYPE_CODE IN ('MPE_CVA', 'MPE', 'MPE_PROXY', 'MNE_CP') THEN 'MPE CVA'     WHEN PRODUCT_SUB_TYPE_CODE IN ('MNE_CVA', 'MNE') THEN 'MNE CVA'     ELSE 'Hedge'     END