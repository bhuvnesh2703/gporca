select COB_DATE, BOOK, CURRENCY_MAPPING, DERIVATIVE_BREAKDOWN, BANK_FLAG, PH7_MAPPING, SUM (COALESCE(CASE WHEN (PRODUCT_HIERARCHY_LEVEL7 IN ('FX_SPOT/FORWARDS', 'IR_SWAP_CROSSCURRENCY') OR (PRODUCT_HIERARCHY_LEVEL7 ='UNKNOWN' AND PRODUCT_TYPE_CODE='FX')) then USD_MARKET_VALUE WHEN book in('SWPIN','TDET6', 'TAIRR', 'TAEMR', 'TEEMR', 'HKFAP', 'TTIRR', 'HKFAM', 'TKFVS', 'HKFAS', 'TBAGR') THEN -usd_notional WHEN (book in('CALLT', 'CALTB', 'LCALT', 'LCATB')) THEN usd_MARKET_VALUE else usd_notional END, 0)) AS USD_NOTIONAL FROM cdwuser.U_DM_TREASURY WHERE cob_date in ( '2018-02-28', '2018-02-21' ) and ccc_division = 'TREASURY CAPITAL MARKETS' AND CCC_BUSINESS_AREA = 'LIQUIDITY RESERVE1' AND PRODUCT_TYPE_CODE NOT IN ('REPO', 'CASH', 'SWAP', 'MMF') and account not in ('070002D15', '07700A931', '070002D23', '07200B9V2', '07000AC59', '07000AC67', '07000AC75', '07000ACY6', '070002DZ0', '07000AC18', '07000AC26', '07000AC42', '07000ACZ3', '070002DZ1', '07000AC34', '07700EKT3', '07200B9V2') and not (currency_of_measure='BRL' and PRODUCT_HIERARCHY_LEVEL7='CORPORATE-CERTIFICATE OF DEPOSIT') and ACCOUNT NOT IN ('07000AB27') AND CCC_HIERARCHY_LEVEL10 <> ('INELIGIBLE SEC POOL') AND CCC_HIERARCHY_LEVEL10 NOT LIKE ('%INELIG%') GROUP BY COB_DATE, BOOK, CURRENCY_MAPPING, DERIVATIVE_BREAKDOWN, BANK_FLAG, PH7_MAPPING