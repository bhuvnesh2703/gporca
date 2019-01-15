SELECT     I.COB_DATE,     I.BASEL_III_GLOBAL_BTI,     I.VERTICAL_SYSTEM,     I.BOOK,     I.CCC_BUSINESS_AREA,     I.CCC_DIVISION,     I.CCC_PRODUCT_LINE,     I.PRODUCT_TYPE_NAME,     I.PRODUCT_TYPE_CODE,     I.CURRENCY_OF_MEASURE,     I.PRODUCT_HIERARCHY_LEVEL7,     I.ACCOUNT,     I.TICKET, case when (i.VERTICAL_SYSTEM like '%C1%' and i.BOOK in ('TDEBT','TDET2','TDET5','TDET7','TDET8','TDCLH') and i.COB_DATE in ('2018-02-28') ) then 0 when (i.VERTICAL_SYSTEM like '%C1%' and i.BOOK in ('TDEBT','TDET2','TDET5','TDET7','TDET8','TDCLH') and i.COB_DATE  IN  ('2018-01-31')) then 0  else sum(i.USD_IR_UNIFIED_PV01) end AS USD_IR_UNIFIED_PV01,     CASE         WHEN            CCC_DIVISION IN ('FIC DVA','FID DVA') OR CCC_BUSINESS_AREA = 'OTHER IED' THEN CASE WHEN CCC_BOOK_DETAIL LIKE '%FRN%' THEN 'DVA OTHER' WHEN CCC_BOOK_DETAIL LIKE '%BOND%' THEN 'DVA BOND' WHEN CCC_BOOK_DETAIL LIKE '%SWAP%' THEN 'DVA SWAP' WHEN CCC_BOOK_DETAIL LIKE '%LOAN%' THEN 'DVA BOND' ELSE 'DVA OTHER' END when book in ('EULT'  , 'RPOTO' , 'NYDI' , 'CT0101LIQ1' , 'TSTRP' , 'RPOAF' , 'RPOTP' , 'RPOTU' , 'TLGPP' , 'TSTHK') then 'Other Liabilities' when book in ('TSTJY') then 'Other Assets' when book in ('TSTLN') then 'Other Liabilities' when book in ('TSTNY') then 'Other Assets' when book in ('TSTCY') then 'Other Hedges' when book in ('TSTTK' , 'RWULT' , 'NYTU' , 'EUG7' , 'EUC5' , 'EUIP' , 'EUIS' , 'EUIF' , 'EUPS' , 'EUTF' , 'INDMF') then 'Other Liabilities' when book in ('LIABB') then 'Other Assets' when book in ('TSBR1' , 'RPOCO' , 'RPOEQ' , 'RWILX' , 'RWIKX' , 'TBDAP') then 'Other Liabilities' when book in ('TNSAP' , 'TNSLN' , 'TNSNY') then 'Other Assets' when book in ('TSTDP' , 'TSTTO' , 'RWUIS' , 'RWUIP') then 'Other Liabilities' when book in ('TMSSC') then 'Other Assets' when book in ('NYN2' , 'NYCO' , 'RPOCT' , 'TBSNY' , 'TTSNY' , 'RPOHF' , 'RPOHH' , 'TBSHK' , 'EUFA' , 'EUGY' , 'EUKK' , 'EUUK' , 'PROJECT KARL' , 'TBSAP' , 'TBSLN') then 'Other Liabilities' when book in ('TBSSG' , 'TDPSD') then 'Other Assets' when book in ('TDPSG' , 'TSAGD' , 'TSBAG' , 'TZUMD' , 'RWUUK' , 'EUFA' , 'RWUFA' , 'EUPB' , 'EUO1' , 'RWUPB' , 'RWPO1' , 'RWPO2' , 'RWPO3') then 'Other Liabilities' when book in ('MSBKD') then 'Other Assets' when book in ('RWIBK' , 'China Bank Capital on Treasury 2 - CHCA2-CHCA2' , 'CHINA BANK CAPITAL ON TSY-CHCAP') then 'Other Liabilities' when book in ('TBICD') then 'Other Assets' when book in ('RWBIC' , 'TSGSP') then 'Other Liabilities' when book in ('TSTPM') then 'Non-Bank Investment Portfolios' when book in ('CTFTB') then 'Other Assets' when book in ('MMPDS' , 'TSTMM' , 'EUHT' , 'LX' , 'NYKG' , 'NYKI' , 'NYN1' , 'RPOHE' , 'RPOKE' , 'TNYSL') then 'Other Liabilities' when book in ('TAPRZ' , 'TAPTY') then 'Other Assets' when book in ('TATUY' , 'TAPTZ' , 'TAPUZ' , 'TAPSZ') then 'Other Liabilities' when book in ('TAPUY') then 'Other Assets' when book in ('RPOHB' , 'RPOHG' , 'AEOUT' , 'BCDUT' , 'VNCDB' , 'UTCDS' , 'STRUCTURED CD' , 'MSDEP' , 'TAPBL' , 'TLNBL' , 'TSAGL' , 'TSGBL' , 'TZUML' , 'TBICL' , 'TNYBL' , 'TAPCP' , 'TNYCP') then 'Other Liabilities' when book in ('CFXEU' , 'CFXNA' , 'CT0101SWP' , 'CT0101SWP-AP' , 'CT0101SWP-LN' , 'CT0101SWP-TK' , 'CT0302SWP' , 'TLNFX' , 'TOMFS' , 'TSGFX' , 'TZUFX' , 'KRSWP' , 'CT1633SWP' , 'CT0362SWP' , 'CT0870SWP' , 'THKFX' , 'TICIP' , 'TICMS' , 'CT7713SWP' , 'CT7714SWP' , 'CT7715SWP') then 'Other Hedges' when book in ('EUGS' , 'TAPSS' , 'TLNSS') then 'Other Liabilities' when book in ('CT0302XE' , 'CT0101XE') then 'Other Hedges' when book in ('CAEMR' , 'TKFVL' , 'TAEMR' , 'CAIRR' , 'TAIRR' , 'CBAGR' , 'TBAGR' , 'CEEMR' , 'TEEMR' , 'COMSN' , 'CSCPR' , 'CTHKR' , 'CTIRR' , 'TTIRR' , 'CTNUR' , 'CTTKR' , 'TSYFE' , 'TSYRT' , 'TKFVS') then 'Other Liabilities' when book in ('CALLB' , 'CALTB') then 'Other Hedges' when book in ('LCALT' , 'LCATB' , 'TTIRP' , 'TSYCALL' , 'TSYCSNLN' , 'CTCNR' , 'MSMSR' , 'HKFAM' , 'HKFAP' , 'HKFAS' , 'ASIA ELN SWAPS' , 'ELNLT' , 'NBFCE' , 'LN ELNLT' , 'LNFVO' , 'ASIA ELNLT' , 'MS DVA STRUCTURED NO' , 'MS STRUCTURED NOTES' , 'ISSUANCE FUNDING' , 'TCM_TNY' , 'CLIENT STRUCTURES' , 'EQTRE' , 'FVONY' , 'LN_ELNLT' , 'NY_ELNLT' , 'AP_CALLABLES' , 'MSF_CALLABLES' , 'MSF_CALLABLES_BRM' , 'NY_CALLABLES' , 'NY_CALLABLES_BRM' , 'AEOEQ' , 'EQGRP') then 'Other Liabilities' when book in ('TDEBT' , 'LHRO1' , 'LHRO2') then 'ASC 815 Debt' when book in ('SWPIN' , 'SWPLH' , 'SUBIN' , 'SUBLH' , 'SWCOH' , 'SWCLH') then 'ASC 815 Hedges' when book in ('TDET4') then 'Other Hedges' when book in ('TDET7' , 'TDET8') then 'Other Liabilities' when book in ('AEO' , 'AEOCT' , 'AEOFF' , 'AEOTK' , 'SWPTR' , 'SUBTR' , 'TSTON') then 'Other Hedges' when book in ('TDET5') then 'Other Liabilities' when book in ('AEONE' , 'AEONF') then 'Other Hedges' when book in ('AEOST') then 'Other Liabilities' when book in ('TAPOZ' , 'TST31' , 'TSTOH' , 'TST3F' , 'TSTCH' , 'TSTGB' , 'TSTOF' , 'TSTOL') then 'Other Hedges' when book in ('TDET2' , 'TDET6' , 'TRPFD') then 'Other Liabilities' when book in ('PRFEQ') then 'Preferred Stock' when book in ('CT0201SPT' , 'CT0201SPT2' , 'FXNYDFLTC' , 'RVKRW' , 'TOMCP' , 'TSYFX' , 'TYBRL' , 'TYCNY' , 'TYHDG' , 'TYKRW' , 'TYTWD' , 'TYWON' , 'HNKRW' , 'TSRUB' , 'CT1633SPT' , 'TYBIC' , 'CT0101SPT' , 'GLFX1' , 'ROBIC' , 'CT0302SPT' , 'CT0101SPT6262' , 'CT0101FWD' , 'CT0101FWDP' , 'TYSJV') then 'Other Hedges' when book in ('CTA_PRE_TAX') then 'Other Liabilities' when book in ('TSTEU') then 'Other Hedges' when book in ('AEOAP') then 'Other Liabilities' when book in ('TSFRA') then 'Other Hedges' when book in ('TKFVM') then 'Other Liabilities' when book in ('TSHTM') then 'Non-Bank Investment Portfolios' when book in ('TSTSB') then 'Other Hedges' when book in ('FPDFX' , 'FTDFX' , 'PFDFX') then 'Other Liabilities' when book in ('CT101GAAP') then 'Other Hedges' when book in ('LN_MSMS' , 'TCM_BRL' , 'TCM_MSF' , 'TCM_MSF_CALL' , 'TCM_MSF_CALL_BRM' , 'TK_MSMS' , 'TMSFH' , 'TMSFL' , 'TMSFN' , 'TMSFT') then 'Other Liabilities' when book in ('UNKNOWN') then 'Other Hedges' when book in ('CMSFT' , 'CSTNT') then 'Other Liabilities' when book in ('SWOLH' , 'SUOLH') then 'ASC 815 Hedges' when book in ('SWOTR' , 'SUOTR') then 'Other Hedges' when book in ('SWOIN' , 'SUOIN') then 'ASC 815 Hedges' when book in ('NY_TREASURY_STRUCTUR' , 'TAPZU') then 'Other Liabilities' when book in ('CTA0101XE' , 'CTA0517XE') then 'Other Hedges' when book in ('RPOGI' , 'RPOSE' , 'TYGAS' , 'VNCDP') then 'Other Liabilities' when book in ('CVHL1' , 'CVHN1') then 'Other Hedges' when book in ('TDCLH') then 'ASC 815 Debt' when book in ('SWCLH' , 'SWCOH') then 'ASC 815 Hedges' when book in ('LCALB' , 'LACAB' , 'TCATB' , 'TKCAL') then 'Other Hedges' when book in ('TAPTH' , 'TAPUH' , 'STSTTO') then 'Other Assets'         ELSE 'Other Liabilities'     END AS      PRODUCT_GROUP,      0 AS USD_MARKET_VALUE,      0 AS USD_NOTIONAL FROM      DWUSER.U_IR_MSR I where COB_DATE in ('2018-02-28', '2018-01-31') AND          BASEL_III_GLOBAL_BTI <> 'TRADING' AND     i.CCC_TAPS_COMPANY NOT IN ('1633', '6635') AND     USD_IR_UNIFIED_PV01 IS NOT NULL AND     USD_IR_UNIFIED_PV01 <> 0 AND     ccc_banking_trading = 'BANKING' AND     ccc_business_area NOT IN ('LIQUID FLOW RATES', 'MUNICIPAL SECURITIES', 'OLYMPUS', 'SECURITIZED PRODUCTS GRP') AND     ccc_strategy NOT IN ('CPM FUNDING','XVA CREDIT', 'MS CVA MNE - DERIVATIVES', 'MS CVA MPE - DERIVATIVES','XVA FUNDING') AND product_type_code not in ('FVA','CVA') AND     ((I.CCC_STRATEGY <> 'STRUCTURED N' OR BOOK IN ('TSYCSNLN','CALTB','LCATB','CALFB','MSF_CALLABLES_BRM','NY_CALLABLES_BRM','TCM_MSF_CALL_BRM'))      AND (CCC_DIVISION NOT IN ('FIC DVA', 'FID DVA') OR CCC_BOOK_DETAIL NOT LIKE '%FRN%')) AND     (ccc_division NOT IN ('INSTITUTIONAL EQUITY DIVISION') OR      account NOT IN ('077002SS3', '077002SS', '0770001V9', '0770001V', '074008T03', '074008T0', '072006G45', '072006FS3', '072006G4',                           '072006FS', '072005UD1', '072005UD', '072004X82', '072004X8', '072000ET5', '072000ET', '071004EY7', '071004EY',                           '07800A971', '07800A97', '07700AGS8', '077000BB2', '07700NFD', '07700AGR', '07700AGG4', '07700AGG', '07700AGR0'          , '07700AG3', '07700AGP4', '07700NFE2', '07700AGS', '07700NFE', '077000BB', '07700AGH', '07700AGP', '07700AGJ8', '07700AGJ',                           '07700AG58', '07700AG41', '07700AG33', '07700AG5', '07700AG4', '07700A1Y1', '07700A1Y', '07700NFD4',                           '07700AGH2', '07400XMD7', '07400XMD', '07400XM54', '07400XM5', '07400XK64', '07400XK07', '07400XK6', '07400XK0'          , '07200CT89', '07200XA4', '07200CVG', '07200XA42', '07200CT8', '07200NFC1', '07200CVG8', '07200CTT', '07200CA89', '07200BUB',                           '07200CTT3', '07200BUB2', '07200NFC', '07200CA8', '07100CXB'))  GROUP BY     i.COB_DATE,     i.BASEL_III_GLOBAL_BTI,     i.BOOK,     i.VERTICAL_SYSTEM,     i.CCC_BUSINESS_AREA,     i.CCC_DIVISION,     I.CCC_PRODUCT_LINE, I.CCC_BOOK_DETAIL,     i.PRODUCT_TYPE_CODE,     i.PRODUCT_TYPE_NAME,     i.CURRENCY_OF_MEASURE,     i.PRODUCT_HIERARCHY_LEVEL7,     i.ACCOUNT,     i.currency_of_position,     i.TICKET UNION ALL SELECT     I.COB_DATE,     I.BASEL_III_GLOBAL_BTI,     I.VERTICAL_SYSTEM,     I.BOOK,     I.CCC_BUSINESS_AREA,     I.CCC_DIVISION,     I.CCC_PRODUCT_LINE,      I.PRODUCT_TYPE_NAME,     I.PRODUCT_TYPE_CODE,     I.CURRENCY_OF_MEASURE,     I.PRODUCT_HIERARCHY_LEVEL7,     I.ACCOUNT,     I.TICKET, 0 AS USD_IR_UNIFIED_PV01 ,CASE WHEN           CCC_DIVISION IN ('FIC DVA','FID DVA') OR CCC_BUSINESS_AREA = 'OTHER IED' THEN CASE WHEN CCC_BOOK_DETAIL LIKE '%FRN%' THEN 'DVA OTHER' WHEN CCC_BOOK_DETAIL LIKE '%BOND%' THEN 'DVA BOND' WHEN CCC_BOOK_DETAIL LIKE '%SWAP%' THEN 'DVA SWAP' WHEN CCC_BOOK_DETAIL LIKE '%LOAN%' THEN 'DVA BOND' ELSE 'DVA OTHER' END when book in ('EULT'  , 'RPOTO' , 'NYDI' , 'CT0101LIQ1' , 'TSTRP' , 'RPOAF' , 'RPOTP' , 'RPOTU' , 'TLGPP' , 'TSTHK') then 'Other Liabilities' when book in ('TSTJY') then 'Other Assets' when book in ('TSTLN') then 'Other Liabilities' when book in ('TSTNY') then 'Other Assets' when book in ('TSTCY') then 'Other Hedges' when book in ('TSTTK' , 'RWULT' , 'NYTU' , 'EUG7' , 'EUC5' , 'EUIP' , 'EUIS' , 'EUIF' , 'EUPS' , 'EUTF' , 'INDMF') then 'Other Liabilities' when book in ('LIABB') then 'Other Assets' when book in ('TSBR1' , 'RPOCO' , 'RPOEQ' , 'RWILX' , 'RWIKX' , 'TBDAP') then 'Other Liabilities' when book in ('TNSAP' , 'TNSLN' , 'TNSNY') then 'Other Assets' when book in ('TSTDP' , 'TSTTO' , 'RWUIS' , 'RWUIP') then 'Other Liabilities' when book in ('TMSSC') then 'Other Assets' when book in ('NYN2' , 'NYCO' , 'RPOCT' , 'TBSNY' , 'TTSNY' , 'RPOHF' , 'RPOHH' , 'TBSHK' , 'EUFA' , 'EUGY' , 'EUKK' , 'EUUK' , 'PROJECT KARL' , 'TBSAP' , 'TBSLN') then 'Other Liabilities' when book in ('TBSSG' , 'TDPSD') then 'Other Assets' when book in ('TDPSG' , 'TSAGD' , 'TSBAG' , 'TZUMD' , 'RWUUK' , 'EUFA' , 'RWUFA' , 'EUPB' , 'EUO1' , 'RWUPB' , 'RWPO1' , 'RWPO2' , 'RWPO3') then 'Other Liabilities' when book in ('MSBKD') then 'Other Assets' when book in ('RWIBK' , 'China Bank Capital on Treasury 2 - CHCA2-CHCA2' , 'CHINA BANK CAPITAL ON TSY-CHCAP') then 'Other Liabilities' when book in ('TBICD') then 'Other Assets' when book in ('RWBIC' , 'TSGSP') then 'Other Liabilities' when book in ('TSTPM') then 'Non-Bank Investment Portfolios' when book in ('CTFTB') then 'Other Assets' when book in ('MMPDS' , 'TSTMM' , 'EUHT' , 'LX' , 'NYKG' , 'NYKI' , 'NYN1' , 'RPOHE' , 'RPOKE' , 'TNYSL') then 'Other Liabilities' when book in ('TAPRZ' , 'TAPTY') then 'Other Assets' when book in ('TATUY' , 'TAPTZ' , 'TAPUZ' , 'TAPSZ') then 'Other Liabilities' when book in ('TAPUY') then 'Other Assets' when book in ('RPOHB' , 'RPOHG' , 'AEOUT' , 'BCDUT' , 'VNCDB' , 'UTCDS' , 'STRUCTURED CD' , 'MSDEP' , 'TAPBL' , 'TLNBL' , 'TSAGL' , 'TSGBL' , 'TZUML' , 'TBICL' , 'TNYBL' , 'TAPCP' , 'TNYCP') then 'Other Liabilities' when book in ('CFXEU' , 'CFXNA' , 'CT0101SWP' , 'CT0101SWP-AP' , 'CT0101SWP-LN' , 'CT0101SWP-TK' , 'CT0302SWP' , 'TLNFX' , 'TOMFS' , 'TSGFX' , 'TZUFX' , 'KRSWP' , 'CT1633SWP' , 'CT0362SWP' , 'CT0870SWP' , 'THKFX' , 'TICIP' , 'TICMS' , 'CT7713SWP' , 'CT7714SWP' , 'CT7715SWP') then 'Other Hedges' when book in ('EUGS' , 'TAPSS' , 'TLNSS') then 'Other Liabilities' when book in ('CT0302XE' , 'CT0101XE') then 'Other Hedges' when book in ('CAEMR' , 'TKFVL' , 'TAEMR' , 'CAIRR' , 'TAIRR' , 'CBAGR' , 'TBAGR' , 'CEEMR' , 'TEEMR' , 'COMSN' , 'CSCPR' , 'CTHKR' , 'CTIRR' , 'TTIRR' , 'CTNUR' , 'CTTKR' , 'TSYFE' , 'TSYRT' , 'TKFVS') then 'Other Liabilities' when book in ('CALLB' , 'CALTB') then 'Other Hedges' when book in ('LCALT' , 'LCATB' , 'TTIRP' , 'TSYCALL' , 'TSYCSNLN' , 'CTCNR' , 'MSMSR' , 'HKFAM' , 'HKFAP' , 'HKFAS' , 'ASIA ELN SWAPS' , 'ELNLT' , 'NBFCE' , 'LN ELNLT' , 'LNFVO' , 'ASIA ELNLT' , 'MS DVA STRUCTURED NO' , 'MS STRUCTURED NOTES' , 'ISSUANCE FUNDING' , 'TCM_TNY' , 'CLIENT STRUCTURES' , 'EQTRE' , 'FVONY' , 'LN_ELNLT' , 'NY_ELNLT' , 'AP_CALLABLES' , 'MSF_CALLABLES' , 'MSF_CALLABLES_BRM' , 'NY_CALLABLES' , 'NY_CALLABLES_BRM' , 'AEOEQ' , 'EQGRP') then 'Other Liabilities' when book in ('TDEBT' , 'LHRO1' , 'LHRO2') then 'ASC 815 Debt' when book in ('SWPIN' , 'SWPLH' , 'SUBIN' , 'SUBLH' , 'SWCOH' , 'SWCLH') then 'ASC 815 Hedges' when book in ('TDET4') then 'Other Hedges' when book in ('TDET7' , 'TDET8') then 'Other Liabilities' when book in ('AEO' , 'AEOCT' , 'AEOFF' , 'AEOTK' , 'SWPTR' , 'SUBTR' , 'TSTON') then 'Other Hedges' when book in ('TDET5') then 'Other Liabilities' when book in ('AEONE' , 'AEONF') then 'Other Hedges' when book in ('AEOST') then 'Other Liabilities' when book in ('TAPOZ' , 'TST31' , 'TSTOH' , 'TST3F' , 'TSTCH' , 'TSTGB' , 'TSTOF' , 'TSTOL') then 'Other Hedges' when book in ('TDET2' , 'TDET6' , 'TRPFD') then 'Other Liabilities' when book in ('PRFEQ') then 'Preferred Stock' when book in ('CT0201SPT' , 'CT0201SPT2' , 'FXNYDFLTC' , 'RVKRW' , 'TOMCP' , 'TSYFX' , 'TYBRL' , 'TYCNY' , 'TYHDG' , 'TYKRW' , 'TYTWD' , 'TYWON' , 'HNKRW' , 'TSRUB' , 'CT1633SPT' , 'TYBIC' , 'CT0101SPT' , 'GLFX1' , 'ROBIC' , 'CT0302SPT' , 'CT0101SPT6262' , 'CT0101FWD' , 'CT0101FWDP' , 'TYSJV') then 'Other Hedges' when book in ('CTA_PRE_TAX') then 'Other Liabilities' when book in ('TSTEU') then 'Other Hedges' when book in ('AEOAP') then 'Other Liabilities' when book in ('TSFRA') then 'Other Hedges' when book in ('TKFVM') then 'Other Liabilities' when book in ('TSHTM') then 'Non-Bank Investment Portfolios' when book in ('TSTSB') then 'Other Hedges' when book in ('FPDFX' , 'FTDFX' , 'PFDFX') then 'Other Liabilities' when book in ('CT101GAAP') then 'Other Hedges' when book in ('LN_MSMS' , 'TCM_BRL' , 'TCM_MSF' , 'TCM_MSF_CALL' , 'TCM_MSF_CALL_BRM' , 'TK_MSMS' , 'TMSFH' , 'TMSFL' , 'TMSFN' , 'TMSFT') then 'Other Liabilities' when book in ('UNKNOWN') then 'Other Hedges' when book in ('CMSFT' , 'CSTNT') then 'Other Liabilities' when book in ('SWOLH' , 'SUOLH') then 'ASC 815 Hedges' when book in ('SWOTR' , 'SUOTR') then 'Other Hedges' when book in ('SWOIN' , 'SUOIN') then 'ASC 815 Hedges' when book in ('NY_TREASURY_STRUCTUR' , 'TAPZU') then 'Other Liabilities' when book in ('CTA0101XE' , 'CTA0517XE') then 'Other Hedges' when book in ('RPOGI' , 'RPOSE' , 'TYGAS' , 'VNCDP') then 'Other Liabilities' when book in ('CVHL1' , 'CVHN1') then 'Other Hedges' when book in ('TDCLH') then 'ASC 815 Debt' when book in ('SWCLH' , 'SWCOH') then 'ASC 815 Hedges' when book in ('LCALB' , 'LACAB' , 'TCATB' , 'TKCAL') then 'Other Hedges' when book in ('TAPTH' , 'TAPUH' , 'STSTTO') then 'Other Assets' else 'Other Liabilities' end as product_group, sum(I.USD_MARKET_VALUE)/1000  AS USD_MARKET_VALUE,   sum(I.USD_NOTIONAL)/1000 AS USD_NOTIONAL FROM DWUSER.u_OT_msr i where i.COB_DATE in ('2018-02-28', '2018-01-31') AND          BASEL_III_GLOBAL_BTI <> 'TRADING' AND     i.CCC_TAPS_COMPANY NOT IN ('1633', '6635') AND     ((USD_MARKET_VALUE IS NOT NULL AND USD_MARKET_VALUE <> 0) OR     (USD_NOTIONAL IS NOT NULL AND USD_NOTIONAL <> 0)) AND     ccc_banking_trading = 'BANKING' AND     ccc_business_area NOT IN ('LIQUID FLOW RATES', 'MUNICIPAL SECURITIES', 'OLYMPUS', 'SECURITIZED PRODUCTS GRP') AND     ccc_strategy NOT IN ('CPM FUNDING','XVA CREDIT', 'MS CVA MNE - DERIVATIVES', 'MS CVA MPE - DERIVATIVES','XVA FUNDING') AND product_type_code not in ('FVA','CVA') AND     ((I.CCC_STRATEGY <> 'STRUCTURED N' OR BOOK IN ('TSYCSNLN','CALTB','LCATB','CALFB','MSF_CALLABLES_BRM','NY_CALLABLES_BRM','TCM_MSF_CALL_BRM'))      AND (CCC_DIVISION NOT IN ('FIC DVA', 'FID DVA') OR CCC_BOOK_DETAIL NOT LIKE '%FRN%')) AND     (ccc_division NOT IN ('INSTITUTIONAL EQUITY DIVISION') OR      account NOT IN ('077002SS3', '077002SS', '0770001V9', '0770001V', '074008T03', '074008T0', '072006G45', '072006FS3', '072006G4',                           '072006FS', '072005UD1', '072005UD', '072004X82', '072004X8', '072000ET5', '072000ET', '071004EY7', '071004EY',                           '07800A971', '07800A97', '07700AGS8', '077000BB2', '07700NFD', '07700AGR', '07700AGG4', '07700AGG', '07700AGR0'          , '07700AG3', '07700AGP4', '07700NFE2', '07700AGS', '07700NFE', '077000BB', '07700AGH', '07700AGP', '07700AGJ8', '07700AGJ',                           '07700AG58', '07700AG41', '07700AG33', '07700AG5', '07700AG4', '07700A1Y1', '07700A1Y', '07700NFD4',                           '07700AGH2', '07400XMD7', '07400XMD', '07400XM54', '07400XM5', '07400XK64', '07400XK07', '07400XK6', '07400XK0'          , '07200CT89', '07200XA4', '07200CVG', '07200XA42', '07200CT8', '07200NFC1', '07200CVG8', '07200CTT', '07200CA89', '07200BUB',                           '07200CTT3', '07200BUB2', '07200NFC', '07200CA8', '07100CXB')) GROUP BY     i.COB_DATE,     i.BASEL_III_GLOBAL_BTI,     i.BOOK,     i.VERTICAL_SYSTEM,     i.CCC_BUSINESS_AREA,     i.CCC_DIVISION,     I.CCC_PRODUCT_LINE, I.CCC_BOOK_DETAIL,     i.PRODUCT_TYPE_CODE,     i.PRODUCT_TYPE_NAME,     i.CURRENCY_OF_MEASURE,     i.PRODUCT_HIERARCHY_LEVEL7,     i.ACCOUNT,     i.currency_of_position,     i.TICKET