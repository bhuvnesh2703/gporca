select COB_DATE as "COB DATE", CCC_BUSINESS_AREA as "CCC BUSINESS AREA", STRESS_SCENARIO as "STRESS SCENARIO", substr(currency_pair,1,3) as currency1, substr(currency_pair,4,3) as currency2, currency_pair as "CURRENCY PAIR", sum(SCENARIO_PNL) as "SCENARIO PNL" from ( Select PVT.COB_DATE, PVT.CCC_BUSINESS_AREA, PVT.STRESS_SCENARIO, SUM(case when PRODUCT_TYPE = 'FXOPT' and IS_INCLUDED='YES' then SCENARIO_PNL when PRODUCT_TYPE <> 'FXOPT' and SCENARIO_TYPE = 'GREEK' and ATTRIBUTION<>'FX GAMMA' then SCENARIO_PNL else 0 end) as SCENARIO_PNL, case when strpos(currency_pair,'CNH')>0 then replace(currency_pair,'CNH','CNY') when strpos(currency_pair,'KRX')>0 then replace(currency_pair,'KRX','KRW') when currency_pair = 'AUDJPY' then 'JPYAUD' when currency_pair = 'AUDUSD' then 'USDAUD' when currency_pair = 'BRLCNY' then 'CNYBRL' when currency_pair = 'BRLIDR' then 'IDRBRL' when currency_pair = 'BRLJPY' then 'JPYBRL' when currency_pair = 'BRLMXN' then 'MXNBRL' when currency_pair = 'BRLNOK' then 'NOKBRL' when currency_pair = 'BRLPLN' then 'PLNBRL' when currency_pair = 'BRLRUB' then 'RUBBRL' when currency_pair = 'BRLZAR' then 'ZARBRL' when currency_pair = 'CADJPY' then 'JPYCAD' when currency_pair = 'CHFJPY' then 'JPYCHF' when currency_pair = 'CLPIDR' then 'IDRCLP' when currency_pair = 'CNYRUB' then 'RUBCNY' when currency_pair = 'EURUSD' then 'USDEUR' when currency_pair = 'GBPAUD' then 'AUDGBP' when currency_pair = 'GBPCAD' then 'CADGBP' when currency_pair = 'GBPCHF' then 'CHFGBP' when currency_pair = 'GBPJPY' then 'JPYGBP' when currency_pair = 'GBPUSD' then 'USDGBP' when currency_pair = 'ILSJPY' then 'JPYILS' when currency_pair = 'INRJPY' then 'JPYINR' when currency_pair = 'INRKRW' then 'KRWINR' when currency_pair = 'MXNCNY' then 'CNYMXN' when currency_pair = 'MXNJPY' then 'JPYMXN' when currency_pair = 'MYRCNY' then 'CNYMYR' when currency_pair = 'MYRINR' then 'INRMYR' when currency_pair = 'MYRJPY' then 'JPYMYR' when currency_pair = 'MYRKRW' then 'KRWMYR' when currency_pair = 'MYRPHP' then 'PHPMYR' when currency_pair = 'MYRTWD' then 'TWDMYR' when currency_pair = 'NOKJPY' then 'JPYNOK' when currency_pair = 'NZDCAD' then 'CADNZD' when currency_pair = 'NZDCHF' then 'CHFNZD' when currency_pair = 'NZDJPY' then 'JPYNZD' when currency_pair = 'NZDNOK' then 'NOKNZD' when currency_pair = 'NZDUSD' then 'USDNZD' when currency_pair = 'PLNJPY' then 'JPYPLN' when currency_pair = 'PLNSEK' then 'SEKPLN' when currency_pair = 'RUBINR' then 'INRRUB' when currency_pair = 'RUBKRW' then 'KRWRUB' when currency_pair = 'RUBMXN' then 'MXNRUB' when currency_pair = 'RUBPLN' then 'PLNRUB' when currency_pair = 'SEKJPY' then 'JPYSEK' when currency_pair = 'SGDHKD' then 'HKDSGD' when currency_pair = 'SGDJPY' then 'JPYSGD' when currency_pair = 'SGDMXN' then 'MXNSGD' when currency_pair = 'TRYJPY' then 'JPYTRY' when currency_pair = 'TRYMXN' then 'MXNTRY' when currency_pair = 'TRYNOK' then 'NOKTRY' when currency_pair = 'TWDJPY' then 'JPYTWD' when currency_pair = 'UBDBRD' then 'BRDBRL' when currency_pair = 'UBDBRX' then 'BRXBRL' when currency_pair = 'USDTRL' then 'TRLTRY' when currency_pair = 'ZARJPY' then 'JPYZAR' else currency_pair end as currency_pair from dwuser.U_MODULAR_SCENARIOS PVT where PVT.COB_DATE in('2018-02-28','2018-02-21') and PVT.RUN_Profile = 'FXOPT_MOD_SCN_RUN' and PVT.CCC_BUSINESS_AREA = 'FXEM MACRO TRADING' and exclude_code = 'Not Applicable' and book not in ('BASKET', 'BASKET HEDGES', 'CEEMEA MULTICCY', 'CORRELATION SWAP', 'CORRELATION SWAP 2', 'CORRELATION SWAP 3', 'CORRELATION SWAP 4', 'DUAL CURRENCY', 'MULTICCY SPEC') and PRODUCT_TYPE <> 'FXBSKT' and stress_scenario like 'FX%' and CCC_TAPS_COMPANY IN ('7800','4068','8962','8961','8959','8941','8790','8789','8772','8757','8627','8564',
 '8537','8524','8441','8292','8290','8284','8275','8253','8237','8179','8174','7716','7705','7704','7458','7435','7416',
 '7281','7280','7043','7016','6899','6893','6838','6837','6590','6589','6384','6383','6376','6374','6367','6325','6316','6262',
 '6158','6157','6120','6114','6036','5869','5856','5656','5614','5357','5310','5274','5254','5181','5180','5148','5121','5104',
 '5103','4884','4880','4876','4863','4859','4858','4857','4590','4564','4562','4545','4543','4536','4391','4267','4241','4092',
 '4086','4067','4044','4043','1718','1709','1498','1480','1472','1438','1433','1344','1322','1317','1314','1313','1311','1308','0993',
 '0856','0853','0726','0721','0715','0713','0621','0620','0517','0347','0342','0328','0319','0314','0313','0302') GROUP BY PVT.COB_DATE, PVT.CCC_BUSINESS_AREA, PVT.STRESS_SCENARIO, case when strpos(currency_pair,'CNH')>0 then replace(currency_pair,'CNH','CNY') when strpos(currency_pair,'KRX')>0 then replace(currency_pair,'KRX','KRW') when currency_pair = 'AUDJPY' then 'JPYAUD' when currency_pair = 'AUDUSD' then 'USDAUD' when currency_pair = 'BRLCNY' then 'CNYBRL' when currency_pair = 'BRLIDR' then 'IDRBRL' when currency_pair = 'BRLJPY' then 'JPYBRL' when currency_pair = 'BRLMXN' then 'MXNBRL' when currency_pair = 'BRLNOK' then 'NOKBRL' when currency_pair = 'BRLPLN' then 'PLNBRL' when currency_pair = 'BRLRUB' then 'RUBBRL' when currency_pair = 'BRLZAR' then 'ZARBRL' when currency_pair = 'CADJPY' then 'JPYCAD' when currency_pair = 'CHFJPY' then 'JPYCHF' when currency_pair = 'CLPIDR' then 'IDRCLP' when currency_pair = 'CNYRUB' then 'RUBCNY' when currency_pair = 'EURUSD' then 'USDEUR' when currency_pair = 'GBPAUD' then 'AUDGBP' when currency_pair = 'GBPCAD' then 'CADGBP' when currency_pair = 'GBPCHF' then 'CHFGBP' when currency_pair = 'GBPJPY' then 'JPYGBP' when currency_pair = 'GBPUSD' then 'USDGBP' when currency_pair = 'ILSJPY' then 'JPYILS' when currency_pair = 'INRJPY' then 'JPYINR' when currency_pair = 'INRKRW' then 'KRWINR' when currency_pair = 'MXNCNY' then 'CNYMXN' when currency_pair = 'MXNJPY' then 'JPYMXN' when currency_pair = 'MYRCNY' then 'CNYMYR' when currency_pair = 'MYRINR' then 'INRMYR' when currency_pair = 'MYRJPY' then 'JPYMYR' when currency_pair = 'MYRKRW' then 'KRWMYR' when currency_pair = 'MYRPHP' then 'PHPMYR' when currency_pair = 'MYRTWD' then 'TWDMYR' when currency_pair = 'NOKJPY' then 'JPYNOK' when currency_pair = 'NZDCAD' then 'CADNZD' when currency_pair = 'NZDCHF' then 'CHFNZD' when currency_pair = 'NZDJPY' then 'JPYNZD' when currency_pair = 'NZDNOK' then 'NOKNZD' when currency_pair = 'NZDUSD' then 'USDNZD' when currency_pair = 'PLNJPY' then 'JPYPLN' when currency_pair = 'PLNSEK' then 'SEKPLN' when currency_pair = 'RUBINR' then 'INRRUB' when currency_pair = 'RUBKRW' then 'KRWRUB' when currency_pair = 'RUBMXN' then 'MXNRUB' when currency_pair = 'RUBPLN' then 'PLNRUB' when currency_pair = 'SEKJPY' then 'JPYSEK' when currency_pair = 'SGDHKD' then 'HKDSGD' when currency_pair = 'SGDJPY' then 'JPYSGD' when currency_pair = 'SGDMXN' then 'MXNSGD' when currency_pair = 'TRYJPY' then 'JPYTRY' when currency_pair = 'TRYMXN' then 'MXNTRY' when currency_pair = 'TRYNOK' then 'NOKTRY' when currency_pair = 'TWDJPY' then 'JPYTWD' when currency_pair = 'UBDBRD' then 'BRDBRL' when currency_pair = 'UBDBRX' then 'BRXBRL' when currency_pair = 'USDTRL' then 'TRLTRY' when currency_pair = 'ZARJPY' then 'JPYZAR' else currency_pair end) a group by COB_DATE, CCC_BUSINESS_AREA, STRESS_SCENARIO, substr(currency_pair,1,3) , substr(currency_pair,4,3), currency_pair