SELECT  PRODUCT_TYPE,        ctpy_credit_ultimate_name,             source_scn_name,SUM(M_PNL_current) FROM      (SELECT        ctpy_credit_ultimate_name,         PRODUCT_SUBTYPE,         PRODUCT_TYPE,         book,         SOURCE_SCN_NAME,         ccc_strategy,        SUM( case when  strpos(source_scn_name,'50PCT') > 0 then RAW_PNL*2 else RAW_PNL end)  as M_PNL_current     from DWUSER.U_RAW_SCENARIO_PNL a      WHERE cob_date = '2018-02-28' and         (PRODUCT_SUBTYPE like '%MPE%' or PRODUCT_SUBTYPE like '%MNE%') AND         source_scn_name  in ( 'CESTRESS_Z2WK_USD_IR_UP3SD','CESTRESS_Z2WK_USD_IR_DOWN3SD', 'CESTRESS_Z2WK_EUR_FX_UP3SD','CESTRESS_Z2WK_EUR_FX_DOWN3SD', 'CESTRESS_Z2WK_EUR_IR_UP3SD','CESTRESS_Z2WK_EUR_IR_DOWN3SD', 'CESTRESS_Z2WK_JPY_FX_UP3SD','CESTRESS_Z2WK_JPY_FX_DOWN3SD', 'CESTRESS_Z2WK_GBP_FX_UP3SD','CESTRESS_Z2WK_GBP_FX_DOWN3SD', 'CESTRESS_Z2WK_GBP_IR_UP3SD','CESTRESS_Z2WK_GBP_IR_DOWN3SD', 'CESTRESS_Z2WK_US_EQ_UP3SD', 'CESTRESS_Z2WK_CNY_FX_UP3SD', 'CESTRESS_Z2WK_CHF_FX_UP3SD', 'CESTRESS_Z2WK_TRY_FX_UP3SD' ,'CESTRESS_Z2WK_CR_DOWN50PCT' ,'CESTRESS_Z2WK_CR_UP50PCT', '_UP3SD', '_UP3SD', '_UP3SD', '_UP3SD', '_UP3SD', '_UP3SD', '_UP3SD', '_UP3SD', '_UP3SD', '_UP3SD' ) AND ctpy_credit_ultimate_name in (                                                  '(blank)', '#N/A',                          'AEGON NV', 'ARBEJDSMARKEDETS TILLAGSPENSION', 'BANK OF AMERICA CORPORATION', 'BARCLAYS PLC', 'CITIGROUP INC.', 'DEUTSCHE BANK AG', 'FEDERAL REPUBLIC OF GERMANY', 'JPMORGAN CHASE & CO.', 'LEGAL & GENERAL GROUP PLC', 'LLOYDS BANKING GROUP PLC', 'METLIFE, INC.', 'MITSUBISHI UFJ FINANCIAL GROUP, INC.', 'PEOPLE''S REPUBLIC OF CHINA', 'PRUDENTIAL FINANCIAL INC.', 'RBS PENSION TRUSTEE LIMITED AS TRUSTEE OF THE ROYAL BANK OF SCOTLAND GROUP PENSION FUND', 'STICHTING BEDRIJFSTAKPENSIOENFONDS VOOR HET BEROEPSVERVOER OVER DE WEG', 'THE ROYAL BANK OF SCOTLAND GROUP PLC', '(blank)', 'THE GOLDMAN SACHS GROUP INC.', 'APG TREASURY CENTER B.V.', 'ROKOS GLOBAL MACRO MASTER FUND LP', 'NIPPON LIFE INSURANCE COMPANY', 'PENSION INSURANCE CORPORATION PLC', '#N/A',                 'dummy' )     GROUP BY        ctpy_credit_ultimate_name,         PRODUCT_SUBTYPE,         PRODUCT_TYPE,         SOURCE_SCN_NAME,         COB_DATE,         book,         ccc_strategy ) t Group by product_type,        ctpy_credit_ultimate_name, source_scn_name