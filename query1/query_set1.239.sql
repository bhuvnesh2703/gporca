SELECT
    A.CCC_BUSINESS_AREA,
    A.CCC_PRODUCT_LINE,
    A.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, 
    CASE WHEN substring(A.BOOK,length(A.BOOK)+1-5) = 'DISDL' THEN 'DIRECT LENDING' 
        when A.PRODUCT_TYPE_CODE in ('GVTBOND','ETF','OPTION','CRDINDEX','CDSOPTIDX') then 'HEDGES'
        when UPPER(A.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME) IN 
                ('WASHINGTON MUTUAL PREFERRED FUNDING TRUST I', 'WASHINGTON MUTUAL, INC.', 
                'WASHINGTON MUTUAL PREFERRED FUNDING TRUST III', 'WASHINGTON MUTUAL BANK', 
                'LEHMAN BROTHERS HOLDINGS INC.', 
                'LEHMAN BROTHERS HOLDINGS, INC.', 
                'LEHMAN BROTHERS HOLDINGS E-CAPITAL TRUST I', 
                'LEHMAN BROTHERS UK CAPITAL FUNDING II LP', 
                'LEHMAN BROTHERS HOLDINGS CAPITAL TRUST VARIOUS SERIES', 
                'LEHMAN BROTHERS COMMERCIAL MORTGAGE K.K.', 'LEHMAN BROTHERS JAPAN INC.', 
                'NORTEL NETWORKS CORPORATION',
                'LANDSBANKI ISLANDS HF', 'GLITNIR BANKI HF', 'KAUPTHING BANK HF', 
                'FONTAINEBLEAU LAS VEGAS, LLC','LEHMAN BROTHERS TREASURY CO. BV','HYPO ALPE-ADRIA-BANK INTERNATIONAL AG','LA SEDA DE BARCELONA SA','REYAL URBIS S.A.') then 'LIQUIDATION'
        when A.CCC_PL_REPORTING_REGION in ('JAPAN','ASIA PACIFIC') then 'WORKOUT'
        when A.BOOK in ('SECTD') then 'WORKOUT'
        when A.REFERENCE_ENTITY_NAME in ('SIC PROCESSING AG','SOLARWORLD AG','Q-CELLS INTERNATIONAL FINANCE B.V.','CIRIO FINANZIARA SPA') then 'LIQUIDATION'
        when (A.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME like '%TRAVELPORT%' and A.COB_DATE > '2015-03-25') OR A.PRODUCT_TYPE_CODE in ('STOCK','ADR','EQUITY') then 'EQUITY'
        else 'TRADING' END AS TRADE_GROUPING,
   SUM (CASE WHEN A.COB_DATE = '2018-02-28' THEN A.USD_INVENTORY ELSE 0 END) AS CURRENT_INVENTORY,
   ABS(SUM (CASE WHEN A.COB_DATE = '2018-02-28' THEN A.USD_INVENTORY ELSE 0 END)) AS ABS_CURRENT_INVENTORY,
   SUM (CASE WHEN A.COB_DATE = '2018-01-31' THEN A.USD_INVENTORY ELSE 0 END) AS PREVIOUS_INVENTORY,
    ABS(SUM (CASE WHEN A.COB_DATE = '2018-02-28' THEN A.USD_INVENTORY ELSE -A.USD_INVENTORY END)) AS ABS_INVENTORY_DOD
FROM CDWUSER.U_DM_CC a
WHERE
    A.COB_DATE IN ('2018-02-28','2018-01-31') AND 
    a.CCC_PL_REPORTING_REGION in ('NON JAPAN ASIA','ASIA PACIFIC','JAPAN') AND 
    A.CCC_PRODUCT_LINE = 'DISTRESSED TRADING' AND
    A.PRODUCT_TYPE_CODE NOT IN ('GVTBOND', 'ETF', 'OPTION', 'CRDINDEX')
--   AND A.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME <> 'UNDEFINED'
 AND (CCC_PL_REPORTING_REGION IN ('NON JAPAN ASIA','ASIA PACIFIC','JAPAN'))
GROUP BY
    A.CCC_BUSINESS_AREA,
    A.CCC_PRODUCT_LINE,
    A.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME, 
    CASE WHEN substring(A.BOOK,length(A.BOOK)+1-5) = 'DISDL' THEN 'DIRECT LENDING' 
        when A.PRODUCT_TYPE_CODE in ('GVTBOND','ETF','OPTION','CRDINDEX','CDSOPTIDX') then 'HEDGES'
        when UPPER(A.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME) IN 
                ('WASHINGTON MUTUAL PREFERRED FUNDING TRUST I', 'WASHINGTON MUTUAL, INC.', 
                'WASHINGTON MUTUAL PREFERRED FUNDING TRUST III', 'WASHINGTON MUTUAL BANK', 
                'LEHMAN BROTHERS HOLDINGS INC.', 
                'LEHMAN BROTHERS HOLDINGS, INC.', 
                'LEHMAN BROTHERS HOLDINGS E-CAPITAL TRUST I', 
                'LEHMAN BROTHERS UK CAPITAL FUNDING II LP', 
                'LEHMAN BROTHERS HOLDINGS CAPITAL TRUST VARIOUS SERIES', 
                'LEHMAN BROTHERS COMMERCIAL MORTGAGE K.K.', 'LEHMAN BROTHERS JAPAN INC.', 
                'NORTEL NETWORKS CORPORATION',
                'LANDSBANKI ISLANDS HF', 'GLITNIR BANKI HF', 'KAUPTHING BANK HF', 
                'FONTAINEBLEAU LAS VEGAS, LLC','LEHMAN BROTHERS TREASURY CO. BV','HYPO ALPE-ADRIA-BANK INTERNATIONAL AG','LA SEDA DE BARCELONA SA','REYAL URBIS S.A.') then 'LIQUIDATION'
        when A.CCC_PL_REPORTING_REGION in ('JAPAN','ASIA PACIFIC') then 'WORKOUT'
        when A.BOOK in ('SECTD') then 'WORKOUT'
        when A.REFERENCE_ENTITY_NAME in ('SIC PROCESSING AG','SOLARWORLD AG','Q-CELLS INTERNATIONAL FINANCE B.V.','CIRIO FINANZIARA SPA') then 'LIQUIDATION'
        when (A.POSITION_ULT_ISSUER_PARTY_DARWIN_NAME like '%TRAVELPORT%' and A.COB_DATE > '2015-03-25') OR A.PRODUCT_TYPE_CODE in ('STOCK','ADR','EQUITY') then 'EQUITY'
        else 'TRADING' END