SELECT cob_date, f.NET_VALUE, cast(f.SR_CHARGE_FACTOR as numeric) AS SR_CHARGE_FACTOR, f.NET_VALUE / (cast(f.SR_CHARGE_FACTOR as float) / 100) AS VALUATION, f.process_id, f.position_id FROM CDWUSER.U_FSA_NETTING f WHERE cob_date in ('2018-02-28', '2018-02-27') AND hierarchy_group_id = 202 AND hierarchy_id = 53 AND company_code_group != '-1' AND f.asset_class LIKE 'ALL%' AND f.ORIGINAL_VALUE != 0 AND f.COMPANY_CODE_GROUP = '0302(G)'