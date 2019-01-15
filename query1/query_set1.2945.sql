SELECT cob_date,vintage, ccc_business_area, core_flag, ccc_product_line, CCC_PL_REPORTING_REGION, slice_m50, slice_m30, slice_m25, slice_m20, slice_m10, slice_0, slice_50, slice_30, slice_25, slice_20, slice_10, slice_m50ptl, slice_m30ptl, slice_m20ptl, slice_m10ptl, slice_m5ptl, slice_m2_5ptl, slice_50ptl, slice_30ptl, slice_20ptl, slice_10ptl, slice_5ptl, slice_2_5ptl, spg_desc2, spg_desc3, spg_desc4 from ( SELECT cob_date,vintage, ccc_business_area, CASE WHEN ccc_business_area = 'SECURITIZED PRODUCTS GRP' THEN 'CORE' ELSE 'NON CORE' END AS core_flag, ccc_product_line, CCC_PL_REPORTING_REGION, case when curve_type IN ('CREDITCURVE', 'CREDITCURVE:CLO', 'CREDITCURVE:GRANITE') then (Sum (COALESCE (SLIDE_SPGCC_MIN_50PCT_USD, 0)) + Sum (COALESCE (SLIDE_CMBX_MIN_50PCT_USD, 0))) else Sum (COALESCE (SLIDE_CMBX_MIN_50PCT_USD, 0)) end AS slice_m50, case when curve_type IN ('CREDITCURVE', 'CREDITCURVE:CLO', 'CREDITCURVE:GRANITE') then (Sum (COALESCE (SLIDE_SPGCC_MIN_30PCT_USD, 0)) + Sum (COALESCE (SLIDE_CMBX_MIN_30PCT_USD, 0))) else Sum (COALESCE (SLIDE_CMBX_MIN_30PCT_USD, 0)) end AS slice_m30, case when curve_type IN ('CREDITCURVE', 'CREDITCURVE:CLO', 'CREDITCURVE:GRANITE') then (Sum (COALESCE (SLIDE_SPGCC_MIN_25PCT_USD, 0)) ) end AS slice_m25, case when curve_type IN ('CREDITCURVE', 'CREDITCURVE:CLO', 'CREDITCURVE:GRANITE') then (Sum (COALESCE (SLIDE_SPGCC_MIN_20PCT_USD, 0)) + Sum (COALESCE (SLIDE_CMBX_MIN_20PCT_USD, 0))) else Sum (COALESCE (SLIDE_CMBX_MIN_20PCT_USD, 0)) end AS slice_m20, case when curve_type IN ('CREDITCURVE', 'CREDITCURVE:CLO', 'CREDITCURVE:GRANITE') then (Sum (COALESCE (SLIDE_SPGCC_MIN_10PCT_USD, 0)) + Sum (COALESCE (SLIDE_CMBX_MIN_10PCT_USD, 0))) else Sum (COALESCE (SLIDE_CMBX_MIN_10PCT_USD, 0)) end AS slice_m10, case when curve_type IN ('CREDITCURVE', 'CREDITCURVE:CLO', 'CREDITCURVE:GRANITE') then (Sum (COALESCE (SLIDE_SPGCC_USD, 0)) + Sum (COALESCE (SLIDE_CMBX_USD, 0))) else Sum (COALESCE (SLIDE_CMBX_USD, 0)) end AS slice_0, case when curve_type IN ('CREDITCURVE', 'CREDITCURVE:CLO', 'CREDITCURVE:GRANITE') then (Sum (COALESCE (SLIDE_SPGCC_PLS_50PCT_USD, 0)) + Sum (COALESCE (SLIDE_CMBX_PLS_50PCT_USD, 0))) else Sum (COALESCE (SLIDE_CMBX_PLS_50PCT_USD, 0)) end AS slice_50, case when curve_type IN ('CREDITCURVE', 'CREDITCURVE:CLO', 'CREDITCURVE:GRANITE') then (Sum (COALESCE (SLIDE_SPGCC_PLS_30PCT_USD, 0)) + Sum (COALESCE (SLIDE_CMBX_PLS_30PCT_USD, 0))) else Sum (COALESCE (SLIDE_CMBX_PLS_30PCT_USD, 0)) end AS slice_30, case when curve_type IN ('CREDITCURVE', 'CREDITCURVE:CLO', 'CREDITCURVE:GRANITE') then (Sum (COALESCE (SLIDE_SPGCC_PLS_25PCT_USD, 0)) ) end AS slice_25, case when curve_type IN ('CREDITCURVE', 'CREDITCURVE:CLO', 'CREDITCURVE:GRANITE') then (Sum (COALESCE (SLIDE_SPGCC_PLS_20PCT_USD, 0)) + Sum (COALESCE (SLIDE_CMBX_PLS_20PCT_USD, 0))) else Sum (COALESCE (SLIDE_CMBX_PLS_20PCT_USD, 0)) end AS slice_20, case when curve_type IN ('CREDITCURVE', 'CREDITCURVE:CLO', 'CREDITCURVE:GRANITE') then (Sum (COALESCE (SLIDE_SPGCC_PLS_10PCT_USD, 0)) + Sum (COALESCE (SLIDE_CMBX_PLS_10PCT_USD, 0))) else Sum (COALESCE (SLIDE_CMBX_PLS_10PCT_USD, 0)) end AS slice_10, Sum (COALESCE (SLIDE_ptlpv_MIN_50PCT_USD, 0)) AS slice_m50ptl, Sum (COALESCE (SLIDE_ptlpv_MIN_30PCT_USD, 0)) AS slice_m30ptl, Sum (COALESCE (SLIDE_ptlpv_MIN_20PCT_USD, 0)) AS slice_m20ptl, Sum (COALESCE (SLIDE_ptlpv_MIN_10PCT_USD, 0)) AS slice_m10ptl, Sum (COALESCE (SLIDE_PTLPV_MIN_2P5PCT_USD, 0)) AS slice_m5ptl, Sum (COALESCE (SLIDE_PTLPV_MIN_2P5PCT_USD, 0)) AS slice_m2_5ptl, Sum (COALESCE (SLIDE_ptlpv_PLS_50PCT_USD, 0)) AS slice_50ptl, Sum (COALESCE (SLIDE_ptlpv_PLS_30PCT_USD, 0)) AS slice_30ptl, Sum (COALESCE (SLIDE_ptlpv_PLS_20PCT_USD, 0)) AS slice_20ptl, Sum (COALESCE (SLIDE_ptlpv_PLS_10PCT_USD, 0)) AS slice_10ptl, Sum (COALESCE (SLIDE_PTLPV_PLS_2P5PCT_USD, 0)) AS slice_5ptl, Sum (COALESCE (SLIDE_PTLPV_PLS_2P5PCT_USD, 0)) AS slice_2_5ptl, case spg_desc when 'ABS AIRPLANE SECURITY' then 'ABS' when 'ABS AUTO LOAN & SECURITY' then 'ABS' when 'ABS CREDIT BASKET' then 'ABS' when 'ABS CREDIT CARD SECURITY' then 'ABS' when 'ABS DEFAULT SWAP' then 'ABS' when 'ABS EQUIPMENT SECURITY' then 'ABS' when 'ABS FLOORPLAN SECURITY' then 'ABS' when 'ABS FRANCHISE SECURITY' then 'ABS' when 'ABS MANUFACTURED HOUSING SECURITY' then 'ABS' when 'ABS OTHER SECURITY' then 'ABS' when 'ABS PRIVATE STUDENT ARN' then 'ABS' when 'ABS PRIVATE STUDENT SECURITY' then 'ABS' when 'ABS STUDENT ARN' then 'ABS' when 'ABS STUDENT SECURITY' then 'ABS' when 'ABS UTILITY SECURITY' then 'ABS' when 'AGENCY CMBS SECURITY' then 'Agency' when 'CMBS CDO' then 'CMBS' when 'CMBS CREDIT BASKET' then 'CMBS' when 'CMBS DEFAULT SWAP' then 'CMBS' when 'CMBS HIGRADE CREDIT BASKET' then 'CMBS' when 'CMBS INDEX' then 'CMBS' when 'CMBS IO' then 'CMBS' when 'CMBS IO REREMIC' then 'CMBS' when 'CMBS LOAN' then 'CMBS' when 'CMBS LOAN IO' then 'CMBS' when 'CMBS MEZZANINE LOAN' then 'CMBS' when 'CMBS MEZZANINE SECURITY' then 'CMBS' when 'CMBS SECURITY' then 'CMBS' when 'CMBS SECURITY REREMIC' then 'CMBS' when 'CMBS TOTAL RETURN SWAP' then 'CMBS' when 'CORPORATE CDO DEFAULT SWAP' then 'CLO' when 'CORPORATE CDO EQUITY' then 'CLO' when 'CORPORATE CDO PREFERRED' then 'CLO' when 'CORPORATE CLO' then 'CLO' when 'CORPORATE CLO TRUPS' then 'CLO' when 'CORPORATE DEFAULT SWAP' then 'Other' when 'OTHER' then 'Other' when 'RMBS ABSPOKE' then 'Resi Securities' when 'RMBS AGENCY CMO' then 'Agency' when 'RMBS AGENCY DERIVATIVES' then 'Agency' when 'RMBS ALTA DEFAULT SWAP' then 'Resi Securities' when 'RMBS ALTA IO' then 'Resi Securities' when 'RMBS ALTA LOAN' then 'Resi Loans' when 'RMBS ALTA REREMIC' then 'Resi Securities' when 'RMBS ALTA REREMIC TRS' then 'Resi Securities' when 'RMBS ALTA RESIDUAL' then 'Resi Securities' when 'RMBS ALTA SECURITY' then 'Resi Securities' when 'RMBS CDO' then 'Resi Securities' when 'RMBS CDO DEFAULT SWAP' then 'Resi Securities' when 'RMBS CDO PREFERRED' then 'Resi Securities' when 'RMBS DEFAULT SWAP' then 'Resi Securities' when 'RMBS HELOC SECURITY' then 'Resi Securities' when 'RMBS INDEX TRANCHE' then 'Resi Securities' when 'RMBS MBS FANNIE MAE SECURITY' then 'Agency' when 'RMBS MBS FREDDIE MAC SECURITY' then 'Agency' when 'RMBS NIMS' then 'Resi Securities' when 'RMBS NON CONFORMING DEFAULT SWAP' then 'Resi Securities' when 'RMBS OPTION ARM SECURITY' then 'Resi Securities' when 'RMBS POST NIM' then 'Resi Securities' when 'RMBS PRIME DEFAULT SWAP' then 'Resi Securities' when 'RMBS PRIME INDEX' then 'Resi Securities' when 'RMBS PRIME IO' then 'Resi Securities' when 'RMBS PRIME PREPAYMENT PENALTY' then 'Resi Securities' when 'RMBS PRIME REREMIC' then 'Resi Securities' when 'RMBS PRIME REREMIC TRS' then 'Resi Securities' when 'RMBS PRIME RESIDUAL' then 'Resi Securities' when 'RMBS PRIME SECURITY' then 'Resi Securities' when 'RMBS SD LOAN' then 'Resi Loans' when 'RMBS SD RESIDUAL' then 'Resi Securities' when 'RMBS SD SECURITY' then 'Resi Securities' when 'RMBS SECOND SECURITY' then 'Resi Securities' when 'RMBS SUB PRIME INDEX' then 'Resi Securities' when 'RMBS SUB PRIME IO' then 'Resi Securities' when 'RMBS SUB PRIME REREMIC' then 'Resi Securities' when 'RMBS SUB PRIME RESIDUAL' then 'Resi Securities' when 'RMBS SUB PRIME SECURITY' then 'Resi Securities' when 'RMBS SUPER SENIOR' then 'Resi Securities' else 'Not Grouped' end as spg_desc2, case spg_desc when 'ABS AIRPLANE SECURITY' then 'ABS Credit Card' when 'ABS AUTO LOAN & SECURITY' then 'ABS Auto' when 'ABS CREDIT BASKET' then 'ABS Credit Card' when 'ABS CREDIT CARD SECURITY' then 'ABS Credit Card' when 'ABS DEFAULT SWAP' then 'ABS Credit Card' when 'ABS EQUIPMENT SECURITY' then 'ABS Credit Card' when 'ABS FLOORPLAN SECURITY' then 'ABS Auto' when 'ABS FRANCHISE SECURITY' then 'ABS Credit Card' when 'ABS MANUFACTURED HOUSING SECURITY' then 'ABS Credit Card' when 'ABS OTHER SECURITY' then 'ABS Credit Card' when 'ABS PRIVATE STUDENT ARN' then 'ABS Private Student ARN' when 'ABS PRIVATE STUDENT SECURITY' then 'ABS Private Student Security' when 'ABS STUDENT ARN' then 'ABS Student ARN' when 'ABS STUDENT SECURITY' then 'ABS Student Security' when 'ABS UTILITY SECURITY' then 'ABS Credit Card' when 'AGENCY CMBS SECURITY' then 'Agency' when 'CMBS CDO' then 'CMBS CDO' when 'CMBS CREDIT BASKET' then 'CMBS CDO' when 'CMBS DEFAULT SWAP' then 'CMBS CDS' when 'CMBS HIGRADE CREDIT BASKET' then 'CMBS CDO' when 'CMBS INDEX' then 'CMBS Index' when 'CMBS IO' then 'CMBS Bond' when 'CMBS IO REREMIC' then 'CMBS Bond' when 'CMBS LOAN' then 'CMBS Loan' when 'CMBS LOAN IO' then 'CMBS Loan' when 'CMBS MEZZANINE LOAN' then 'CMBS Loan' when 'CMBS MEZZANINE SECURITY' then 'CMBS Bond' when 'CMBS SECURITY' then 'CMBS Bond' when 'CMBS SECURITY REREMIC' then 'CMBS Bond' when 'CMBS TOTAL RETURN SWAP' then 'CMBS CDS' when 'CORPORATE CDO DEFAULT SWAP' then 'CLO' when 'CORPORATE CDO EQUITY' then 'CLO' when 'CORPORATE CDO PREFERRED' then 'CLO' when 'CORPORATE CLO' then 'CLO' when 'CORPORATE CLO TRUPS' then 'CLO' when 'CORPORATE DEFAULT SWAP' then 'Other' when 'OTHER' then 'Other' when 'RMBS ABSPOKE' then 'Resi Subprime' when 'RMBS AGENCY CMO' then 'Agency' when 'RMBS AGENCY DERIVATIVES' then 'Agency' when 'RMBS ALTA DEFAULT SWAP' then 'Resi AltA' when 'RMBS ALTA IO' then 'Resi AltA' when 'RMBS ALTA LOAN' then 'Resi Loans' when 'RMBS ALTA REREMIC' then 'Resi AltA' when 'RMBS ALTA REREMIC TRS' then 'Resi AltA' when 'RMBS ALTA RESIDUAL' then 'Resi AltA' when 'RMBS ALTA SECURITY' then 'Resi AltA' when 'RMBS CDO' then 'Resi Subprime' when 'RMBS CDO DEFAULT SWAP' then 'Resi Subprime' when 'RMBS CDO PREFERRED' then 'Resi Subprime' when 'RMBS DEFAULT SWAP' then 'Resi Subprime' when 'RMBS HELOC SECURITY' then 'Resi Subprime' when 'RMBS INDEX TRANCHE' then 'Resi Subprime' when 'RMBS MBS FANNIE MAE SECURITY' then 'Agency' when 'RMBS MBS FREDDIE MAC SECURITY' then 'Agency' when 'RMBS NIMS' then 'Resi Subprime' when 'RMBS NON CONFORMING DEFAULT SWAP' then 'Resi Subprime' when 'RMBS OPTION ARM SECURITY' then 'Resi Subprime' when 'RMBS POST NIM' then 'Resi Subprime' when 'RMBS PRIME DEFAULT SWAP' then 'Resi Prime' when 'RMBS PRIME INDEX' then 'Resi Prime' when 'RMBS PRIME IO' then 'Resi Prime' when 'RMBS PRIME PREPAYMENT PENALTY' then 'Resi Prime' when 'RMBS PRIME REREMIC' then 'Resi Prime' when 'RMBS PRIME REREMIC TRS' then 'Resi Prime' when 'RMBS PRIME RESIDUAL' then 'Resi Prime' when 'RMBS PRIME SECURITY' then 'Resi Prime' when 'RMBS SD LOAN' then 'Resi Loans' when 'RMBS SD RESIDUAL' then 'Resi Subprime' when 'RMBS SD SECURITY' then 'Resi Subprime' when 'RMBS SECOND SECURITY' then 'Resi Subprime' when 'RMBS SUB PRIME INDEX' then 'Resi Subprime' when 'RMBS SUB PRIME IO' then 'Resi Subprime' when 'RMBS SUB PRIME REREMIC' then 'Resi Subprime' when 'RMBS SUB PRIME RESIDUAL' then 'Resi Subprime' when 'RMBS SUB PRIME SECURITY' then 'Resi Subprime' when 'RMBS SUPER SENIOR' then 'Resi Subprime' else 'Not Grouped' end as spg_desc3, case spg_desc when 'ABS AIRPLANE SECURITY' then 'other' when 'ABS AUTO LOAN & SECURITY' then 'other' when 'ABS CREDIT BASKET' then 'other' when 'ABS CREDIT CARD SECURITY' then 'other' when 'ABS DEFAULT SWAP' then 'other' when 'ABS EQUIPMENT SECURITY' then 'other' when 'ABS FLOORPLAN SECURITY' then 'other' when 'ABS FRANCHISE SECURITY' then 'other' when 'ABS MANUFACTURED HOUSING SECURITY' then 'other' when 'ABS OTHER SECURITY' then 'other' when 'ABS PRIVATE STUDENT ARN' then 'other' when 'ABS PRIVATE STUDENT SECURITY' then 'other' when 'ABS STUDENT ARN' then 'other' when 'ABS STUDENT SECURITY' then 'other' when 'ABS UTILITY SECURITY' then 'other' when 'AGENCY CMBS SECURITY' then 'other' when 'CMBS CDO' then 'other' when 'CMBS CREDIT BASKET' then 'other' when 'CMBS DEFAULT SWAP' then 'other' when 'CMBS HIGRADE CREDIT BASKET' then 'other' when 'CMBS INDEX' then 'other' when 'CMBS IO' then 'other' when 'CMBS IO REREMIC' then 'other' when 'CMBS LOAN' then 'other' when 'CMBS LOAN IO' then 'other' when 'CMBS MEZZANINE LOAN' then 'other' when 'CMBS MEZZANINE SECURITY' then 'other' when 'CMBS SECURITY' then 'other' when 'CMBS SECURITY REREMIC' then 'other' when 'CMBS TOTAL RETURN SWAP' then 'other' when 'CORPORATE CDO DEFAULT SWAP' then 'other' when 'CORPORATE CDO EQUITY' then 'other' when 'CORPORATE CDO PREFERRED' then 'other' when 'CORPORATE CLO' then 'other' when 'CORPORATE CLO TRUPS' then 'other' when 'CORPORATE DEFAULT SWAP' then 'other' when 'OTHER' then 'other' when 'RMBS ABSPOKE' then 'CDO' when 'RMBS AGENCY CMO' then 'other' when 'RMBS AGENCY DERIVATIVES' then 'other' when 'RMBS ALTA DEFAULT SWAP' then 'Synthetic' when 'RMBS ALTA IO' then 'Cash' when 'RMBS ALTA LOAN' then 'other' when 'RMBS ALTA REREMIC' then 'Cash' when 'RMBS ALTA REREMIC TRS' then 'Synthetic' when 'RMBS ALTA RESIDUAL' then 'Cash' when 'RMBS ALTA SECURITY' then 'Cash' when 'RMBS CDO' then 'CDO' when 'RMBS CDO DEFAULT SWAP' then 'CDO' when 'RMBS CDO PREFERRED' then 'CDO' when 'RMBS DEFAULT SWAP' then 'Synthetic' when 'RMBS HELOC SECURITY' then 'Cash' when 'RMBS INDEX TRANCHE' then 'CDO' when 'RMBS MBS FANNIE MAE SECURITY' then 'other' when 'RMBS MBS FREDDIE MAC SECURITY' then 'other' when 'RMBS NIMS' then 'Cash' when 'RMBS NON CONFORMING DEFAULT SWAP' then 'Synthetic' when 'RMBS OPTION ARM SECURITY' then 'Cash' when 'RMBS POST NIM' then 'Cash' when 'RMBS PRIME DEFAULT SWAP' then 'Synthetic' when 'RMBS PRIME INDEX' then 'Synthetic' when 'RMBS PRIME IO' then 'Cash' when 'RMBS PRIME PREPAYMENT PENALTY' then 'Cash' when 'RMBS PRIME REREMIC' then 'Cash' when 'RMBS PRIME REREMIC TRS' then 'Synthetic' when 'RMBS PRIME RESIDUAL' then 'Cash' when 'RMBS PRIME SECURITY' then 'Cash' when 'RMBS SD LOAN' then 'other' when 'RMBS SD RESIDUAL' then 'Cash' when 'RMBS SD SECURITY' then 'Cash' when 'RMBS SECOND SECURITY' then 'Cash' when 'RMBS SUB PRIME INDEX' then 'Synthetic' when 'RMBS SUB PRIME IO' then 'Cash' when 'RMBS SUB PRIME REREMIC' then 'Cash' when 'RMBS SUB PRIME RESIDUAL' then 'Cash' when 'RMBS SUB PRIME SECURITY' then 'Cash' when 'RMBS SUPER SENIOR' then 'CDO' else 'Not Grouped' end as spg_desc4 from DWUSER.U_SP_MSR where COB_DATE IN ( '2018-02-28','2018-02-21' ) AND CCC_PL_REPORTING_REGION In ('EMEA') AND ccc_strategy NOT IN ( 'MS CVA MPE - DERIVATIVES', 'MS CVA MPE DERIVS CPM' ) and ccc_business_area IN ( 'SECURITIZED PRODUCTS GRP', 'RESIDENTIAL', 'COMMERCIAL RE (PTG)' ) group by cob_date,vintage, ccc_business_area, CASE WHEN ccc_business_area = 'SECURITIZED PRODUCTS GRP' THEN 'CORE' ELSE 'NON CORE' END, ccc_product_line, CCC_PL_REPORTING_REGION, SPG_DESC, curve_type ) AS ALIAS where not ( vintage in (
'PRE04'
,'04-1'
,'04-2'
,'04-3'
,'04-4'
,'05-1'
,'05-2'
,'05-3'
,'05-4'
,'06-1'
,'06-2'
,'06-3'
,'06-4'
,'07-1'
,'07-2'
,'07-3'
,'07-4'
,'08-1'
,'08-2'
,'09-4'
,'10-3'
,'10-4') and CCC_PL_REPORTING_REGION in ('AMERICAS') and SPG_DESC2 in ('CMBS') and SPG_DESC3 in ('CMBS Bond')
)