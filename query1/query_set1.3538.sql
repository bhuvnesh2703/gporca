SELECT X.COB_DATE, x.BOOK, case when X.BOOK_STRATEGY in ('Parent', 'Non Bank Subsidiary', 'Non-US Bank Subsidiaries', 'Non-Bank Investment Portfolios', 'MSAIL Assets', 'MSHK Assets', 'Other Securities', 'Spec Financing ISG', 'Inter Company Financing', 'Reval Hedges', 'CTA Hedges') then 'Total Assets' when x.BOOK_STRATEGY in ('Retail CDs / Institutional CDs', 'Third Party Deposit', 'Commercial Paper', 'Bank Loans', 'Short Term Loans', 'Funding Swaps', 'Specific Financing', 'FRN FID', 'FRN IED', 'Accrual Callable', 'ASC 815 Hedges', 'Non-ASC 815 Hedges', 'De-designated Debt', 'Floating Rate', 'Floating Callable Debt', 'IR Basis Swaps', 'MSAIL OIS', 'Cross-Currency Swaps', 'Unhedged Fixed') then 'Total Liabilities' when x.BOOK_STRATEGY in ('PP-E', 'Preferred Stock') or x.book in ('TRPFD', 'PRFEQ') then 'Total Equity' when x.BOOK_STRATEGY in ('ASC 815 Debt') then 'Non-Discounted PV01' else 'Non-Economic' end as Economic_Flag, X.BUCKETED_Term, SUM(X.usd_pv01) AS usd_PV01 FROM (SELECT BOOK_STRATEGY, BUCKETED_TERM, a.BOOK, a.COB_DATE, sum(usd_ir_unified_pv01) as usd_pv01 FROM Cdwuser.U_DM_TREASURY A where cob_date in ('02-28-18') and ccc_division = 'TREASURY CAPITAL MARKETS' and BOOK not in ('TSYGLRCSH') group by BOOK_STRATEGY, BUCKETED_TERM, a.BOOK, a.COB_DATE )x GROUP BY x.book, X.COB_DATE, case when X.BOOK_STRATEGY in ('Parent', 'Non Bank Subsidiary', 'Non-US Bank Subsidiaries', 'Non-Bank Investment Portfolios', 'MSAIL Assets', 'MSHK Assets', 'Other Securities', 'Spec Financing ISG', 'Inter Company Financing', 'Reval Hedges', 'CTA Hedges') then 'Total Assets' when x.BOOK_STRATEGY in ('Retail CDs / Institutional CDs', 'Third Party Deposit', 'Commercial Paper', 'Bank Loans', 'Short Term Loans', 'Funding Swaps', 'Specific Financing', 'FRN FID', 'FRN IED', 'Accrual Callable', 'ASC 815 Hedges', 'Non-ASC 815 Hedges', 'De-designated Debt', 'Floating Rate', 'Floating Callable Debt', 'IR Basis Swaps', 'MSAIL OIS', 'Cross-Currency Swaps', 'Unhedged Fixed') then 'Total Liabilities' when x.BOOK_STRATEGY in ('PP-E', 'Preferred Stock') or x.book in ('TRPFD', 'PRFEQ') then 'Total Equity' when x.BOOK_STRATEGY in ('ASC 815 Debt') then 'Non-Discounted PV01' else 'Non-Economic' end, X.BUCKETED_Term