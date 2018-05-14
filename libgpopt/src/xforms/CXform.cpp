//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CXform.cpp
//
//	@doc:
//		Base class for all transformations
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/xforms/CXform.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXform::CXform
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXform::CXform
	(
	CExpression *pexpr
	)
	:
	m_pexpr(pexpr)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(FCheckPattern(pexpr));
}


//---------------------------------------------------------------------------
//	@function:
//		CXform::~CXform
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CXform::~CXform()
{
	m_pexpr->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CXform::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CXform::OsPrint
	(
	IOstream &os
	) 
	const
{
	os << "Xform: " << SzId();

	if (GPOS_FTRACE(EopttracePrintXformPattern))
	{
		os	<< std::endl << "Pattern:" << std::endl << *m_pexpr;
	}

	return os;
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CXform::FCheckPattern
//
//	@doc:
//		check a given expression against the pattern
//
//---------------------------------------------------------------------------
BOOL
CXform::FCheckPattern
	(
	CExpression *pexpr
	) 
	const
{
	return pexpr->FMatchPattern(PexprPattern());
}


//---------------------------------------------------------------------------
//	@function:
//		CXform::FPromising
//
//	@doc:
//		Verify xform promise for the given expression
//
//---------------------------------------------------------------------------
BOOL
CXform::FPromising
	(
	IMemoryPool *pmp,
	const CXform *pxform,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pxform);
	GPOS_ASSERT(NULL != pexpr);
	
	CExpressionHandle exprhdl(pmp);
	exprhdl.Attach(pexpr);
	exprhdl.DeriveProps(NULL /*pdpctxt*/);

	return ExfpNone < pxform->Exfp(exprhdl);
}

#endif // GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CXform::FEqualIds
//
//	@doc:
//		Equality function on xform ids
//
//---------------------------------------------------------------------------
BOOL
CXform::FEqualIds
	(
	const CHAR *szIdOne,
	const CHAR *szIdTwo
	)
{
	return 0 == clib::IStrCmp(szIdOne, szIdTwo);
}

//---------------------------------------------------------------------------
//	@function:
//		CXform::PbsIndexJoinXforms
//
//	@doc:
//		Returns a set containing all xforms related to index join.
//		Caller takes ownership of the returned set
//
//---------------------------------------------------------------------------
CBitSet *CXform::PbsIndexJoinXforms
	(
	IMemoryPool *pmp
	)
{
	CBitSet *pbs = GPOS_NEW(pmp) CBitSet(pmp, EopttraceSentinel);
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfLeftOuterJoin2IndexGetApply));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfLeftOuterJoinWithInnerSelect2IndexGetApply));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfLeftOuterJoin2BitmapIndexGetApply));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfLeftOuterJoinWithInnerSelect2BitmapIndexGetApply));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfInnerJoin2IndexGetApply));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfInnerJoin2DynamicIndexGetApply));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfInnerJoin2PartialDynamicIndexGetApply));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfInnerJoinWithInnerSelect2IndexGetApply));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfInnerJoinWithInnerSelect2DynamicIndexGetApply));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfInnerJoinWithInnerSelect2PartialDynamicIndexGetApply));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfInnerJoin2BitmapIndexGetApply));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfInnerJoin2DynamicBitmapIndexGetApply));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfInnerJoinWithInnerSelect2BitmapIndexGetApply));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply));

	return pbs;
}

//---------------------------------------------------------------------------
//	@function:
//		CXform::PbsBitmapIndexXforms
//
//	@doc:
//		Returns a set containing all xforms related to bitmap indexes.
//		Caller takes ownership of the returned set
//
//---------------------------------------------------------------------------
CBitSet *CXform::PbsBitmapIndexXforms
	(
	IMemoryPool *pmp
	)
{
	CBitSet *pbs = GPOS_NEW(pmp) CBitSet(pmp, EopttraceSentinel);
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfSelect2BitmapBoolOp));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfSelect2DynamicBitmapBoolOp));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfLeftOuterJoin2BitmapIndexGetApply));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfLeftOuterJoinWithInnerSelect2BitmapIndexGetApply));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfInnerJoin2BitmapIndexGetApply));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfInnerJoin2DynamicBitmapIndexGetApply));
	(void) pbs->ExchangeSet(
			GPOPT_DISABLE_XFORM_TF(CXform::ExfInnerJoinWithInnerSelect2BitmapIndexGetApply));
	(void) pbs->ExchangeSet(
			GPOPT_DISABLE_XFORM_TF(CXform::ExfInnerJoinWithInnerSelect2DynamicBitmapIndexGetApply));

	return pbs;
}

//---------------------------------------------------------------------------
//	@function:
//		CXform::PbsHeterogeneousIndexXforms
//
//	@doc:
//		Returns a set containing all xforms related to heterogeneous indexes.
//		Caller takes ownership of the returned set
//
//---------------------------------------------------------------------------
CBitSet *CXform::PbsHeterogeneousIndexXforms
	(
	IMemoryPool *pmp
	)
{
	CBitSet *pbs = GPOS_NEW(pmp) CBitSet(pmp, EopttraceSentinel);

	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfSelect2PartialDynamicIndexGet));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfInnerJoin2PartialDynamicIndexGetApply));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfInnerJoinWithInnerSelect2PartialDynamicIndexGetApply));

	return pbs;
}

//	returns a set containing all xforms that generate a plan with hash join
//	Caller takes ownership of the returned set
CBitSet *CXform::PbsHashJoinXforms
	(
	IMemoryPool *pmp
	)
{
	CBitSet *pbs = GPOS_NEW(pmp) CBitSet(pmp, EopttraceSentinel);

	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfInnerJoin2HashJoin));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfLeftOuterJoin2HashJoin));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfLeftSemiJoin2HashJoin));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfLeftAntiSemiJoin2HashJoin));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfLeftAntiSemiJoinNotIn2HashJoinNotIn));

	return pbs;
}

CBitSet *CXform::PbsJoinOrderInQueryXforms
	(
	IMemoryPool *pmp
	)
{
	CBitSet *pbs = GPOS_NEW(pmp) CBitSet(pmp, EopttraceSentinel);

	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfExpandNAryJoinDP));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfExpandNAryJoinMinCard));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfJoinAssociativity));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfJoinCommutativity));

	return pbs;
}

CBitSet *CXform::PbsJoinOrderOnGreedyXforms
	(
	IMemoryPool *pmp
	)
{
	CBitSet *pbs = GPOS_NEW(pmp) CBitSet(pmp, EopttraceSentinel);

	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfExpandNAryJoinDP));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfJoinAssociativity));
	(void) pbs->ExchangeSet(GPOPT_DISABLE_XFORM_TF(CXform::ExfJoinCommutativity));

	return pbs;
}
// EOF

