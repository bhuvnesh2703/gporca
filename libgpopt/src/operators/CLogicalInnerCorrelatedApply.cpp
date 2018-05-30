//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalInnerCorrelatedApply.cpp
//
//	@doc:
//		Implementation of inner correlated apply operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CLogicalInnerCorrelatedApply.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerCorrelatedApply::CLogicalInnerCorrelatedApply
//
//	@doc:
//		Ctor - for patterns
//
//---------------------------------------------------------------------------
CLogicalInnerCorrelatedApply::CLogicalInnerCorrelatedApply
	(
	IMemoryPool *memory_pool
	)
	:
	CLogicalInnerApply(memory_pool)
{}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerCorrelatedApply::CLogicalInnerCorrelatedApply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalInnerCorrelatedApply::CLogicalInnerCorrelatedApply
	(
	IMemoryPool *memory_pool,
	DrgPcr *pdrgpcrInner,
	EOperatorId eopidOriginSubq
	)
	:
	CLogicalInnerApply(memory_pool, pdrgpcrInner, eopidOriginSubq)
{}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerCorrelatedApply::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalInnerCorrelatedApply::PxfsCandidates
	(
	IMemoryPool *memory_pool
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(memory_pool) CXformSet(memory_pool);
	(void) pxfs->ExchangeSet(CXform::ExfImplementInnerCorrelatedApply);

	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerCorrelatedApply::FMatch
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalInnerCorrelatedApply::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		return m_pdrgpcrInner->Equals(CLogicalInnerCorrelatedApply::PopConvert(pop)->PdrgPcrInner());
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerCorrelatedApply::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalInnerCorrelatedApply::PopCopyWithRemappedColumns
	(
	IMemoryPool *memory_pool,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcrInner = CUtils::PdrgpcrRemap(memory_pool, m_pdrgpcrInner, phmulcr, fMustExist);

	return GPOS_NEW(memory_pool) CLogicalInnerCorrelatedApply(memory_pool, pdrgpcrInner, m_eopidOriginSubq);
}

// EOF

