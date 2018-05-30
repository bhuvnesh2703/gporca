//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CLogicalLeftAntiSemiCorrelatedApply.cpp
//
//	@doc:
//		Implementation of left anti semi correlated apply for NOT EXISTS subqueries
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CLogicalLeftAntiSemiCorrelatedApply.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftAntiSemiCorrelatedApply::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalLeftAntiSemiCorrelatedApply::PxfsCandidates
	(
	IMemoryPool *memory_pool
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(memory_pool) CXformSet(memory_pool);
	(void) pxfs->ExchangeSet(CXform::ExfImplementLeftAntiSemiCorrelatedApply);

	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftAntiSemiCorrelatedApply::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalLeftAntiSemiCorrelatedApply::PopCopyWithRemappedColumns
	(
	IMemoryPool *memory_pool,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcrInner = CUtils::PdrgpcrRemap(memory_pool, m_pdrgpcrInner, phmulcr, fMustExist);

	return GPOS_NEW(memory_pool) CLogicalLeftAntiSemiCorrelatedApply(memory_pool, pdrgpcrInner, m_eopidOriginSubq);
}


// EOF

