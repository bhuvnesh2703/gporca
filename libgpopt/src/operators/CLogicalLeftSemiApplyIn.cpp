//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CLogicalLeftSemiApplyIn.cpp
//
//	@doc:
//		Implementation of left-semi-apply operator with In semantics
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CLogicalLeftSemiApplyIn.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiApplyIn::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalLeftSemiApplyIn::PxfsCandidates
	(
	IMemoryPool *memory_pool
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(memory_pool) CXformSet(memory_pool);
	(void) pxfs->ExchangeSet(CXform::ExfLeftSemiApplyIn2LeftSemiJoin);
	(void) pxfs->ExchangeSet(CXform::ExfLeftSemiApplyInWithExternalCorrs2InnerJoin);
	(void) pxfs->ExchangeSet(CXform::ExfLeftSemiApplyIn2LeftSemiJoinNoCorrelations);

	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiApplyIn::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalLeftSemiApplyIn::PopCopyWithRemappedColumns
	(
	IMemoryPool *memory_pool,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcrInner = CUtils::PdrgpcrRemap(memory_pool, m_pdrgpcrInner, phmulcr, fMustExist);

	return GPOS_NEW(memory_pool) CLogicalLeftSemiApplyIn(memory_pool, pdrgpcrInner, m_eopidOriginSubq);
}

// EOF

