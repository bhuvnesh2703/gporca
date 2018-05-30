//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CLogicalUnion.cpp
//
//	@doc:
//		Implementation of union operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CKeyCollection.h"

#include "gpopt/operators/CLogicalUnion.h"
#include "gpopt/operators/CLogicalUnionAll.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalGbAgg.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnion::CLogicalUnion
//
//	@doc:
//		ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalUnion::CLogicalUnion
	(
	IMemoryPool *memory_pool
	)
	:
	CLogicalSetOp(memory_pool)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnion::CLogicalUnion
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalUnion::CLogicalUnion
	(
	IMemoryPool *memory_pool,
	DrgPcr *pdrgpcrOutput,
	DrgDrgPcr *pdrgpdrgpcrInput
	)
	:
	CLogicalSetOp(memory_pool, pdrgpcrOutput, pdrgpdrgpcrInput)
{

#ifdef GPOS_DEBUG
	DrgPcr *pdrgpcrInput = (*pdrgpdrgpcrInput)[0];
	const ULONG ulCols = pdrgpcrOutput->Size();
	GPOS_ASSERT(ulCols == pdrgpcrInput->Size());

	// Ensure that the output columns are the same as first input
	for(ULONG ul = 0; ul < ulCols; ul++)
	{
		GPOS_ASSERT( (*pdrgpcrOutput)[ul] == (*pdrgpcrInput)[ul]);
	}

#endif // GPOS_DEBUG
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnion::~CLogicalUnion
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalUnion::~CLogicalUnion()
{
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnion::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalUnion::PopCopyWithRemappedColumns
	(
	IMemoryPool *memory_pool,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcrOutput = CUtils::PdrgpcrRemap(memory_pool, m_pdrgpcrOutput, phmulcr, fMustExist);
	DrgDrgPcr *pdrgpdrgpcrInput = CUtils::PdrgpdrgpcrRemap(memory_pool, m_pdrgpdrgpcrInput, phmulcr, fMustExist);

	return GPOS_NEW(memory_pool) CLogicalUnion(memory_pool, pdrgpcrOutput, pdrgpdrgpcrInput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnion::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalUnion::Maxcard
	(
	IMemoryPool *, // memory_pool
	CExpressionHandle &exprhdl
	)
	const
{
	const ULONG ulArity = exprhdl.Arity();
	
	CMaxCard maxcard = exprhdl.Pdprel(0)->Maxcard();
	for (ULONG ul = 1; ul < ulArity; ul++)
	{
		maxcard += exprhdl.Pdprel(ul)->Maxcard();
	}
	
	return maxcard;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnion::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalUnion::PxfsCandidates
	(
	IMemoryPool *memory_pool
	) 
	const
{
	CXformSet *pxfs = GPOS_NEW(memory_pool) CXformSet(memory_pool);
	(void) pxfs->ExchangeSet(CXform::ExfUnion2UnionAll);
	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnion::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalUnion::PstatsDerive
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl,
	DrgPstat * // not used
	)
	const
{
	GPOS_ASSERT(Esp(exprhdl) > EspNone);

	// union is transformed into a group by over an union all
	// we follow the same route to compute statistics
	IStatistics *pstatsUnionAll = CLogicalUnionAll::PstatsDeriveUnionAll(memory_pool, exprhdl);

	// computed columns
	ULongPtrArray *pdrgpulComputedCols = GPOS_NEW(memory_pool) ULongPtrArray(memory_pool);

	IStatistics *pstats = CLogicalGbAgg::PstatsDerive
											(
											memory_pool,
											pstatsUnionAll,
											m_pdrgpcrOutput, // we group by the output columns
											pdrgpulComputedCols, // no computed columns for set ops
											NULL // no keys, use all grouping cols
											);

	// clean up
	pdrgpulComputedCols->Release();
	pstatsUnionAll->Release();

	return pstats;
}


// EOF
