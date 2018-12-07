//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformLeftOuterJoin2RightOuterJoin.cpp
//
//	@doc:
//		Simplify Left Outer Join with constant false predicate
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformLeftOuterJoin2RightOuterJoin.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuterJoin2RightOuterJoin::CXformLeftOuterJoin2RightOuterJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformLeftOuterJoin2RightOuterJoin::CXformLeftOuterJoin2RightOuterJoin
	(
	IMemoryPool *mp
	)
	:
	CXformExploration
		(
		 // pattern
		GPOS_NEW(mp) CExpression
					(
					mp,
					GPOS_NEW(mp) CLogicalLeftOuterJoin(mp),
					GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)), // left child
					GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // right child
					GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // predicate tree
					)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuterJoin2RightOuterJoin::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftOuterJoin2RightOuterJoin::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
//	CExpression *pexprScalar = exprhdl.PexprScalarChild(2 /*child_index*/);
	if (exprhdl.Pop()->Eopid() == COperator::EopLogicalLeftOuterJoin)
	{
		// if LOJ predicate is False, we can replace inner child with empty table
		return CXform::ExfpHigh;
	}

	return CXform::ExfpNone;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftOuterJoin2RightOuterJoin::Transform
//
//	@doc:
//		Actual transformation to simplify left outer join
//
//---------------------------------------------------------------------------
void
CXformLeftOuterJoin2RightOuterJoin::Transform
	(
	CXformContext *pxfctxt,
	CXformResult *pxfres,
	CExpression *pexpr
	)
	const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	IMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprOuter = (*pexpr)[0];
	CExpression *pexprInner = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	pexprOuter->AddRef();
	pexprScalar->AddRef();
	pexprInner->AddRef();
	
	CExpression *pexprROJ = CUtils::PexprLogicalJoin<CLogicalRightOuterJoin>(mp, pexprInner, pexprOuter, pexprScalar);

	pxfres->Add(pexprROJ);
}

// EOF
