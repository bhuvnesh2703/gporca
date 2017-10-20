
//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformExpandNAryJoinDP.cpp
//
//	@doc:
//		Implementation of n-ary join expansion using dynamic programming
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/xforms/CXformExpandNAryJoinDP.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/xforms/CJoinOrderDP.h"



using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinDP::CXformExpandNAryJoinDP
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformExpandNAryJoinDP::CXformExpandNAryJoinDP
	(
	IMemoryPool *pmp
	)
	:
	CXformExploration
		(
		 // pattern
		GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalNAryJoin(pmp),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternMultiLeaf(pmp)),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))
					)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinDP::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformExpandNAryJoinDP::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	return CXformUtils::ExfpExpandJoinOrder(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinDP::Transform
//
//	@doc:
//		Actual transformation of n-ary join to cluster of inner joins using
//		dynamic programming
//
//---------------------------------------------------------------------------
void
CXformExpandNAryJoinDP::Transform
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

	IMemoryPool *pmp = pxfctxt->Pmp();

	const ULONG ulArity = pexpr->UlArity();
	GPOS_ASSERT(ulArity >= 3);
// For a input tree, pexpr like below
	
//		+--CLogicalNAryJoin
//		   |--CLogicalDynamicGet "p1" ("p1")
//		   |--CLogicalDynamicGet "p2" ("p2")
//		   |--CLogicalDynamicGet "p3" ("p3")
//		   +--CScalarBoolOp (EboolopAnd)
//			  |--CScalarCmp (=)   
//			  |  |--CScalarIdent "b" (1) 
//			  |  +--CScalarIdent "b" (16)
//			  |--CScalarCmp (=)
//			  |  |--CScalarIdent "b" (16)
//			  |  +--CScalarIdent "b" (31)
//			  +--CScalarCmp (=) 
//				 |--CScalarIdent "b" (1)
//				 +--CScalarIdent "b" (31)
// We create an array pdrgpexpr of expression which will hold all the Expression except the last child which is the scalar cmp
// CLogicalDynamicGet "p1" ("p1"), CLogicalDynamicGet "p2" ("p2"), CLogicalDynamicGet "p3" ("p3")
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulArity - 1; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		pexprChild->AddRef();
		pdrgpexpr->Append(pexprChild);
	}
	// The below pexprScalar expression holds the scalar child
//		   +--CScalarBoolOp (EboolopAnd)
//			  |--CScalarCmp (=)   
//			  |  |--CScalarIdent "b" (1) 
//			  |  +--CScalarIdent "b" (16)
//			  |--CScalarCmp (=)
//			  |  |--CScalarIdent "b" (16)
//			  |  +--CScalarIdent "b" (31)
//			  +--CScalarCmp (=) 
//				 |--CScalarIdent "b" (1)
//				 +--CScalarIdent "b" (31)
	CExpression *pexprScalar = (*pexpr)[ulArity - 1];
	
	// The below array of expressions holds the scalar cmp expressions as individual elements
//		+--CScalarCmp (=)
//		   |--CScalarIdent "b" (1)
//		   +--CScalarIdent "b" (16)
//		+--CScalarCmp (=)
//		   |--CScalarIdent "b" (16)
//		   +--CScalarIdent "b" (31)
//		+--CScalarCmp (=)
//		   |--CScalarIdent "b" (1)
//		   +--CScalarIdent "b" (31)
	DrgPexpr *pdrgpexprPreds = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprScalar);

	// create join order using dynamic programming
//	In JoinOrder we have created a bitset map which maps the scalar cmp column reference set and the
// expression which use them
	CJoinOrderDP jodp(pmp, pdrgpexpr, pdrgpexprPreds);
	CExpression *pexprResult = jodp.PexprExpand();

	if (NULL != pexprResult)
	{
		// normalize resulting expression
		CExpression *pexprNormalized = CNormalizer::PexprNormalize(pmp, pexprResult);
		pexprResult->Release();
		pxfres->Add(pexprNormalized);

		const ULONG UlTopKJoinOrders = jodp.PdrgpexprTopK()->UlLength();
		for (ULONG ul = 0; ul < UlTopKJoinOrders; ul++)
		{
			CExpression *pexprJoinOrder = (*jodp.PdrgpexprTopK())[ul];
			if (pexprJoinOrder != pexprResult)
			{
				pexprJoinOrder->AddRef();
				pxfres->Add(pexprJoinOrder);
			}
		}
	}
}

// EOF
