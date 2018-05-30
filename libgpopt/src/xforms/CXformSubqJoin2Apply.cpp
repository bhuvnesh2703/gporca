//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformSubqJoin2Apply.cpp
//
//	@doc:
//		Implementation of Inner Join to Apply transform
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/xforms/CSubqueryHandler.h"
#include "gpopt/xforms/CXformSubqJoin2Apply.h"
#include "gpopt/xforms/CXformUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSubqJoin2Apply::CXformSubqJoin2Apply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSubqJoin2Apply::CXformSubqJoin2Apply
	(
	IMemoryPool *memory_pool
	)
	:
	// pattern
	CXformSubqueryUnnest
		(
		GPOS_NEW(memory_pool) CExpression
				(
				memory_pool,
				GPOS_NEW(memory_pool) CLogicalInnerJoin(memory_pool),
				GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternLeaf(memory_pool)), // relational child
				GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternLeaf(memory_pool)), // relational child
				GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternTree(memory_pool))  // predicate tree
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformSubqJoin2Apply::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
// 		if subqueries exist in the scalar predicate, we must have an
// 		equivalent logical Apply expression created during exploration;
// 		no need for generating a Join expression here
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSubqJoin2Apply::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	if (exprhdl.Pdpscalar(exprhdl.Arity() - 1)->FHasSubquery())
	{
		return CXform::ExfpHigh;
	}

	return CXform::ExfpNone;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSubqJoin2Apply::CollectSubqueries
//
//	@doc:
//		Collect subqueries that exclusively use columns from one join child
//
//---------------------------------------------------------------------------
void
CXformSubqJoin2Apply::CollectSubqueries
	(
	IMemoryPool *memory_pool,
	CExpression *pexpr,
	DrgPcrs *pdrgpcrs,
	DrgPdrgPexpr *pdrgpdrgpexprSubqs // array-of-arrays indexed on join child index.
									//  i^{th} entry is an array corresponding to subqueries collected for join child #i
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pdrgpcrs);
	GPOS_ASSERT(NULL != pdrgpdrgpexprSubqs);

	COperator *pop = pexpr->Pop();
	if (CUtils::FSubquery(pop))
	{
		// extract outer references below subquery
		CColRefSet *pcrsOuter = GPOS_NEW(memory_pool) CColRefSet(memory_pool, *CDrvdPropRelational::Pdprel((*pexpr)[0]->PdpDerive())->PcrsOuter());

		// add columns used by subquery
		pcrsOuter->Union(CDrvdPropScalar::Pdpscalar(pexpr->PdpDerive())->PcrsUsed());

		ULONG ulChildIndex = ULONG_MAX;
		const ULONG ulSize = pdrgpcrs->Size();
		for (ULONG ul = 0; ul < ulSize; ul++)
		{
			CColRefSet *pcrsOutput = (*pdrgpcrs)[ul];
			if (pcrsOutput->ContainsAll(pcrsOuter))
			{
				// outer columns all come from the same join child, break here
				ulChildIndex = ul;
				break;
			}
		}

		if (ULONG_MAX != ulChildIndex)
		{
			pexpr->AddRef();
			(*pdrgpdrgpexprSubqs)[ulChildIndex]->Append(pexpr);
		}

		pcrsOuter->Release();
		return;
	}

	// recursively process children
	const ULONG ulArity = pexpr->Arity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		CollectSubqueries(memory_pool, pexprChild, pdrgpcrs, pdrgpdrgpexprSubqs);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSubqJoin2Apply::PexprReplaceSubqueries
//
//	@doc:
//		Replace subqueries with scalar identifiers based on given map
//
//---------------------------------------------------------------------------
CExpression *
CXformSubqJoin2Apply::PexprReplaceSubqueries
	(
	IMemoryPool *memory_pool,
	CExpression *pexprScalar,
	HMExprCr *phmexprcr
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexprScalar);
	GPOS_ASSERT(NULL != phmexprcr);

	CColRef *pcr = phmexprcr->Find(pexprScalar);
	if (NULL != pcr)
	{
		// look-up succeeded on root operator, we return here
		return CUtils::PexprScalarIdent(memory_pool, pcr);
	}

	// recursively process children
	const ULONG ulArity = pexprScalar->Arity();
	DrgPexpr *pdrgpexprChildren = GPOS_NEW(memory_pool) DrgPexpr(memory_pool);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = PexprReplaceSubqueries(memory_pool, (*pexprScalar)[ul], phmexprcr);
		pdrgpexprChildren->Append(pexprChild);
	}

	COperator *pop = pexprScalar->Pop();
	pop->AddRef();

	return GPOS_NEW(memory_pool) CExpression(memory_pool, pop, pdrgpexprChildren);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSubqJoin2Apply::PexprSubqueryPushdown
//
//	@doc:
//		Push down subquery below join
//
//---------------------------------------------------------------------------
CExpression *
CXformSubqJoin2Apply::PexprSubqueryPushDown
	(
	IMemoryPool *memory_pool,
	CExpression *pexpr,
	BOOL fEnforceCorrelatedApply
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(COperator::EopLogicalSelect == pexpr->Pop()->Eopid());

	CExpression *pexprJoin = (*pexpr)[0];
	const ULONG ulArity = pexprJoin->Arity();
	CExpression *pexprScalar = (*pexpr)[1];
	CExpression *pexprJoinPred = (*pexprJoin)[ulArity - 1];

	// collect output columns of all logical children
	DrgPcrs *pdrgpcrs = GPOS_NEW(memory_pool) DrgPcrs(memory_pool);
	DrgPdrgPexpr *pdrgpdrgpexprSubqs = GPOS_NEW(memory_pool) DrgPdrgPexpr(memory_pool);
	for (ULONG ul = 0; ul < ulArity - 1; ul++)
	{
		CExpression *pexprChild = (*pexprJoin)[ul];
		CColRefSet *pcrsOutput = CDrvdPropRelational::Pdprel(pexprChild->PdpDerive())->PcrsOutput();
		pcrsOutput->AddRef();
		pdrgpcrs->Append(pcrsOutput);

		pdrgpdrgpexprSubqs->Append(GPOS_NEW(memory_pool) DrgPexpr(memory_pool));
	}

	// collect subqueries that exclusively use columns from each join child
	CollectSubqueries(memory_pool, pexprScalar, pdrgpcrs, pdrgpdrgpexprSubqs);

	// create new join children by pushing subqueries to Project nodes on top
	// of corresponding join children
	DrgPexpr *pdrgpexprNewChildren = GPOS_NEW(memory_pool) DrgPexpr(memory_pool);
	HMExprCr *phmexprcr = GPOS_NEW(memory_pool) HMExprCr(memory_pool);
	for (ULONG ulChild = 0; ulChild < ulArity - 1; ulChild++)
	{
		CExpression *pexprChild = (*pexprJoin)[ulChild];
		pexprChild->AddRef();
		CExpression *pexprNewChild = pexprChild;

		DrgPexpr *pdrgpexprSubqs = (*pdrgpdrgpexprSubqs)[ulChild];
		const ULONG ulSubqs = pdrgpexprSubqs->Size();
		if (0 < ulSubqs)
		{
			// join child has pushable subqueries
			pexprNewChild = CUtils::PexprAddProjection(memory_pool, pexprChild, pdrgpexprSubqs);
			CExpression *pexprPrjList = (*pexprNewChild)[1];

			// add pushed subqueries to map
			for (ULONG ulSubq = 0; ulSubq < ulSubqs; ulSubq++)
			{
				CExpression *pexprSubq = (*pdrgpexprSubqs)[ulSubq];
				pexprSubq->AddRef();
				CColRef *pcr = CScalarProjectElement::PopConvert((*pexprPrjList)[ulSubq]->Pop())->Pcr();
	#ifdef GPOS_DEBUG
				BOOL fInserted =
	#endif // GPOS_DEBUG
					phmexprcr->Insert(pexprSubq, pcr);
				GPOS_ASSERT(fInserted);
			}

			// unnest subqueries in newly created child
			CExpression *pexprUnnested = PexprSubqueryUnnest(memory_pool, pexprNewChild, fEnforceCorrelatedApply);
			pexprNewChild->Release();
			pexprNewChild = pexprUnnested;
		}

		pdrgpexprNewChildren->Append(pexprNewChild);
	}

	pexprJoinPred->AddRef();
	pdrgpexprNewChildren->Append(pexprJoinPred);

	// replace subqueries in the original scalar expression with
	// scalar identifiers based on constructed map
	CExpression *pexprNewScalar = PexprReplaceSubqueries(memory_pool, pexprScalar, phmexprcr);

	phmexprcr->Release();
	pdrgpcrs->Release();
	pdrgpdrgpexprSubqs->Release();

	// build the new join expression
	COperator *pop = pexprJoin->Pop();
	pop->AddRef();
	CExpression *pexprNewJoin = GPOS_NEW(memory_pool) CExpression(memory_pool, pop, pdrgpexprNewChildren);

	// return a new Select expression
	pop = pexpr->Pop();
	pop->AddRef();
	return GPOS_NEW(memory_pool) CExpression(memory_pool, pop, pexprNewJoin, pexprNewScalar);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSubqJoin2Apply::Transform
//
//	@doc:
//		Helper of transformation function
//
//---------------------------------------------------------------------------
void
CXformSubqJoin2Apply::Transform
	(
	CXformContext *pxfctxt,
	CXformResult *pxfres,
	CExpression *pexpr,
	BOOL fEnforceCorrelatedApply
	)
	const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	IMemoryPool *memory_pool = pxfctxt->Pmp();
	CExpression *pexprSelect = CXformUtils::PexprSeparateSubqueryPreds(memory_pool, pexpr);

	// attempt pushing subqueries to join children,
	// this optimization may not always succeed since unnested subqueries below joins
	// could hide columns needed to evaluate join condition
	CExpression *pexprSubqsPushedDown = PexprSubqueryPushDown(memory_pool, pexprSelect, fEnforceCorrelatedApply);

	// check if join columns in join condition are still accessible after subquery pushdown
	CExpression *pexprJoin = (*pexprSubqsPushedDown)[0];
	CExpression *pexprJoinCondition = (*pexprJoin)[pexprJoin->Arity() - 1];
	CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexprJoinCondition->PdpDerive())->PcrsUsed();
	CColRefSet *pcrsJoinOutput = CDrvdPropRelational::Pdprel(pexprJoin->PdpDerive())->PcrsOutput();
	if (!pcrsJoinOutput->ContainsAll(pcrsUsed))
	{
		// discard expression after subquery push down
		pexprSubqsPushedDown->Release();
		pexprSelect->AddRef();
		pexprSubqsPushedDown = pexprSelect;
	}

	pexprSelect->Release();

	CExpression *pexprResult = NULL;
	BOOL fHasSubquery = CDrvdPropScalar::Pdpscalar((*pexprSubqsPushedDown)[1]->PdpDerive())->FHasSubquery();
	if (fHasSubquery)
	{
		// unnest subqueries remaining in the top Select expression
		pexprResult = PexprSubqueryUnnest(memory_pool, pexprSubqsPushedDown, fEnforceCorrelatedApply);
		pexprSubqsPushedDown->Release();
	}
	else
	{
		pexprResult = pexprSubqsPushedDown;
	}

	if (NULL == pexprResult)
	{
		// unnesting failed, return here
		return;
	}

	// normalize resulting expression and add it to xform results container
	CExpression *pexprNormalized = CNormalizer::PexprNormalize(memory_pool, pexprResult);
	pexprResult->Release();
	pxfres->Add(pexprNormalized);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSubqJoin2Apply::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformSubqJoin2Apply::Transform
	(
	CXformContext *pxfctxt,
	CXformResult *pxfres,
	CExpression *pexpr
	)
	const
{
	Transform(pxfctxt, pxfres, pexpr, false /*fEnforceCorrelatedApply*/);
	Transform(pxfctxt, pxfres, pexpr, true /*fEnforceCorrelatedApply*/);
}


// EOF

