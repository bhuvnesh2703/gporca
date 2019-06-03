//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Greenplum, Inc.
//
//	@filename:
//		CPhysicalInnerIndexNLJoin.cpp
//
//	@doc:
//		Implementation of index inner nested-loops join operator
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecNonSingleton.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"


#include "gpopt/operators/CPhysicalInnerIndexNLJoin.h"



using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::CPhysicalInnerIndexNLJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalInnerIndexNLJoin::CPhysicalInnerIndexNLJoin
	(
	CMemoryPool *mp,
	CColRefArray *colref_array
	)
	:
	CPhysicalInnerNLJoin(mp),
	m_pdrgpcrOuterRefs(colref_array)
{
//	m_pexprScalar = NULL;
	GPOS_ASSERT(NULL != colref_array);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::~CPhysicalInnerIndexNLJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalInnerIndexNLJoin::~CPhysicalInnerIndexNLJoin()
{
	m_pdrgpcrOuterRefs->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CPhysicalInnerIndexNLJoin::Matches
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		return m_pdrgpcrOuterRefs->Equals(CPhysicalInnerIndexNLJoin::PopConvert(pop)->PdrgPcrOuterRefs());
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerIndexNLJoin::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child;
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalInnerIndexNLJoin::PdsRequired
	(
	CMemoryPool *mp,
	CExpressionHandle &exprhdl,
	CDistributionSpec *,//pdsRequired,
	ULONG child_index,
	CDrvdProp2dArray *pdrgpdpCtxt,
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(2 > child_index);

	if (1 == child_index)
	{
		// inner (index-scan side) is requested for Any distribution,
		// we allow outer references on the inner child of the join since it needs
		// to refer to columns in join's outer child
		return GPOS_NEW(mp) CDistributionSpecAny(this->Eopid(), true /*fAllowOuterRefs*/);
	}

	// we need to match distribution of inner
	CDistributionSpec *pdsInner = CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();
	CDistributionSpec::EDistributionType edtInner = pdsInner->Edt();
	if (CDistributionSpec::EdtSingleton == edtInner ||
		CDistributionSpec::EdtStrictSingleton == edtInner ||
		CDistributionSpec::EdtUniversal == edtInner)
	{
		// enforce executing on a single host
		return GPOS_NEW(mp) CDistributionSpecSingleton();
	}

	if (CDistributionSpec::EdtHashed == edtInner)
	{
		// check if we could create an equivalent hashed distribution request to the inner child
		CDistributionSpecHashed *pdshashed = CDistributionSpecHashed::PdsConvert(pdsInner);
		CColRefSet *pcrsInner = pdshashed->PcrsUsed(mp);
		CDistributionSpecHashed *pdshashedEquiv = pdshashed->PdshashedEquiv();
		CExpression *pexprScalar = CPhysicalNLJoin::PopConvert(exprhdl.Pop())->ScalarExpr();
		CExpressionArray *pdrgpexprPreds = CPredicateUtils::PdrgpexprConjuncts(mp, pexprScalar);
		CExpressionArray *pdsHashExprs = GPOS_NEW(mp) CExpressionArray(mp);
		for (ULONG ul = 0; ul < pdrgpexprPreds->Size(); ul++)
		{
			CExpression *pexprPred = (*pdrgpexprPreds)[ul];
			if (pexprPred->Pop()->Eopid() == COperator::EopScalarConst)
				continue;
			CExpression *pexprLeft = (*pexprPred)[0];
			CColRefSet *pcrsLeft = CDrvdPropScalar::GetDrvdScalarProps(pexprLeft->PdpDerive())->PcrsUsed();
			CExpression *pexprRight = (*pexprPred)[1];
			CColRefSet *pcrsRight = CDrvdPropScalar::GetDrvdScalarProps(pexprRight->PdpDerive())->PcrsUsed();
			if (pcrsLeft->ContainsAll(pcrsInner))
			{
				pexprRight->AddRef();
				pdsHashExprs->Append(pexprRight);
			}
			else if (pcrsRight->ContainsAll(pcrsInner))
			{
				pexprLeft->AddRef();
				pdsHashExprs->Append(pexprLeft);
			}
			GPOS_ASSERT(pdsHashExprs);
			
			
		}
		pcrsInner->Release();
		CRefCount::SafeRelease(pdrgpexprPreds);
		if (pdsHashExprs->Size() > 0)
		{
			CDistributionSpecHashed *pdsHashedRequired = GPOS_NEW(mp) CDistributionSpecHashed(pdsHashExprs, pdshashed->FNullsColocated());
			pdsHashedRequired->ComputeEquivHashExprs(mp, exprhdl);
			return pdsHashedRequired;
		}
		pdsHashExprs->Release();
		GPOS_ASSERT(pexprScalar);
		if (NULL != pdshashedEquiv)
		{
			// request hashed distribution from outer
			pdshashedEquiv->Pdrgpexpr()->AddRef();
			CDistributionSpecHashed *pdsHashedRequired = GPOS_NEW(mp) CDistributionSpecHashed(pdshashedEquiv->Pdrgpexpr(), pdshashedEquiv->FNullsColocated());
			pdsHashedRequired->ComputeEquivHashExprs(mp, exprhdl);
			return pdsHashedRequired;
		}
	}

	// otherwise, require outer child to be replicated
	return GPOS_NEW(mp) CDistributionSpecReplicated();
}


// EOF

