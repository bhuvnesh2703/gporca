//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalInnerHashJoin.cpp
//
//	@doc:
//		Implementation of inner hash join operator
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/operators/CExpressionHandle.h"

#include "gpopt/operators/CPhysicalInnerHashJoin.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::CPhysicalInnerHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalInnerHashJoin::CPhysicalInnerHashJoin
	(
	IMemoryPool *mp,
	CExpressionArray *pdrgpexprOuterKeys,
	CExpressionArray *pdrgpexprInnerKeys
	)
	:
	CPhysicalHashJoin(mp, pdrgpexprOuterKeys, pdrgpexprInnerKeys)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::~CPhysicalInnerHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalInnerHashJoin::~CPhysicalInnerHashJoin()
{
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::PdshashedCreateMatching
//
//	@doc:
//		Helper function for creating a matching hashed distribution
//
//---------------------------------------------------------------------------
CDistributionSpecHashed *
CPhysicalInnerHashJoin::PdshashedCreateMatching
	(
	IMemoryPool *mp,
	CDistributionSpecHashed *pdshashed,
	ULONG ulSourceChild // index of child that delivered the given hashed distribution
	)
	const
{
	GPOS_ASSERT(NULL != pdshashed);

 	CDistributionSpecHashed *pdshashedMatching = PdshashedMatching(mp, pdshashed, ulSourceChild);
	pdshashedMatching->Pdrgpexpr()->AddRef();
	pdshashed->AddRef();
	
	CDistributionSpecHashed *pdsHashedMatchingEquivalents = GPOS_NEW(mp) CDistributionSpecHashed
	(
	pdshashedMatching->Pdrgpexpr(),
	pdshashedMatching->FNullsColocated(),
	pdshashed // matching distribution spec is equivalent to passed distribution spec
	);
	pdshashedMatching->Release();
	return pdsHashedMatchingEquivalents;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::PdsDeriveFromHashedChildren
//
//	@doc:
//		Derive hash join distribution from hashed children;
//		return NULL if derivation failed
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalInnerHashJoin::PdsDeriveFromHashedChildren
	(
	IMemoryPool *mp,
	CDistributionSpec *pdsOuter,
	CDistributionSpec *pdsInner
	)
	const
{
	GPOS_ASSERT(NULL != pdsOuter);
	GPOS_ASSERT(NULL != pdsInner);

	CDistributionSpecHashed *pdshashedOuter = CDistributionSpecHashed::PdsConvert(pdsOuter);
 	CDistributionSpecHashed *pdshashedInner = CDistributionSpecHashed::PdsConvert(pdsInner);

	if (pdshashedOuter->CoversRequiredCols(PdrgpexprOuterKeys()) && pdshashedInner->CoversRequiredCols(PdrgpexprInnerKeys()))
 	{
 	 	// if both sides are hashed on subsets of hash join keys, join's output can be
 		// seen as distributed on outer spec or (equivalently) on inner spec,
 	 	// in this case, we create a new spec based on outer side and mark inner
 		// side as an equivalent one,

		return PdshashedCreateMatching(mp, pdshashedOuter, 0 /*ulSourceChild*/);
 	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::PdsDeriveFromReplicatedOuter
//
//	@doc:
//		Derive hash join distribution from a replicated outer child;
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalInnerHashJoin::PdsDeriveFromReplicatedOuter
	(
	IMemoryPool *mp,
	CDistributionSpec *
#ifdef GPOS_DEBUG
	pdsOuter
#endif // GPOS_DEBUG
	,
	CDistributionSpec *pdsInner
	)
	const
{
	GPOS_ASSERT(NULL != pdsOuter);
	GPOS_ASSERT(NULL != pdsInner);
	GPOS_ASSERT(CDistributionSpec::EdtReplicated == pdsOuter->Edt());

	// if outer child is replicated, join results distribution is defined by inner child
	if (CDistributionSpec::EdtHashed == pdsInner->Edt())
	{
		CDistributionSpecHashed *pdshashedInner = CDistributionSpecHashed::PdsConvert(pdsInner);
		if (pdshashedInner->CoversRequiredCols(PdrgpexprInnerKeys()))
		{
			// inner child is hashed on a subset of inner hashkeys,
		 	// return a hashed distribution equivalent to a matching outer distribution
			return PdshashedCreateMatching(mp, pdshashedInner, 1 /*ulSourceChild*/);
		}
	}

	// otherwise, pass-through inner distribution
	pdsInner->AddRef();
	return pdsInner;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::PdsDeriveFromHashedOuter
//
//	@doc:
//		Derive hash join distribution from a hashed outer child;
//		return NULL if derivation failed
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalInnerHashJoin::PdsDeriveFromHashedOuter
	(
	IMemoryPool *mp,
	CDistributionSpec *pdsOuter,
	CDistributionSpec *
#ifdef GPOS_DEBUG
	pdsInner
#endif // GPOS_DEBUG
	)
	const
{
	GPOS_ASSERT(NULL != pdsOuter);
	GPOS_ASSERT(NULL != pdsInner);

	GPOS_ASSERT(CDistributionSpec::EdtHashed == pdsOuter->Edt());

	 CDistributionSpecHashed *pdshashedOuter = CDistributionSpecHashed::PdsConvert(pdsOuter);
//	for (ULONG ul = 0; ul < PdrgpexprOuterKeys()->Size(); ul++)
//	{
//		CExpression *pexpr = (*PdrgpexprOuterKeys())[ul];
//		pexpr->DbgPrint();
//		GPOS_ASSERT(pexpr);
//	}
//	for (ULONG ul = 0; ul < PdrgpexprInnerKeys()->Size(); ul++)
//	{
//		CExpression *pexpr = (*PdrgpexprInnerKeys())[ul];
//		pexpr->DbgPrint();
//		GPOS_ASSERT(pexpr);
//	}
	if (pdshashedOuter->CoversRequiredCols(PdrgpexprOuterKeys()))
	 {
	 	// outer child is hashed on a subset of outer hashkeys,
	 	// return a hashed distribution equivalent to a matching outer distribution
		return PdshashedCreateMatching(mp, pdshashedOuter, 0 /*ulSourceChild*/);
	 }
	
	CDistributionSpecHashed *pds = pdshashedOuter;
	CDistributionSpecHashed *pdsFinal = NULL;
	while (pds)
	{
		pds->Pdrgpexpr()->AddRef();
		CDistributionSpecHashed *pdsTemp = GPOS_NEW(mp) CDistributionSpecHashed(pds->Pdrgpexpr(), pds->FNullsColocated());
		CDistributionSpecHashed *pdsTempEquiv = PdsDeriveEquivInnerSpec(mp, pdsTemp);
		if (pdsTempEquiv == NULL)
		{
			if (pdsFinal != NULL)
			{
				pdsTemp->Pdrgpexpr()->AddRef();
				pdsFinal = GPOS_NEW(mp) CDistributionSpecHashed(pdsTemp->Pdrgpexpr(),
																pdsTemp->FNullsColocated(),
																pdsFinal);
			}
			else
			{
				pdsTemp->Pdrgpexpr()->AddRef();
				pdsFinal = GPOS_NEW(mp) CDistributionSpecHashed(pdsTemp->Pdrgpexpr(),
																pdsTemp->FNullsColocated());
			}
			pdsTemp->Release();
			pds = pds->PdshashedEquiv();
			continue;
		}
		CDistributionSpecHashed *pdsTempEquivAndPrevious = NULL;
		if (pdsFinal == NULL)
		{
			pdsTemp->AddRef();
			pdsTempEquivAndPrevious = pdsTemp;
		}
		else
		{
			
			pdsTemp->Pdrgpexpr()->AddRef();
			pdsTempEquivAndPrevious = GPOS_NEW(mp) CDistributionSpecHashed(pdsTemp->Pdrgpexpr(),
																		   pdsTemp->FNullsColocated(),
																		   pdsFinal);
		}

		pdsTempEquiv->Pdrgpexpr()->AddRef();
		pdsFinal = GPOS_NEW(mp) CDistributionSpecHashed(pdsTempEquiv->Pdrgpexpr(),
														pdsTempEquiv->FNullsColocated(),
														pdsTempEquivAndPrevious);
		pdsTempEquiv->Release();
		pdsTemp->Release();
		pds = pds->PdshashedEquiv();
	}

//	pdsFinal->DbgPrint();
	return pdsFinal;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalInnerHashJoin::PdsDerive
(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl
	)
	const
{
	CDistributionSpec *pdsOuter = exprhdl.Pdpplan(0 /*child_index*/)->Pds();
 	CDistributionSpec *pdsInner = exprhdl.Pdpplan(1 /*child_index*/)->Pds();

//	if (exprhdl.Pgexpr() != NULL && pdsOuter->Edt() == CDistributionSpec::EdtHashed && pdsInner->Edt() == CDistributionSpec::EdtReplicated)
//	{
//		ULONG id1 = (*exprhdl.Pgexpr())[0]->Id();
//		ULONG id2 = (*exprhdl.Pgexpr())[1]->Id();
//		if (id1 == 14 && id2 == 2)
//		{
//			GPOS_ASSERT(id1);
//		}
//	}
 	if (CDistributionSpec::EdtUniversal == pdsOuter->Edt())
 	{
 		// if outer is universal, pass through inner distribution
 		pdsInner->AddRef();
 		return pdsInner;
 	}

 	if (CDistributionSpec::EdtHashed == pdsOuter->Edt() && CDistributionSpec::EdtHashed == pdsInner->Edt())
 	{
 		CDistributionSpec *pdsDerived = PdsDeriveFromHashedChildren(mp, pdsOuter, pdsInner);
 		if (NULL != pdsDerived)
 		{
 			return pdsDerived;
 		}
 	}

 	if (CDistributionSpec::EdtReplicated == pdsOuter->Edt())
 	{
 		return PdsDeriveFromReplicatedOuter(mp, pdsOuter, pdsInner);
 	}

 	if (CDistributionSpec::EdtHashed == pdsOuter->Edt())
 	{
 		CDistributionSpec *pdsDerived = PdsDeriveFromHashedOuter(mp, pdsOuter, pdsInner);
 		 if (NULL != pdsDerived)
 		 {
 		 	return pdsDerived;
 		 }
 	 }

 	// otherwise, pass through outer distribution
 	pdsOuter->AddRef();
 	return pdsOuter;
}

CDistributionSpecHashed *
CPhysicalInnerHashJoin::PdsDeriveEquivInnerSpec
	(
	IMemoryPool *mp,
	CDistributionSpecHashed *pdsOuterHashed
	)
	const
{
	CExpressionArray *pdsOuterKeys = const_cast<CExpressionArray *>(PdrgpexprOuterKeys());
	CColRefSet *pcrsDistKeys = GPOS_NEW(mp) CColRefSet(mp);
	for (ULONG ul = 0; ul < pdsOuterKeys->Size(); ul++)
	{
		CExpression *pexprDistKeys = (*pdsOuterKeys)[ul];
		CColRefSet *pcrsDistKey = CDrvdPropScalar::GetDrvdScalarProps(pexprDistKeys->PdpDerive())->PcrsUsed();
		pcrsDistKeys->Union(pcrsDistKey);
	}

	CExpressionArray *pexprResultingInnerKeys = GPOS_NEW(mp) CExpressionArray(mp);
	CColRefSet *pcrsJoinOuterCols = GPOS_NEW(mp) CColRefSet(mp);
	for (ULONG ul = 0; ul < PdrgpexprOuterKeys()->Size(); ul++)
	{
		CExpression *pexprJoinOuterCol = (*PdrgpexprOuterKeys())[ul];
		CColRefSet *pcrsJoinOuterCol = CDrvdPropScalar::GetDrvdScalarProps(pexprJoinOuterCol->PdpDerive())->PcrsUsed();
		CExpression *pexprJoinInnerCol = (*PdrgpexprInnerKeys())[ul];
		if (pcrsDistKeys->FIntersects(pcrsJoinOuterCol))
		{
			pcrsJoinOuterCols->Union(pcrsJoinOuterCol);
			pexprJoinInnerCol->AddRef();
			pexprResultingInnerKeys->Append(pexprJoinInnerCol);
		}
	}
	
	CColRefSet *pdsHashedUsed = pdsOuterHashed->PcrsUsed(mp);
	CColRefSet *pcrsRemainingDistKeys = GPOS_NEW(mp) CColRefSet(mp, *pdsHashedUsed);
	pcrsRemainingDistKeys->Difference(pcrsJoinOuterCols);
	CExpressionArray *pdsHashOuterKeys = pdsOuterHashed->Pdrgpexpr();
	for (ULONG ul = 0; ul < pdsHashOuterKeys->Size(); ul++)
	{
		CExpression *pexprJoinOuterCol = (*pdsHashOuterKeys)[ul];
		CColRefSet *pcrsJoinOuterCol = CDrvdPropScalar::GetDrvdScalarProps(pexprJoinOuterCol->PdpDerive())->PcrsUsed();
		if (pcrsRemainingDistKeys->FIntersects(pcrsJoinOuterCol))
		{
			pexprJoinOuterCol->AddRef();
			pexprResultingInnerKeys->Append(pexprJoinOuterCol);
		}
	}

	CDistributionSpecHashed *pds = GPOS_NEW(mp) CDistributionSpecHashed(pexprResultingInnerKeys,
																  pdsOuterHashed->FNullsColocated());

	pcrsJoinOuterCols->Release();
	pcrsRemainingDistKeys->Release();
	pcrsDistKeys->Release();
	pdsHashedUsed->Release();

	return pds;
}
//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerHashJoin::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalInnerHashJoin::PppsRequired
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	CPartitionPropagationSpec *pppsRequired,
	ULONG child_index,
	CDrvdProp2dArray *pdrgpdpCtxt,
	ULONG ulOptReq
	)
{

	if (1 == ulOptReq)
	{
		// request (1): push partition propagation requests to join's children,
		// do not consider possible dynamic partition elimination using join predicate here,
		// this is handled by optimization request (0) below
		return CPhysical::PppsRequiredPushThruNAry(mp, exprhdl, pppsRequired, child_index);
	}

	// request (0): push partition progagation requests to join child considering
	// DPE possibility. For HJ, PS request is pushed to the inner child if there
	// is a consumer (DTS) on the outer side of the join.
	GPOS_ASSERT(0 == ulOptReq);
	return PppsRequiredJoinChild(mp, exprhdl, pppsRequired, child_index, pdrgpdpCtxt, false);
}

// EOF

