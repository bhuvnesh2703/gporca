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
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/operators/CScalarIdent.h"

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
	CDistributionSpecHashed *pdsHashedMatchingEquivalents = GPOS_NEW(mp)
								CDistributionSpecHashed(
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
	CDistributionSpec *pdsInner,
	CExpressionHandle &exprhdl
	)
	const
{
	GPOS_ASSERT(NULL != pdsOuter);
	GPOS_ASSERT(NULL != pdsInner);
	GPOS_ASSERT(CDistributionSpec::EdtReplicated == pdsOuter->Edt());

	CDistributionSpecHashed *pdshashedInner = CDistributionSpecHashed::PdsConvert(pdsInner);
	// if outer child is replicated, join results distribution is defined by inner child
	if (CDistributionSpec::EdtHashed == pdsInner->Edt())
	{
		
		if (pdshashedInner->CoversRequiredCols(PdrgpexprInnerKeys()))
		{
			// inner child is hashed on a subset of inner hashkeys,
		 	// return a hashed distribution equivalent to a matching outer distribution
			return PdshashedCreateMatching(mp, pdshashedInner, 1 /*ulSourceChild*/);
		}
	}
	
	
	return CreateEquivHashSpec(mp, pdshashedInner, exprhdl);
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
	pdsInner,
#endif // GPOS_DEBUG
	CExpressionHandle &exprhdl
	)
	const
{
	GPOS_ASSERT(NULL != pdsOuter);
	GPOS_ASSERT(NULL != pdsInner);

	GPOS_ASSERT(CDistributionSpec::EdtHashed == pdsOuter->Edt());



	CDistributionSpecHashed *pdshashedOuter = CDistributionSpecHashed::PdsConvert(pdsOuter);
	 if (pdshashedOuter->CoversRequiredCols(PdrgpexprOuterKeys()))
	 {
	 	// outer child is hashed on a subset of outer hashkeys,
	 	// return a hashed distribution equivalent to a matching outer distribution
		return PdshashedCreateMatching(mp, pdshashedOuter, 0 /*ulSourceChild*/);
	 }
	
	return CreateEquivHashSpec(mp, pdshashedOuter, exprhdl);
}


CDistributionSpecHashed *
CPhysicalInnerHashJoin::CreateEquivHashSpec
	(
	IMemoryPool *mp,
	CDistributionSpecHashed *pdsHashed,
	CExpressionHandle &exprhdl
	)
	const
{
	CExpressionArrays *equiv_dist_keys = GPOS_NEW(mp) CExpressionArrays(mp);
	CColRefSetArray *dist_key_colrefsets = GPOS_NEW(mp) CColRefSetArray(mp);
	for (ULONG id = 0; id < pdsHashed->Pdrgpexpr()->Size(); id++)
	{
		CExpression *dist_key_expr = (*pdsHashed->Pdrgpexpr())[id];
		CDrvdPropRelational *drvd_prop_relational = exprhdl.GetRelationalProperties();
		CExpressionArray *pexprEquivIdentArray = CUtils::GetEquivScalarIdents(mp, drvd_prop_relational, dist_key_expr);
		CUtils::ExtractEquivDistributionKeyArrays(mp, pdsHashed->Pdrgpexpr(), pexprEquivIdentArray, id, equiv_dist_keys, dist_key_colrefsets);
	}

	CDistributionSpecHashed *pdsHashedSpec = pdsHashed->GetHashedEquivSpecs(mp, equiv_dist_keys, dist_key_colrefsets);
	equiv_dist_keys->Release();
	dist_key_colrefsets->Release();
	return pdsHashedSpec;
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
 		return PdsDeriveFromReplicatedOuter(mp, pdsOuter, pdsInner, exprhdl);
 	}

 	if (CDistributionSpec::EdtHashed == pdsOuter->Edt())
 	{
 		CDistributionSpec *pdsDerived = PdsDeriveFromHashedOuter(mp, pdsOuter, pdsInner, exprhdl);
 		 if (NULL != pdsDerived)
 		 {
 		 	return pdsDerived;
 		 }
 	 }

 	// otherwise, pass through outer distribution
 	pdsOuter->AddRef();
 	return pdsOuter;
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

