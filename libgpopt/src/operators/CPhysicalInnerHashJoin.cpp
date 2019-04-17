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
	
	
	return DeriveHashSpecUsingEquivClasses(mp, pdshashedInner, exprhdl);
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
	 ,
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
	
	return DeriveHashSpecUsingEquivClasses(mp, pdshashedOuter, exprhdl);
}

CDistributionSpecHashed *
CPhysicalInnerHashJoin::DeriveHashSpecUsingEquivClasses
	(
	IMemoryPool *mp,
	CDistributionSpecHashed *spec,
	CExpressionHandle &exprhdl
	)
	const
{
	CExpressionArrays *equiv_hash_dist_scalar_ident_exprs_final = GPOS_NEW(mp) CExpressionArrays(mp);
	CColRefSetArray *equiv_hash_dist_scalar_ident_colrefset = GPOS_NEW(mp) CColRefSetArray(mp);
	CDistributionSpecHashed *spec_temp = spec;
	while (spec_temp)
	{
		CExpressionArray *dist_scalar_ident_exprs = spec_temp->Pdrgpexpr();
		CColRefSet *dist_scalar_ident_colrefset = spec_temp->PcrsUsed(mp);
		dist_scalar_ident_exprs->AddRef();
		equiv_hash_dist_scalar_ident_exprs_final->Append(dist_scalar_ident_exprs);
		equiv_hash_dist_scalar_ident_colrefset->Append(dist_scalar_ident_colrefset);
		spec_temp = spec_temp->PdshashedEquiv();
	}

	for (ULONG scalar_ident_idx = 0; scalar_ident_idx < spec->Pdrgpexpr()->Size(); scalar_ident_idx++)
	{
		CExpression *dist_scalar_ident_expr = (*spec->Pdrgpexpr())[scalar_ident_idx];
		CDrvdPropRelational *relational_prop = exprhdl.GetRelationalProperties();
		
		// get an array containing equivalent scalar idents for the current distribution scalar ident
		CExpressionArray *equiv_dist_scalar_ident_exprs = CUtils::GetEquivScalarIdentExprs(mp, relational_prop, dist_scalar_ident_expr);
		
		// using the equivalent scalar idents for the current distribution scalar ident, create an array containing
		// all the scalar idents required for the distribution key
		// for example:
		// if the distribution spec contains columns t1.a, t1.b
		// where t1.a is equivalent to t2.a
		// create an array containing t2.a, t1.b
		CUtils::ExtractEquivDistributionKeyArrays
					(
					mp,
					spec->Pdrgpexpr(),
					equiv_dist_scalar_ident_exprs,
					scalar_ident_idx,
					equiv_hash_dist_scalar_ident_exprs_final,
					equiv_hash_dist_scalar_ident_colrefset
					);
		CRefCount::SafeRelease(equiv_dist_scalar_ident_exprs);
	}

	// iterate over an array of distribution scalar ident array, and create distribution spec where spec generated using
	// every distribution scalar ident array is equivalent to the spec generated for the other scalar ident array
	// for example: if the array contains
	// {t1.a, t1.b}, {t2.a, t1.b}
	// create resulting spec where Spec(t1.a, t1.b) -> equivalent Spec (t2.a, t1.b) -> equivalent Spec (NULL)
	CDistributionSpecHashed *result_hash_spec = NULL;
	CDistributionSpecHashed *hash_spec_last = NULL;
	INT last_elem_idx = equiv_hash_dist_scalar_ident_exprs_final->Size() - 1;
	CColRefSet *equiv_hash_scalar_ident_processed_colrefset = GPOS_NEW(mp) CColRefSet(mp);
	for (INT id = last_elem_idx; id >= 0; id--)
	{
		CExpressionArray *dist_scalar_ident_exprs = (*equiv_hash_dist_scalar_ident_exprs_final)[id];
		CColRefSet *dist_scalar_ident_colrefset = (*equiv_hash_dist_scalar_ident_colrefset)[id];
//		if (equiv_hash_scalar_ident_processed_colrefset->ContainsAll(dist_scalar_ident_colrefset))
//		{
//			continue;
//		}
		equiv_hash_scalar_ident_processed_colrefset->Include(dist_scalar_ident_colrefset);
		dist_scalar_ident_exprs->AddRef();
		if (hash_spec_last)
		{
			result_hash_spec = GPOS_NEW(mp) CDistributionSpecHashed(dist_scalar_ident_exprs,
																	spec->FNullsColocated(),
																	hash_spec_last);
		}
		else
		{
			result_hash_spec = GPOS_NEW(mp) CDistributionSpecHashed(dist_scalar_ident_exprs,
																	spec->FNullsColocated());
		}
		hash_spec_last = result_hash_spec;
	}
	equiv_hash_dist_scalar_ident_exprs_final->Release();
	equiv_hash_dist_scalar_ident_colrefset->Release();
	equiv_hash_scalar_ident_processed_colrefset->Release();
	return hash_spec_last;
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
	
	if (exprhdl.Pgexpr() != NULL)
		exprhdl.Pgexpr()->DbgPrint();
	else
		exprhdl.Pexpr()->DbgPrint();
	pdsOuter->DbgPrint();
	pdsInner->DbgPrint();

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

