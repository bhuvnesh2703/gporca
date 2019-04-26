//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDistributionSpecHashed.cpp
//
//	@doc:
//		Specification of hashed distribution
//---------------------------------------------------------------------------

#include "naucrates/traceflags/traceflags.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CCastUtils.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/operators/CPhysicalMotionBroadcast.h"
#include "gpopt/operators/CPhysicalMotionHashDistribute.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CExpressionPreprocessor.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CExpressionHandle.h"


#define GPOPT_DISTR_SPEC_HASHED_EXPRESSIONS      (ULONG(5))

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::CDistributionSpecHashed
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDistributionSpecHashed::CDistributionSpecHashed
	(
	CExpressionArray *pdrgpexpr,
	BOOL fNullsColocated
	)
	:
	m_pdrgpexpr(pdrgpexpr),
	m_fNullsColocated(fNullsColocated),
	m_pdshashedEquiv(NULL),
	m_hash_idents_equiv_cols(NULL),
	m_hash_idents_equiv_exprs(NULL)
{
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(0 < pdrgpexpr->Size());
}

CDistributionSpecHashed::CDistributionSpecHashed
(
 CExpressionArray *pdrgpexpr,
 BOOL fNullsColocated,
 CColRefSetArray *hash_idents_equiv_cols
 )
:
m_pdrgpexpr(pdrgpexpr),
m_fNullsColocated(fNullsColocated),
m_pdshashedEquiv(NULL),
m_hash_idents_equiv_cols(hash_idents_equiv_cols),
m_hash_idents_equiv_exprs(NULL)
{
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(0 < pdrgpexpr->Size());
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::CDistributionSpecHashed
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDistributionSpecHashed::CDistributionSpecHashed
	(
	CExpressionArray *pdrgpexpr,
	BOOL fNullsColocated,
	CDistributionSpecHashed *pdshashedEquiv
	)
	:
	m_pdrgpexpr(pdrgpexpr),
	m_fNullsColocated(fNullsColocated),
	m_pdshashedEquiv(pdshashedEquiv),
	m_hash_idents_equiv_cols(NULL),
	m_hash_idents_equiv_exprs(NULL)
{
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(0 < pdrgpexpr->Size());
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::~CDistributionSpecHashed
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDistributionSpecHashed::~CDistributionSpecHashed()
{
	m_pdrgpexpr->Release();
	CRefCount::SafeRelease(m_pdshashedEquiv);
	CRefCount::SafeRelease(m_hash_idents_equiv_cols);
	CRefCount::SafeRelease(m_hash_idents_equiv_exprs);
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::PdsCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the distribution spec with remapped columns
//
//---------------------------------------------------------------------------
CDistributionSpec *
CDistributionSpecHashed::PdsCopyWithRemappedColumns
	(
	IMemoryPool *mp,
	UlongToColRefMap *colref_mapping,
	BOOL must_exist
	)
{
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG length = m_pdrgpexpr->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CExpression *pexpr = (*m_pdrgpexpr)[ul];
		pdrgpexpr->Append(pexpr->PexprCopyWithRemappedColumns(mp, colref_mapping, must_exist));
	}

	if (NULL == m_pdshashedEquiv)
	{
		return GPOS_NEW(mp) CDistributionSpecHashed(pdrgpexpr, m_fNullsColocated);
	}

	// copy equivalent distribution
	CDistributionSpec *pds = m_pdshashedEquiv->PdsCopyWithRemappedColumns(mp, colref_mapping, must_exist);
	CDistributionSpecHashed *pdshashed = CDistributionSpecHashed::PdsConvert(pds);
	return GPOS_NEW(mp) CDistributionSpecHashed(pdrgpexpr, m_fNullsColocated, pdshashed);
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::FSatisfies
//
//	@doc:
//		Check if this distribution spec satisfies the given one
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecHashed::FSatisfies
	(
	const CDistributionSpec *pds
	)
	const
{	
	if (NULL != m_pdshashedEquiv && m_pdshashedEquiv->FSatisfies(pds))
 	{
 		return true;
 	}

	if (Matches(pds))
	{
		// exact match implies satisfaction
		return true;
	 }

	if (EdtAny == pds->Edt() || EdtNonSingleton == pds->Edt())
	{
		// hashed distribution satisfies the "any" and "non-singleton" distribution requirement
		return true;
	}

	if (EdtHashed != pds->Edt())
	{
		return false;
	}
	
	GPOS_ASSERT(EdtHashed == pds->Edt());
	
	const CDistributionSpecHashed *pdsHashed =
			dynamic_cast<const CDistributionSpecHashed *>(pds);

	return FMatchSubset(pdsHashed);
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::FMatchSubset
//
//	@doc:
//		Check if the expressions of the object match a subset of the passed spec's
//		expressions
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecHashed::FMatchSubset
	(
	const CDistributionSpecHashed *pdsHashed
	)
	const
{
	const ULONG ulOwnExprs = m_pdrgpexpr->Size();
	const ULONG ulOtherExprs = pdsHashed->m_pdrgpexpr->Size();

	if (ulOtherExprs < ulOwnExprs || !FNullsColocated(pdsHashed) || !FDuplicateSensitiveCompatible(pdsHashed))
	{
		return false;
	}

	for (ULONG ulOuter = 0; ulOuter < ulOwnExprs; ulOuter++)
	{
		CExpression *pexprOwn = CCastUtils::PexprWithoutBinaryCoercibleCasts((*m_pdrgpexpr)[ulOuter]);
		CExpressionArrays *equi_exprs = m_hash_idents_equiv_exprs;

		BOOL fFound = false;
		for (ULONG ulInner = 0; ulInner < ulOtherExprs; ulInner++)
		{
			CExpression *pexprOther = CCastUtils::PexprWithoutBinaryCoercibleCasts((*(pdsHashed->m_pdrgpexpr))[ulInner]);
			if (CUtils::Equals(pexprOwn, pexprOther))
			{
				fFound = true;
				break;
			}
			
			if (equi_exprs != NULL && equi_exprs->Size() > 0)
			{
				BOOL innerfFound = false;
				CExpressionArray *equiv_exprs = (*equi_exprs)[ulOuter];
				for (ULONG id = 0; id < equiv_exprs->Size() && equiv_exprs->Size() >0; id++)
				{
					CExpression *pexprOwn = (*equiv_exprs)[id];
					if (CUtils::Equals(pexprOwn, pexprOther))
					{
						innerfFound = true;
						break;
					}
				}
				if (innerfFound)
				{
					fFound = true;
					break;
				}
			}
		}
		
		

		if (!fFound)
		{
			return false;
		}
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::PdshashedExcludeColumns
//
//	@doc:
//		Return a copy of the distribution spec after excluding the given
//		columns, return NULL if all distribution expressions are excluded
//
//---------------------------------------------------------------------------
CDistributionSpecHashed *
CDistributionSpecHashed::PdshashedExcludeColumns
	(
	IMemoryPool *mp,
	CColRefSet *pcrs
	)
{
	GPOS_ASSERT(NULL != pcrs);

	CExpressionArray *pdrgpexprNew = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG ulExprs = m_pdrgpexpr->Size();
	for (ULONG ul = 0; ul < ulExprs; ul++)
	{
		CExpression *pexpr = (*m_pdrgpexpr)[ul];
		COperator *pop = pexpr->Pop();
		if (COperator::EopScalarIdent == pop->Eopid())
		{
			// we only care here about column identifiers,
			// any more complicated expressions are copied to output
			const CColRef *colref = CScalarIdent::PopConvert(pop)->Pcr();
			if (pcrs->FMember(colref))
			{
				continue;
			}
		}

		pexpr->AddRef();
		pdrgpexprNew->Append(pexpr);
	}

	if (0 == pdrgpexprNew->Size())
	{
		pdrgpexprNew->Release();
		return NULL;
	}

	return GPOS_NEW(mp) CDistributionSpecHashed(pdrgpexprNew, m_fNullsColocated);
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecHashed::Equals
(
 const CDistributionSpecHashed *pds
 )
const
{
	if (pds->Edt() != this->Edt())
	{
		return false;
	}
	
	const CDistributionSpecHashed *pdshashed = CDistributionSpecHashed::PdsConvert(pds);
	CDistributionSpecHashed *pdsThis = this->PdshashedEquiv();
	CDistributionSpecHashed *pdsHashed = pdshashed->PdshashedEquiv();
	
	if ((pdsThis != NULL && pdshashed == NULL) || (pdsThis == NULL && pdsHashed != NULL))
		return false;
	
	BOOL equals = true;
	if (pdsThis != NULL && pdsHashed != NULL)
	{
		equals = pdsThis->Equals(pdsHashed);
	}
	
	
	if (!equals)
		return false;
	
	BOOL matches = m_fNullsColocated == pdshashed->FNullsColocated() &&
	m_is_duplicate_sensitive == pdshashed->IsDuplicateSensitive() &&
	m_fSatisfiedBySingleton == pdshashed->FSatisfiedBySingleton() &&
	CUtils::Equals(m_pdrgpexpr, pdshashed->m_pdrgpexpr) &&
	Edt() == pdshashed->Edt();
	
	if (!matches)
		return false;
	
	CExpressionArrays *thisexprarrays = HashSpecEquivExprs();
	CExpressionArrays *hashedexprarrays = pdshashed->HashSpecEquivExprs();
	
	if ((thisexprarrays == NULL && hashedexprarrays != NULL) || (thisexprarrays != NULL && hashedexprarrays == NULL))
		return false;
	
	if (thisexprarrays == NULL && hashedexprarrays == NULL)
		return true;
	
	if (thisexprarrays->Size() != hashedexprarrays->Size())
		return false;
	
	BOOL match = true;
	for (ULONG id = 0; id < thisexprarrays->Size() && match; id++)
	{
		match = CUtils::Equals((*thisexprarrays)[id], (*hashedexprarrays)[id]);
	}
	
	
	return match;
	
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::AppendEnforcers
//
//	@doc:
//		Add required enforcers to dynamic array
//
//---------------------------------------------------------------------------
void
CDistributionSpecHashed::AppendEnforcers
	(
	IMemoryPool *mp,
	CExpressionHandle &, // exprhdl
	CReqdPropPlan *
#ifdef GPOS_DEBUG
	prpp
#endif // GPOS_DEBUG
	,
	CExpressionArray *pdrgpexpr,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(!GPOS_FTRACE(EopttraceDisableMotions));
	GPOS_ASSERT(this == prpp->Ped()->PdsRequired() &&
	            "required plan properties don't match enforced distribution spec");

	if (GPOS_FTRACE(EopttraceDisableMotionHashDistribute))
	{
		// hash-distribute Motion is disabled
		return;
	}

	// add a hashed distribution enforcer
	AddRef();
	pexpr->AddRef();
	CExpression *pexprMotion = GPOS_NEW(mp) CExpression
										(
										mp,
										GPOS_NEW(mp) CPhysicalMotionHashDistribute(mp, this),
										pexpr
										);
	pdrgpexpr->Append(pexprMotion);
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG 
CDistributionSpecHashed::HashValue() const
{
	ULONG ulHash = (ULONG) Edt();
	
	CDistributionSpecHashed *pdsTemp = this->PdshashedEquiv();
	if (pdsTemp != NULL)
		ulHash = gpos::CombineHashes(ulHash, pdsTemp->HashValue());
	
	ULONG ulHashedExpressions = std::min(m_pdrgpexpr->Size(), GPOPT_DISTR_SPEC_HASHED_EXPRESSIONS);
	
	for (ULONG ul = 0; ul < ulHashedExpressions; ul++)
	{
		CExpression *pexpr = (*m_pdrgpexpr)[ul];
		ulHash = gpos::CombineHashes(ulHash, CExpression::HashValue(pexpr));
	}
	
	if (NULL != m_hash_idents_equiv_exprs && m_hash_idents_equiv_exprs->Size() > 0)
	{
		for (ULONG ul = 0; ul < m_hash_idents_equiv_exprs->Size(); ul++)
		{
			CExpressionArray *pexprArray = (*m_hash_idents_equiv_exprs)[ul];
			for (ULONG id = 0; id < pexprArray->Size();  id++)
			{
				CExpression *pexpr = (*pexprArray)[id];
				ulHash = gpos::CombineHashes(ulHash, CExpression::HashValue(pexpr));
			}
		}
	}
	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::PcrsUsed
//
//	@doc:
//		Extract columns used by the distribution spec
//
//---------------------------------------------------------------------------
CColRefSet *
CDistributionSpecHashed::PcrsUsed
	(
	IMemoryPool *mp
	)
	const
{
	return CUtils::PcrsExtractColumns(mp, m_pdrgpexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::FMatchHashedDistribution
//
//	@doc:
//		Exact match against given hashed distribution
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecHashed::FMatchHashedDistribution
	(
	const CDistributionSpecHashed *pdshashed
	)
	const
{
	GPOS_ASSERT(NULL != pdshashed);

	if (m_pdrgpexpr->Size() != pdshashed->m_pdrgpexpr->Size() ||
		FNullsColocated() != pdshashed->FNullsColocated() ||
		IsDuplicateSensitive() != pdshashed->IsDuplicateSensitive())
	{
		return false;
	}

	const ULONG length = m_pdrgpexpr->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CExpressionArrays *req_equivexprs = pdshashed->HashSpecEquivExprs();
		CExpressionArray *req_expr_equiv_exprs = NULL;
		if (NULL != req_equivexprs && req_equivexprs->Size() > 0)
			req_expr_equiv_exprs = (*req_equivexprs)[ul];
		CExpression *pexprLeft = (*(pdshashed->m_pdrgpexpr))[ul];
		CExpression *pexprRight = (*m_pdrgpexpr)[ul];
		BOOL fSuccess = false;
		if (req_expr_equiv_exprs != NULL && req_expr_equiv_exprs->Size() > 0)
		{
			for (ULONG id = 0; id < req_expr_equiv_exprs->Size() && !fSuccess; id++)
			{
				CExpression *test = (*req_expr_equiv_exprs)[id];
				fSuccess = CUtils::Equals(test, pexprRight);
			}
		}
		else
		{
			fSuccess = CUtils::Equals(pexprLeft, pexprRight);
		}
		if (!fSuccess)
		{
			return fSuccess;
		}
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL 
CDistributionSpecHashed::Matches
	(
	const CDistributionSpec *pds
	) 
	const
{
	if (Edt() != pds->Edt())
	{
		return false;
	}

	const CDistributionSpecHashed *pdshashed = CDistributionSpecHashed::PdsConvert(pds);

	if (NULL != m_pdshashedEquiv && m_pdshashedEquiv->Matches(pdshashed))
	{
		return true;
	 }

	if (NULL != pdshashed->PdshashedEquiv() && pdshashed->PdshashedEquiv()->Matches(this))
	{
		return true;
	}

	return FMatchHashedDistribution(pdshashed);
}

BOOL
CDistributionSpecHashed::MatchesForHash
(
 const CDistributionSpec *pds
 )
const
{
	//	GPOS_ASSERT(pds->Edt() == CDistributionSpec::EdtHashed);
	if (pds->Edt() != this->Edt())
		return false;
	CDistributionSpecHashed *pdsThis = this->PdshashedEquiv();
	const CDistributionSpecHashed *pdshashed = CDistributionSpecHashed::PdsConvert(pds);
	CDistributionSpecHashed *pdsHashed = pdshashed->PdshashedEquiv();
	
	if ((pdsThis != NULL && pdshashed == NULL) || (pdsThis == NULL && pdsHashed != NULL))
		return false;
	
	BOOL equals = true;
	if (pdsThis != NULL && pdsHashed != NULL)
	{
		equals = pdsThis->MatchesForHash(pdsHashed);
	}
	
	
	if (!equals)
		return false;
	
	BOOL matches = m_fNullsColocated == pdshashed->FNullsColocated() &&
	m_is_duplicate_sensitive == pdshashed->IsDuplicateSensitive() &&
	m_fSatisfiedBySingleton == pdshashed->FSatisfiedBySingleton() &&
	CUtils::Equals(m_pdrgpexpr, pdshashed->m_pdrgpexpr) &&
	Edt() == pdshashed->Edt();
	
	if (!matches)
		return false;
	
	CExpressionArrays *thisexprarrays = HashSpecEquivExprs();
	CExpressionArrays *hashedexprarrays = pdshashed->HashSpecEquivExprs();
	
	if ((thisexprarrays == NULL && hashedexprarrays != NULL) || (thisexprarrays != NULL && hashedexprarrays == NULL))
		return false;
	
	if (thisexprarrays == NULL && hashedexprarrays == NULL)
		return true;
	
	if (thisexprarrays->Size() != hashedexprarrays->Size())
		return false;
	
	BOOL match = true;
	for (ULONG id = 0; id < thisexprarrays->Size() && match; id++)
	{
		match = CUtils::Equals((*thisexprarrays)[id], (*hashedexprarrays)[id]);
	}
	
	
	return match;
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::PdshashedMaximal
//
//	@doc:
//		Return a hashed distribution on the maximal hashable subset of
//		given columns,
//		if all columns are not hashable, return NULL
//
//---------------------------------------------------------------------------
CDistributionSpecHashed *
CDistributionSpecHashed::PdshashedMaximal
	(
	IMemoryPool *mp,
	CColRefArray *colref_array,
	BOOL fNullsColocated
	)
{
	GPOS_ASSERT(NULL != colref_array);
	GPOS_ASSERT(0 < colref_array->Size());

	CColRefArray *pdrgpcrHashable = CUtils::PdrgpcrRedistributableSubset(mp, colref_array);
	CDistributionSpecHashed *pdshashed = NULL;
	if (0 < pdrgpcrHashable->Size())
	{
		CExpressionArray *pdrgpexpr = CUtils::PdrgpexprScalarIdents(mp, pdrgpcrHashable);
		pdshashed = GPOS_NEW(mp) CDistributionSpecHashed(pdrgpexpr, fNullsColocated);
	}
	pdrgpcrHashable->Release();

	return pdshashed;
}

// check if the distribution key expression are covered by the input
// expression array
BOOL
CDistributionSpecHashed::CoveredBy
	(
	const CExpressionArray *dist_cols_expr_array
	)
	const
{
	BOOL covers = false;
	const CDistributionSpecHashed *pds = this;
	while (pds && !covers)
	{
		
		covers = CUtils::Contains(dist_cols_expr_array, pds->Pdrgpexpr());
		pds = pds->PdshashedEquiv();
	}
	return covers;
}

// iterate over all the distribution exprs keys and create an array holding
// equivalent exprs which correspond to the same distribution based on equivalence
// for example: select * from t1, t2 where t1.a = t2.c and t1.b = t2.d;
// if the resulting spec consists of column t1.a, t1,b, based on the equivalence,
// it implies that t1.a distribution is equivalent to t2.c and t1.b is equivalent to t2.d
// t1.a --> equivalent exprs: [t1.a, t2.c]
// t1.b --> equivalent exprs:[t1.b, t2.d]
void
CDistributionSpecHashed::SetEquivHashExprs
	(
	IMemoryPool *mp,
	CExpressionHandle &expression_handle
	)
{
	CExpressionArray *distribution_exprs = m_pdrgpexpr;
	CExpressionArrays *equiv_distribution_all_exprs = m_hash_idents_equiv_exprs;
	if (NULL == equiv_distribution_all_exprs)
	{
		equiv_distribution_all_exprs = GPOS_NEW(mp) CExpressionArrays(mp);
		for (ULONG distribution_key_idx = 0; distribution_key_idx < distribution_exprs->Size(); distribution_key_idx++)
		{
			CExpression *distribution_expr = (*distribution_exprs)[distribution_key_idx];
			CExpression *sc_ident_expr = CCastUtils::PexprWithoutCasts(distribution_expr);
			const CColRef *sc_ident_colref = CScalarIdent::PopConvert(sc_ident_expr->Pop())->Pcr();

			CExpressionArray *equiv_distribution_exprs = GPOS_NEW(mp) CExpressionArray(mp);
			distribution_expr->AddRef();

			// the input expr is always equivalent to itself, so add it to the equivalent expr array
			equiv_distribution_exprs->Append(distribution_expr);

			CColRefSet *equiv_cols = expression_handle.GetRelationalProperties()->Ppc()->PcrsEquivClass(sc_ident_colref);
			// if there are equivalent columns found, then we have a chance to create equivalent distribution exprs
			if (NULL != equiv_cols)
			{
				// create a scalar expr with all the equivalent columns
				CExpression *predicate_expr_with_inferred_quals = CExpressionPreprocessor::PexprConjEqualityPredicates(mp, equiv_cols);
				// put all the scalar expr into an array
				CExpressionArray *predicate_exprs = CPredicateUtils::PdrgpexprConjuncts(mp, predicate_expr_with_inferred_quals);

				// colrefset to track what all exprs has already been considered, it helps avoiding duplicates
				CColRefSet *processed_colrefs = GPOS_NEW(mp) CColRefSet(mp);
				processed_colrefs->Include(sc_ident_colref);
				for (ULONG predicate_idx = 0; predicate_idx < predicate_exprs->Size(); predicate_idx++)
				{
					CExpression *predicate_expr = (*predicate_exprs)[predicate_idx];
					if (!CUtils::FScalarCmp(predicate_expr))
					{
						// if the predicate is anything other than scalar comparision,
						// skip it. for instance: CScalarConst(1)
						continue;
					}

					CColRefSet *scalar_condition_colrefset = CDrvdPropScalar::GetDrvdScalarProps(predicate_expr->PdpDerive())->PcrsUsed();
					if (!scalar_condition_colrefset->FMember(sc_ident_colref))
					{
						// if the current distribution colref is not part of the generated predicate, skip it.
						// we need to consider only the predicate expr which are made up of current
						// distribution expr
						continue;
					}

					// add cast on the expressions if required, both the outer and inner hash exprs
					// should be of the same data type else they may be hashed to different segments
					CExpression *predicate_expr_with_casts = CCastUtils::PexprAddCast(mp, predicate_expr);
					CExpression *original_predicate_expr = predicate_expr;
					if (predicate_expr_with_casts)
					{
						original_predicate_expr = predicate_expr_with_casts;
					}
					CExpression *left_distribution_expr = (*original_predicate_expr)[0];
					CExpression *right_distribution_expr = (*original_predicate_expr)[1];

					// if the predicate is a = b, and a is the current distribution expr,
					// then the equivalent expr consists of b
					CExpression *equiv_distribution_expr = NULL;
					if (CUtils::Equals(left_distribution_expr, distribution_expr))
					{
						equiv_distribution_expr = right_distribution_expr;
					}
					else if (CUtils::Equals(right_distribution_expr, distribution_expr))
					{
						equiv_distribution_expr = left_distribution_expr;
					}

					// if equivalent distributione expr is found, add it to the array holding equivalent distribution exprs
					if (equiv_distribution_expr)
					{
						CExpression *equiv_distribution_expr_without_bcc = CCastUtils::PexprWithoutBinaryCoercibleCasts(equiv_distribution_expr);
						CColRefSet *distribution_expr_without_bcc_colrefset = CDrvdPropScalar::GetDrvdScalarProps(equiv_distribution_expr_without_bcc->PdpDerive())->PcrsUsed();
						// check if the entry has already been processed
						if (!processed_colrefs->FIntersects(distribution_expr_without_bcc_colrefset))
						{
							equiv_distribution_expr_without_bcc->AddRef();
							equiv_distribution_exprs->Append(equiv_distribution_expr_without_bcc);
							processed_colrefs->Include(distribution_expr_without_bcc_colrefset);
						}
					}
					CRefCount::SafeRelease(predicate_expr_with_casts);
				}
				processed_colrefs->Release();
				predicate_exprs->Release();
				predicate_expr_with_inferred_quals->Release();
			}
			equiv_distribution_all_exprs->Append(equiv_distribution_exprs);
		}
		m_hash_idents_equiv_exprs = equiv_distribution_all_exprs;
		GPOS_ASSERT(m_hash_idents_equiv_exprs->Size() == m_pdrgpexpr->Size());
	}
}
//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CDistributionSpecHashed::OsPrint
	(
	IOstream &os
	)
	const
{
	os << this->SzId() << ": [ ";
	const ULONG length = m_pdrgpexpr->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		os << *((*m_pdrgpexpr)[ul]) << " ";
	}
	if (m_fNullsColocated)
	{
		os <<  ", nulls colocated";
	}
	else
	{
		os <<  ", nulls not colocated";
	}

	if (m_is_duplicate_sensitive)
	{
		os <<  ", duplicate sensitive";
	}
	
	if (!m_fSatisfiedBySingleton)
	{
		os << ", across-segments";
	}

	os <<  " ]";

	if (NULL != m_pdshashedEquiv)
	{
		os << ", equiv. dist: ";
		m_pdshashedEquiv->OsPrint(os);
	}
	
	if (NULL != m_hash_idents_equiv_exprs && m_hash_idents_equiv_exprs->Size() > 0)
	{
		os << "," << std::endl;
		for (ULONG ul = 0; ul < m_hash_idents_equiv_exprs->Size(); ul++)
		{
			CExpressionArray *pexprArray = (*m_hash_idents_equiv_exprs)[ul];
			os << "equiv exprs: " << ul << ":" ;
			for (ULONG id = 0; pexprArray->Size() >0 && id < pexprArray->Size(); id++)
			{
				CExpression *pexpr = (*pexprArray)[id];
				os << *pexpr << ",";
			}
		}
	}

	return os;
}

// EOF

