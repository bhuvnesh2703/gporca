//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CPhysicalPartitionSelector.cpp
//
//	@doc:
//		Implementation of physical partition selector
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDrvdPropCtxtPlan.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalPartitionSelector.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/base/CColRef.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::CPhysicalPartitionSelector
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalPartitionSelector::CPhysicalPartitionSelector
	(
	IMemoryPool *memory_pool,
	ULONG ulScanId,
	IMDId *pmdid,
	DrgDrgPcr *pdrgpdrgpcr,
	PartCnstrMap *ppartcnstrmap,
	CPartConstraint *ppartcnstr,
	HMUlExpr *phmulexprEqPredicates,
	HMUlExpr *phmulexprPredicates,
	CExpression *pexprResidual
	)
	:
	CPhysical(memory_pool),
	m_ulScanId(ulScanId),
	m_pmdid(pmdid),
	m_pdrgpdrgpcr(pdrgpdrgpcr),
	m_ppartcnstrmap(ppartcnstrmap),
	m_ppartcnstr(ppartcnstr),
	m_phmulexprEqPredicates(phmulexprEqPredicates),
	m_phmulexprPredicates(phmulexprPredicates),
	m_pexprResidual(pexprResidual)
{
	GPOS_ASSERT(0 < ulScanId);
	GPOS_ASSERT(pmdid->IsValid());
	GPOS_ASSERT(NULL != pdrgpdrgpcr);
	GPOS_ASSERT(0 < pdrgpdrgpcr->Size());
	GPOS_ASSERT(NULL != ppartcnstrmap);
	GPOS_ASSERT(NULL != ppartcnstr);
	GPOS_ASSERT(NULL != phmulexprEqPredicates);
	GPOS_ASSERT(NULL != phmulexprPredicates);

	m_pexprCombinedPredicate = PexprCombinedPartPred(memory_pool);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::CPhysicalPartitionSelector
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalPartitionSelector::CPhysicalPartitionSelector
	(
	IMemoryPool *memory_pool,
	IMDId *pmdid,
	HMUlExpr *phmulexprEqPredicates
	)
	:
	CPhysical(memory_pool),
	m_ulScanId(0),
	m_pmdid(pmdid),
	m_pdrgpdrgpcr(NULL),
	m_ppartcnstrmap(NULL),
	m_ppartcnstr(NULL),
	m_phmulexprEqPredicates(phmulexprEqPredicates),
	m_phmulexprPredicates(NULL),
	m_pexprResidual(NULL),
	m_pexprCombinedPredicate(NULL)
{
	GPOS_ASSERT(pmdid->IsValid());
	GPOS_ASSERT(NULL != phmulexprEqPredicates);

	m_phmulexprPredicates = GPOS_NEW(memory_pool) HMUlExpr(memory_pool);
	m_pexprCombinedPredicate = PexprCombinedPartPred(memory_pool);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::~CPhysicalPartitionSelector
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalPartitionSelector::~CPhysicalPartitionSelector()
{
	CRefCount::SafeRelease(m_pdrgpdrgpcr);
	CRefCount::SafeRelease(m_ppartcnstr);
	CRefCount::SafeRelease(m_ppartcnstrmap);
	m_phmulexprPredicates->Release();
	m_pmdid->Release();
	m_phmulexprEqPredicates->Release();
	CRefCount::SafeRelease(m_pexprResidual);
	CRefCount::SafeRelease(m_pexprCombinedPredicate);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::FMatchExprMaps
//
//	@doc:
//		Check whether two expression maps match
//
//---------------------------------------------------------------------------
BOOL
CPhysicalPartitionSelector::FMatchExprMaps
	(
	HMUlExpr *phmulexprFst,
	HMUlExpr *phmulexprSnd
	)
{
	GPOS_ASSERT(NULL != phmulexprFst);
	GPOS_ASSERT(NULL != phmulexprSnd);

	const ULONG ulEntries = phmulexprFst->Size();
	if (ulEntries != phmulexprSnd->Size())
	{
		return false;
	}

	HMUlExprIter hmulei(phmulexprFst);

	while (hmulei.Advance())
	{
		ULONG ulKey = *(hmulei.Key());
		const CExpression *pexprFst = hmulei.Value();
		CExpression *pexprSnd = phmulexprSnd->Find(&ulKey);
		if (!CUtils::Equals(pexprFst, pexprSnd))
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::FMatchPartCnstr
//
//	@doc:
//		Match part constraints
//
//---------------------------------------------------------------------------
BOOL
CPhysicalPartitionSelector::FMatchPartCnstr
	(
	PartCnstrMap *ppartcnstrmap
	)
	const
{
	if (NULL == m_ppartcnstrmap || NULL == ppartcnstrmap)
	{
		return NULL == m_ppartcnstrmap && NULL == ppartcnstrmap;
	}

	return m_ppartcnstrmap->Size() == ppartcnstrmap->Size() &&
			FSubsetPartCnstr(ppartcnstrmap, m_ppartcnstrmap);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::FSubsetPartCnstr
//
//	@doc:
//		Check if first part constraint map is a subset of the second one
//
//---------------------------------------------------------------------------
BOOL
CPhysicalPartitionSelector::FSubsetPartCnstr
	(
	PartCnstrMap *ppartcnstrmapFst,
	PartCnstrMap *ppartcnstrmapSnd
	)
{
	GPOS_ASSERT(NULL != ppartcnstrmapFst);
	GPOS_ASSERT(NULL != ppartcnstrmapSnd);
	if (ppartcnstrmapFst->Size() > ppartcnstrmapSnd->Size())
	{
		return false;
	}

	PartCnstrMapIter partcnstriter(ppartcnstrmapFst);

	while (partcnstriter.Advance())
	{
		ULONG ulKey = *(partcnstriter.Key());
		CPartConstraint *ppartcnstr = ppartcnstrmapSnd->Find(&ulKey);

		if (NULL == ppartcnstr || !partcnstriter.Value()->FEquivalent(ppartcnstr))
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::FHasFilter
//
//	@doc:
//		Check whether this operator has a partition selection filter
//
//---------------------------------------------------------------------------
BOOL
CPhysicalPartitionSelector::FHasFilter() const
{
	return (NULL != m_pexprResidual ||
			0 < m_phmulexprEqPredicates->Size() ||
			0 < m_phmulexprPredicates->Size());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::FMatch
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalPartitionSelector::FMatch
	(
	COperator *pop
	)
	const
{
	if (Eopid() != pop->Eopid())
	{
		return false;
	}

	CPhysicalPartitionSelector *popPartSelector = CPhysicalPartitionSelector::PopConvert(pop);

	BOOL fScanIdCmp = popPartSelector->UlScanId() == m_ulScanId;
	BOOL fMdidCmp = popPartSelector->MDId()->Equals(MDId());
	BOOL fPartCnstrMapCmp = FMatchPartCnstr(popPartSelector->m_ppartcnstrmap);
	BOOL fColRefCmp = CColRef::Equals(popPartSelector->Pdrgpdrgpcr(), m_pdrgpdrgpcr) ;
	BOOL fPartCnstrEquiv = popPartSelector->m_ppartcnstr->FEquivalent(m_ppartcnstr) ;
	BOOL fEqPredCmp = FMatchExprMaps(popPartSelector->m_phmulexprEqPredicates, m_phmulexprEqPredicates) ;
	BOOL fPredCmp = FMatchExprMaps(popPartSelector->m_phmulexprPredicates, m_phmulexprPredicates) ;
	BOOL fResPredCmp = CUtils::Equals(popPartSelector->m_pexprResidual, m_pexprResidual);

	return fScanIdCmp && fMdidCmp && fPartCnstrMapCmp && fColRefCmp && fPartCnstrEquiv && fEqPredCmp && fResPredCmp && fPredCmp;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::HashValue
//
//	@doc:
//		Hash operator
//
//---------------------------------------------------------------------------
ULONG
CPhysicalPartitionSelector::HashValue() const
{
	return gpos::CombineHashes
				(
				Eopid(),
				gpos::CombineHashes(m_ulScanId, MDId()->HashValue())
				);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PexprEqFilter
//
//	@doc:
//		Return the equality filter expression for the given level
//
//---------------------------------------------------------------------------
CExpression *
CPhysicalPartitionSelector::PexprEqFilter
	(
	ULONG ulPartLevel
	)
	const
{
	return m_phmulexprEqPredicates->Find(&ulPartLevel);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PexprFilter
//
//	@doc:
//		Return the non-equality filter expression for the given level
//
//---------------------------------------------------------------------------
CExpression *
CPhysicalPartitionSelector::PexprFilter
	(
	ULONG ulPartLevel
	)
	const
{
	return m_phmulexprPredicates->Find(&ulPartLevel);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PexprPartPred
//
//	@doc:
//		Return the partition selection predicate for the given level
//
//---------------------------------------------------------------------------
CExpression *
CPhysicalPartitionSelector::PexprPartPred
	(
	IMemoryPool *memory_pool,
	ULONG ulPartLevel
	)
	const
{
	GPOS_ASSERT(ulPartLevel < UlPartLevels());

	CExpression *pexpr = PexprEqFilter(ulPartLevel);
	if (NULL != pexpr)
	{
		// we have one side of an equality predicate - need to construct the
		// whole predicate
		GPOS_ASSERT(NULL == m_phmulexprPredicates->Find(&ulPartLevel));
		pexpr->AddRef();
		if (NULL != m_pdrgpdrgpcr)
		{
			CColRef *pcrPartKey = (*(*m_pdrgpdrgpcr)[ulPartLevel])[0];
			return CUtils::PexprScalarEqCmp(memory_pool, pcrPartKey, pexpr);
		}
		else
		{
			return pexpr;
		}
	}

	pexpr = m_phmulexprPredicates->Find(&ulPartLevel);
	if (NULL != pexpr)
	{
		pexpr->AddRef();
	}

	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PexprCombinedPartPred
//
//	@doc:
//		Return a single combined partition selection predicate
//
//---------------------------------------------------------------------------
CExpression *
CPhysicalPartitionSelector::PexprCombinedPartPred
	(
	IMemoryPool *memory_pool
	)
	const
{
	DrgPexpr *pdrgpexpr = GPOS_NEW(memory_pool) DrgPexpr(memory_pool);

	const ULONG ulLevels = UlPartLevels();
	for (ULONG ul = 0; ul < ulLevels; ul++)
	{
		CExpression *pexpr = PexprPartPred(memory_pool, ul);
		if (NULL != pexpr)
		{
			pdrgpexpr->Append(pexpr);
		}
	}

	if (NULL != m_pexprResidual)
	{
		m_pexprResidual->AddRef();
		pdrgpexpr->Append(m_pexprResidual);
	}

	return CPredicateUtils::PexprConjunction(memory_pool, pdrgpexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::UlPartLevels
//
//	@doc:
//		Number of partitioning levels
//
//---------------------------------------------------------------------------
ULONG
CPhysicalPartitionSelector::UlPartLevels() const
{
	if (NULL != m_pdrgpdrgpcr)
	{
		return m_pdrgpdrgpcr->Size();
	}

	return m_phmulexprEqPredicates->Size();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PpfmDerive
//
//	@doc:
//		Derive partition filter map
//
//---------------------------------------------------------------------------
CPartFilterMap *
CPhysicalPartitionSelector::PpfmDerive
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl
	)
	const
{
	if (!FHasFilter())
	{
		return PpfmPassThruOuter(exprhdl);
	}

	CPartFilterMap *ppfm = PpfmDeriveCombineRelational(memory_pool, exprhdl);
	IStatistics *pstats = exprhdl.Pstats();
	GPOS_ASSERT(NULL != pstats);
	m_pexprCombinedPredicate->AddRef();
	pstats->AddRef();
	ppfm->AddPartFilter(memory_pool, m_ulScanId, m_pexprCombinedPredicate, pstats);
	return ppfm;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalPartitionSelector::PcrsRequired
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsInput,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
{
	GPOS_ASSERT(0 == ulChildIndex &&
				"Required properties can only be computed on the relational child");

	CColRefSet *pcrs = GPOS_NEW(memory_pool) CColRefSet(memory_pool, *pcrsInput);
	pcrs->Union(CDrvdPropScalar::Pdpscalar(m_pexprCombinedPredicate->PdpDerive())->PcrsUsed());
	pcrs->Intersection(exprhdl.Pdprel(ulChildIndex)->PcrsOutput());

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalPartitionSelector::PosRequired
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl,
	COrderSpec *posRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);

	return PosPassThru(memory_pool, exprhdl, posRequired, ulChildIndex);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalPartitionSelector::PdsRequired
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl,
	CDistributionSpec *pdsInput,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);

	CDrvdPropRelational *pdprelDrvd = exprhdl.Pdprel();
	CPartInfo *ppartinfo = pdprelDrvd->Ppartinfo();
	BOOL fCovered = ppartinfo->FContainsScanId(m_ulScanId);

	if (fCovered)
	{
		// if partition consumer is defined below, do not pass distribution
		// requirements down as this will cause the consumer and enforcer to be
		// in separate slices
		return GPOS_NEW(memory_pool) CDistributionSpecAny(this->Eopid());
	}

	return PdsPassThru(memory_pool, exprhdl, pdsInput, ulChildIndex);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalPartitionSelector::PrsRequired
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl,
	CRewindabilitySpec *prsRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);

	return PrsPassThru(memory_pool, exprhdl, prsRequired, ulChildIndex);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalPartitionSelector::PppsRequired
	(
	IMemoryPool *memory_pool,
	CExpressionHandle & exprhdl,
	CPartitionPropagationSpec *pppsRequired,
	ULONG
#ifdef GPOS_DEBUG
	ulChildIndex
#endif // GPOS_DEBUG
	,
	DrgPdp *, //pdrgpdpCtxt,
	ULONG //ulOptReq
	)
{
	GPOS_ASSERT(0 == ulChildIndex);
	GPOS_ASSERT(NULL != pppsRequired);

	CPartIndexMap *ppimInput = pppsRequired->Ppim();
	CPartFilterMap *ppfmInput = pppsRequired->Ppfm();

	ULongPtrArray *pdrgpulInputScanIds = ppimInput->PdrgpulScanIds(memory_pool);

	CPartIndexMap *ppim = GPOS_NEW(memory_pool) CPartIndexMap(memory_pool);
	CPartFilterMap *ppfm = GPOS_NEW(memory_pool) CPartFilterMap(memory_pool);

	CPartInfo *ppartinfo = exprhdl.Pdprel(0)->Ppartinfo();

	const ULONG ulScanIds = pdrgpulInputScanIds->Size();

	for (ULONG ul = 0; ul < ulScanIds; ul++)
	{
		ULONG ulScanId = *((*pdrgpulInputScanIds)[ul]);
		ULONG ulExpectedPropagators = ppimInput->UlExpectedPropagators(ulScanId);

		if (ulScanId == m_ulScanId)
		{
			// partition propagation resolved - do not need to require from children
			continue;
		}

		if (!ppartinfo->FContainsScanId(ulScanId) && ppartinfo->FContainsScanId(m_ulScanId))
		{
		    // dynamic scan for the required id not defined below, but the current one is: do not push request down
			continue;
		}

		IMDId *pmdid = ppimInput->PmdidRel(ulScanId);
		DrgPpartkeys *pdrgppartkeys = ppimInput->Pdrgppartkeys(ulScanId);
		PartCnstrMap *ppartcnstrmap = ppimInput->Ppartcnstrmap(ulScanId);
		CPartConstraint *ppartcnstr = ppimInput->PpartcnstrRel(ulScanId);
		CPartIndexMap::EPartIndexManipulator epim = ppimInput->Epim(ulScanId);
		pmdid->AddRef();
		pdrgppartkeys->AddRef();
		ppartcnstrmap->AddRef();
		ppartcnstr->AddRef();

		ppim->Insert(ulScanId, ppartcnstrmap, epim, ulExpectedPropagators, pmdid, pdrgppartkeys, ppartcnstr);
		(void) ppfm->FCopyPartFilter(m_memory_pool, ulScanId, ppfmInput);
	}

	// cleanup
	pdrgpulInputScanIds->Release();

	return GPOS_NEW(memory_pool) CPartitionPropagationSpec(ppim, ppfm);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalPartitionSelector::PcteRequired
	(
	IMemoryPool *, //memory_pool,
	CExpressionHandle &, //exprhdl,
	CCTEReq *pcter,
	ULONG
#ifdef GPOS_DEBUG
	ulChildIndex
#endif
	,
	DrgPdp *, //pdrgpdpCtxt,
	ULONG //ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);
	return PcterPushThru(pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalPartitionSelector::FProvidesReqdCols
	(
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG // ulOptReq
	)
	const
{
	return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalPartitionSelector::PosDerive
	(
	IMemoryPool *, // memory_pool
	CExpressionHandle &exprhdl
	)
	const
{
	return PosDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalPartitionSelector::PdsDerive
	(
	IMemoryPool *, // memory_pool
	CExpressionHandle &exprhdl
	)
	const
{
	return PdsDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PpimDerive
//
//	@doc:
//		Derive partition index map
//
//---------------------------------------------------------------------------
CPartIndexMap *
CPhysicalPartitionSelector::PpimDerive
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl,
	CDrvdPropCtxt *pdpctxt
	)
	const
{
	GPOS_ASSERT(NULL != pdpctxt);

	CDrvdPropPlan *pdpplan = exprhdl.Pdpplan(0 /*ulChildIndex*/);
	CPartIndexMap *ppimInput = pdpplan->Ppim();
	GPOS_ASSERT(NULL != ppimInput);

	ULONG ulExpectedPartitionSelectors = CDrvdPropCtxtPlan::PdpctxtplanConvert(pdpctxt)->UlExpectedPartitionSelectors();

	CPartIndexMap *ppim = ppimInput->PpimPartitionSelector(memory_pool, m_ulScanId, ulExpectedPartitionSelectors);
	if (!ppim->FContains(m_ulScanId))
	{
		// the consumer of this scan id does not come from the child, i.e. it
		// is on the other side of a join
		MDId()->AddRef();
		m_pdrgpdrgpcr->AddRef();
		m_ppartcnstrmap->AddRef();
		m_ppartcnstr->AddRef();

		DrgPpartkeys *pdrgppartkeys = GPOS_NEW(memory_pool) DrgPpartkeys(memory_pool);
		pdrgppartkeys->Append(GPOS_NEW(memory_pool) CPartKeys(m_pdrgpdrgpcr));

		ppim->Insert(m_ulScanId, m_ppartcnstrmap, CPartIndexMap::EpimPropagator, 0 /*ulExpectedPropagators*/, MDId(), pdrgppartkeys, m_ppartcnstr);
	}

	return ppim;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalPartitionSelector::PrsDerive
	(
	IMemoryPool *, // memory_pool
	CExpressionHandle &exprhdl
	)
	const
{
	return PrsDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::EpetDistribution
//
//	@doc:
//		Return the enforcing type for distribution property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalPartitionSelector::EpetDistribution
	(
	CExpressionHandle &exprhdl,
	const CEnfdDistribution *ped
	)
	const
{
	CDrvdPropPlan *pdpplan = exprhdl.Pdpplan(0 /* ulChildIndex */);

	if (ped->FCompatible(pdpplan->Pds()))
	{
		// required distribution established by the operator
		return CEnfdProp::EpetUnnecessary;
	}

	CPartIndexMap *ppimDrvd = pdpplan->Ppim();
	if (!ppimDrvd->FContains(m_ulScanId))
	{
		// part consumer is defined above: prohibit adding a motion on top of the
		// part resolver as this will create two slices
		return CEnfdProp::EpetProhibited;
	}

	GPOS_ASSERT(CPartIndexMap::EpimConsumer == ppimDrvd->Epim(m_ulScanId));

	// part consumer found below: enforce distribution on top of part resolver
	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalPartitionSelector::EpetRewindability
	(
	CExpressionHandle &, // exprhdl
	const CEnfdRewindability * // per
	)
	const
{
	// rewindability is preserved on operator's output
	return CEnfdProp::EpetOptional;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalPartitionSelector::EpetOrder
	(
	CExpressionHandle &, // exprhdl,
	const CEnfdOrder * // ped
	)
	const
{
	return CEnfdProp::EpetOptional;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelector::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalPartitionSelector::OsPrint
	(
	IOstream &os
	)
	const
{

	os	<< SzId()
		<< ", Scan Id: " << m_ulScanId
		<< ", Part Table: ";
	MDId()->OsPrint(os);

	return os;
}

// EOF
