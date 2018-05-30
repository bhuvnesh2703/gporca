//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalSequenceProject.cpp
//
//	@doc:
//		Implementation of physical sequence project operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CWindowFrame.h"
#include "gpopt/cost/ICostModel.h"
#include "gpopt/operators/CPhysicalSequenceProject.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CScalarIdent.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::CPhysicalSequenceProject
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalSequenceProject::CPhysicalSequenceProject
	(
	IMemoryPool *memory_pool,
	CDistributionSpec *pds,
	DrgPos *pdrgpos,
	DrgPwf *pdrgpwf
	)
	:
	CPhysical(memory_pool),
	m_pds(pds),
	m_pdrgpos(pdrgpos),
	m_pdrgpwf(pdrgpwf),
	m_pos(NULL),
	m_pcrsRequiredLocal(NULL)
{
	GPOS_ASSERT(NULL != pds);
	GPOS_ASSERT(NULL != pdrgpos);
	GPOS_ASSERT(NULL != pdrgpwf);
	GPOS_ASSERT(CDistributionSpec::EdtHashed == pds->Edt() ||
			CDistributionSpec::EdtSingleton == pds->Edt());
	CreateOrderSpec(memory_pool);
	ComputeRequiredLocalColumns(memory_pool);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::CreateOrderSpec
//
//	@doc:
//		Create local order spec that we request relational child to satisfy
//
//---------------------------------------------------------------------------
void
CPhysicalSequenceProject::CreateOrderSpec
	(
	IMemoryPool *memory_pool
	)
{
	GPOS_ASSERT(NULL == m_pos);
	GPOS_ASSERT(NULL != m_pds);
	GPOS_ASSERT(NULL != m_pdrgpos);

	m_pos = GPOS_NEW(memory_pool) COrderSpec(memory_pool);

	// add partition by keys to order spec
	if (CDistributionSpec::EdtHashed == m_pds->Edt())
	{
		CDistributionSpecHashed *pdshashed = CDistributionSpecHashed::PdsConvert(m_pds);

		const DrgPexpr *pdrgpexpr = pdshashed->Pdrgpexpr();
		const ULONG ulSize = pdrgpexpr->Size();
		for (ULONG ul = 0; ul < ulSize; ul++)
		{
			CExpression *pexpr = (*pdrgpexpr)[ul];
			// we assume partition-by keys are always scalar idents
			CScalarIdent *popScId = CScalarIdent::PopConvert(pexpr->Pop());
			const CColRef *pcr = popScId->Pcr();

			gpmd::IMDId *pmdid = pcr->Pmdtype()->PmdidCmp(IMDType::EcmptL);
			pmdid->AddRef();

			m_pos->Append(pmdid, pcr, COrderSpec::EntLast);
		}
	}

	if (0 == m_pdrgpos->Size())
	{
		return;
	}

	COrderSpec *posFirst = (*m_pdrgpos)[0];
#ifdef GPOS_DEBUG
	const ULONG length = m_pdrgpos->Size();
	for (ULONG ul = 1; ul < length; ul++)
	{
		COrderSpec *posCurrent = (*m_pdrgpos)[ul];
		GPOS_ASSERT(posFirst->FSatisfies(posCurrent) &&
				"first order spec must satisfy all other order specs");
	}
#endif // GPOS_DEBUG

	// we assume here that the first order spec in the children array satisfies all other
	// order specs in the array, this happens as part of the initial normalization
	// so we need to add columns only from the first order spec
	const ULONG ulSize = posFirst->UlSortColumns();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		const CColRef *pcr = posFirst->Pcr(ul);
		gpmd::IMDId *pmdid = posFirst->PmdidSortOp(ul);
		pmdid->AddRef();
		COrderSpec::ENullTreatment ent = posFirst->Ent(ul);
		m_pos->Append(pmdid, pcr, ent);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::ComputeRequiredLocalColumns
//
//	@doc:
//		Compute local required columns
//
//---------------------------------------------------------------------------
void
CPhysicalSequenceProject::ComputeRequiredLocalColumns
	(
	IMemoryPool *memory_pool
	)
{
	GPOS_ASSERT(NULL != m_pos);
	GPOS_ASSERT(NULL != m_pds);
	GPOS_ASSERT(NULL != m_pdrgpos);
	GPOS_ASSERT(NULL != m_pdrgpwf);
	GPOS_ASSERT(NULL == m_pcrsRequiredLocal);

	m_pcrsRequiredLocal = m_pos->PcrsUsed(memory_pool);
	if (CDistributionSpec::EdtHashed == m_pds->Edt())
	{
		CColRefSet *pcrsHashed = CDistributionSpecHashed::PdsConvert(m_pds)->PcrsUsed(memory_pool);
		m_pcrsRequiredLocal->Include(pcrsHashed);
		pcrsHashed->Release();
	}

	// add the columns used in the window frames
	const ULONG ulSize = m_pdrgpwf->Size();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CWindowFrame *pwf = (*m_pdrgpwf)[ul];
		if (NULL != pwf->PexprLeading())
		{
			m_pcrsRequiredLocal->Union(CDrvdPropScalar::Pdpscalar(pwf->PexprLeading()->PdpDerive())->PcrsUsed());
		}
		if (NULL != pwf->PexprTrailing())
		{
			m_pcrsRequiredLocal->Union(CDrvdPropScalar::Pdpscalar(pwf->PexprTrailing()->PdpDerive())->PcrsUsed());
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::~CPhysicalSequenceProject
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalSequenceProject::~CPhysicalSequenceProject()
{
	m_pds->Release();
	m_pdrgpos->Release();
	m_pdrgpwf->Release();
	m_pos->Release();
	m_pcrsRequiredLocal->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::FMatch
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalSequenceProject::FMatch
	(
	COperator *pop
	)
	const
{
	GPOS_ASSERT(NULL != pop);
	if (Eopid() == pop->Eopid())
	{
		CPhysicalSequenceProject *popPhysicalSequenceProject = CPhysicalSequenceProject::PopConvert(pop);
		return
			m_pds->FMatch(popPhysicalSequenceProject->Pds()) &&
			CWindowFrame::Equals(m_pdrgpwf, popPhysicalSequenceProject->Pdrgpwf()) &&
			COrderSpec::Equals(m_pdrgpos, popPhysicalSequenceProject->Pdrgpos());
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::HashValue
//
//	@doc:
//		Hashing function
//
//---------------------------------------------------------------------------
ULONG
CPhysicalSequenceProject::HashValue() const
{
	ULONG ulHash = 0;
	ulHash = gpos::CombineHashes(ulHash, m_pds->HashValue());
	ulHash = gpos::CombineHashes(ulHash, CWindowFrame::HashValue(m_pdrgpwf, 3 /*ulMaxSize*/));
	ulHash = gpos::CombineHashes(ulHash, COrderSpec::HashValue(m_pdrgpos, 3 /*ulMaxSize*/));

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalSequenceProject::PcrsRequired
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
{
	GPOS_ASSERT(0 == ulChildIndex &&
				"Required properties can only be computed on the relational child");

	CColRefSet *pcrs = GPOS_NEW(memory_pool) CColRefSet(memory_pool, *m_pcrsRequiredLocal);
	pcrs->Union(pcrsRequired);

	CColRefSet *pcrsOutput = PcrsChildReqd(memory_pool, exprhdl, pcrs, ulChildIndex, 1 /*ulScalarIndex*/);
	pcrs->Release();

	return pcrsOutput;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalSequenceProject::PosRequired
	(
	IMemoryPool *, // memory_pool
	CExpressionHandle &, // exprhdl
	COrderSpec *, // posRequired
	ULONG
#ifdef GPOS_DEBUG
	ulChildIndex
#endif // GPOS_DEBUG
	,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);

	m_pos->AddRef();

	return m_pos;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalSequenceProject::PdsRequired
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl,
	CDistributionSpec *pdsRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);

	// if expression has to execute on master then we need a gather
	if (exprhdl.FMasterOnly())
	{
		return PdsEnforceMaster(memory_pool, exprhdl, pdsRequired, ulChildIndex);
	}

	// if there are outer references, then we need a broadcast (or a gather)
	if (exprhdl.FHasOuterRefs())
	{
		if (CDistributionSpec::EdtSingleton == pdsRequired->Edt() ||
			CDistributionSpec::EdtReplicated == pdsRequired->Edt())
		{
			return PdsPassThru(memory_pool, exprhdl, pdsRequired, ulChildIndex);
		}

		return GPOS_NEW(memory_pool) CDistributionSpecReplicated();
	}

	// if the window operator has a partition by clause, then always
	// request hashed distribution on the partition column
	if (CDistributionSpec::EdtHashed == m_pds->Edt())
	{
		m_pds->AddRef();
		return m_pds;
	}

	return GPOS_NEW(memory_pool) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalSequenceProject::PrsRequired
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

	// if there are outer references, then we need a materialize
	if (exprhdl.FHasOuterRefs())
	{
		return GPOS_NEW(memory_pool) CRewindabilitySpec(CRewindabilitySpec::ErtGeneral);
	}

	return PrsPassThru(memory_pool, exprhdl, prsRequired, ulChildIndex);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalSequenceProject::PppsRequired
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl,
	CPartitionPropagationSpec *pppsRequired,
	ULONG 
#ifdef GPOS_DEBUG 
	ulChildIndex
#endif
	,
	DrgPdp *, //pdrgpdpCtxt,
	ULONG //ulOptReq
	)
{
	GPOS_ASSERT(0 == ulChildIndex);
	GPOS_ASSERT(NULL != pppsRequired);

	return CPhysical::PppsRequiredPushThruUnresolvedUnary(memory_pool, exprhdl, pppsRequired, CPhysical::EppcAllowed);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalSequenceProject::PcteRequired
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
//		CPhysicalSequenceProject::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalSequenceProject::FProvidesReqdCols
	(
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(NULL != pcrsRequired);
	GPOS_ASSERT(2 == exprhdl.UlArity());

	CColRefSet *pcrs = GPOS_NEW(m_memory_pool) CColRefSet(m_memory_pool);
	// include defined columns by scalar project list
	pcrs->Union(exprhdl.Pdpscalar(1 /*ulChildIndex*/)->PcrsDefined());

	// include output columns of the relational child
	pcrs->Union(exprhdl.Pdprel(0 /*ulChildIndex*/)->PcrsOutput());

	BOOL fProvidesCols = pcrs->ContainsAll(pcrsRequired);
	pcrs->Release();

	return fProvidesCols;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalSequenceProject::PosDerive
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
//		CPhysicalSequenceProject::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalSequenceProject::PdsDerive
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
//		CPhysicalSequenceProject::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalSequenceProject::PrsDerive
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
//		CPhysicalSequenceProject::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalSequenceProject::EpetOrder
	(
	CExpressionHandle &exprhdl,
	const CEnfdOrder *peo
	)
	const
{
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

	COrderSpec *pos = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pos();
	if (peo->FCompatible(pos))
	{
		return  CEnfdProp::EpetUnnecessary;
	}

	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalSequenceProject::EpetRewindability
	(
	CExpressionHandle &exprhdl,
	const CEnfdRewindability *per
	)
	const
{
	CRewindabilitySpec *prs = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs();
	if (per->FCompatible(prs))
	{
		 // required distribution is already provided
		 return CEnfdProp::EpetUnnecessary;
	}

	// rewindability is enforced on operator's output
	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequenceProject::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalSequenceProject::OsPrint
	(
	IOstream &os
	)
	const
{
	os	<< SzId() << " (";
	(void) m_pds->OsPrint(os);
	os	<< ", ";
	(void) COrderSpec::OsPrint(os, m_pdrgpos);
	os	<< ", ";
	(void) CWindowFrame::OsPrint(os, m_pdrgpwf);

	return os << ")";
}


// EOF

