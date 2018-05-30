//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalMotionHashDistribute.cpp
//
//	@doc:
//		Implementation of hash distribute motion operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalMotionHashDistribute.h"
#include "gpopt/base/CDistributionSpecHashedNoOp.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistribute::CPhysicalMotionHashDistribute
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalMotionHashDistribute::CPhysicalMotionHashDistribute
	(
	IMemoryPool *memory_pool,
	CDistributionSpecHashed *pdsHashed
	)
	:
	CPhysicalMotion(memory_pool),
	m_pdsHashed(pdsHashed),
	m_pcrsRequiredLocal(NULL)
{
	GPOS_ASSERT(NULL != pdsHashed);
	GPOS_ASSERT(0 != pdsHashed->Pdrgpexpr()->Size());

	m_pcrsRequiredLocal = m_pdsHashed->PcrsUsed(memory_pool);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistribute::~CPhysicalMotionHashDistribute
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalMotionHashDistribute::~CPhysicalMotionHashDistribute()
{
	m_pdsHashed->Release();
	m_pcrsRequiredLocal->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistribute::FMatch
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotionHashDistribute::FMatch
	(
	COperator *pop
	)
	const
{
	if (Eopid() != pop->Eopid())
	{
		return false;
	}

	CPhysicalMotionHashDistribute *popHashDistribute =
			CPhysicalMotionHashDistribute::PopConvert(pop);

	return m_pdsHashed->Equals(popHashDistribute->m_pdsHashed);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistribute::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalMotionHashDistribute::PcrsRequired
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
{
	GPOS_ASSERT(0 == ulChildIndex);

	CColRefSet *pcrs = GPOS_NEW(memory_pool) CColRefSet(memory_pool, *m_pcrsRequiredLocal);
	pcrs->Union(pcrsRequired);
	CColRefSet *pcrsChildReqd = PcrsChildReqd(memory_pool, exprhdl, pcrs, ulChildIndex, ULONG_MAX);
	pcrs->Release();

	return pcrsChildReqd;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistribute::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotionHashDistribute::FProvidesReqdCols
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
//		CPhysicalMotionHashDistribute::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalMotionHashDistribute::EpetOrder
	(
	CExpressionHandle &, // exprhdl
	const CEnfdOrder * // peo
	)
	const
{
	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistribute::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalMotionHashDistribute::PosRequired
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &, // exprhdl
	COrderSpec *,//posInput
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

	return GPOS_NEW(memory_pool) COrderSpec(memory_pool);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistribute::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalMotionHashDistribute::PosDerive
	(
	IMemoryPool *memory_pool,
	CExpressionHandle & // exprhdl
	)
	const
{
	return GPOS_NEW(memory_pool) COrderSpec(memory_pool);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistribute::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalMotionHashDistribute::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " ";

	return m_pdsHashed->OsPrint(os);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionHashDistribute::PopConvert
//
//	@doc:
//		Conversion function
//
//---------------------------------------------------------------------------
CPhysicalMotionHashDistribute *
CPhysicalMotionHashDistribute::PopConvert
	(
	COperator *pop
	)
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(EopPhysicalMotionHashDistribute == pop->Eopid());

	return dynamic_cast<CPhysicalMotionHashDistribute*>(pop);
}			

CDistributionSpec *
CPhysicalMotionHashDistribute::PdsRequired
	(
		IMemoryPool *memory_pool,
		CExpressionHandle &exprhdl,
		CDistributionSpec *pdsRequired,
		ULONG ulChildIndex,
		DrgPdp *pdrgpdpCtxt,
		ULONG ulOptReq
	) const
{
	CDistributionSpecHashedNoOp* pdsNoOp = dynamic_cast<CDistributionSpecHashedNoOp*>(m_pdsHashed);
	if (NULL == pdsNoOp)
	{
		return CPhysicalMotion::PdsRequired(memory_pool, exprhdl, pdsRequired, ulChildIndex, pdrgpdpCtxt, ulOptReq);
	}
	else
	{
		DrgPexpr *pdrgpexpr = pdsNoOp->Pdrgpexpr();
		pdrgpexpr->AddRef();
		CDistributionSpecHashed* pdsHashed = GPOS_NEW(memory_pool) CDistributionSpecHashed(pdrgpexpr, pdsNoOp->FNullsColocated());
		return pdsHashed;
	}
}

// EOF

