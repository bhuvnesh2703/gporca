//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalIndexGet.cpp
//
//	@doc:
//		Implementation of basic index access
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"
#include "gpos/common/CAutoP.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalIndexGet.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/metadata/CName.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/COptCtxt.h"

#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexGet::CLogicalIndexGet
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalIndexGet::CLogicalIndexGet
	(
	IMemoryPool *memory_pool
	)
	:
	CLogical(memory_pool),
	m_pindexdesc(NULL),
	m_ptabdesc(NULL),
	m_ulOriginOpId(ULONG_MAX),
	m_pnameAlias(NULL),
	m_pdrgpcrOutput(NULL),
	m_pcrsOutput(NULL),
	m_pos(NULL),
	m_pcrsDist(NULL)
{
	m_fPattern = true;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexGet::CLogicalIndexGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalIndexGet::CLogicalIndexGet
	(
	IMemoryPool *memory_pool,
	const IMDIndex *pmdindex,
	CTableDescriptor *ptabdesc,
	ULONG ulOriginOpId,
	const CName *pnameAlias,
	DrgPcr *pdrgpcrOutput
	)
	:
	CLogical(memory_pool),
	m_pindexdesc(NULL),
	m_ptabdesc(ptabdesc),
	m_ulOriginOpId(ulOriginOpId),
	m_pnameAlias(pnameAlias),
	m_pdrgpcrOutput(pdrgpcrOutput),
	m_pcrsOutput(NULL),
	m_pcrsDist(NULL)
{
	GPOS_ASSERT(NULL != pmdindex);
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pnameAlias);
	GPOS_ASSERT(NULL != pdrgpcrOutput);

	// create the index descriptor
	m_pindexdesc  = CIndexDescriptor::Pindexdesc(memory_pool, ptabdesc, pmdindex);

	// compute the order spec
	m_pos = PosFromIndex(m_memory_pool, pmdindex, m_pdrgpcrOutput, ptabdesc);

	// create a set representation of output columns
	m_pcrsOutput = GPOS_NEW(memory_pool) CColRefSet(memory_pool, pdrgpcrOutput);

	m_pcrsDist = CLogical::PcrsDist(memory_pool, m_ptabdesc, m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexGet::~CLogicalIndexGet
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalIndexGet::~CLogicalIndexGet()
{
	CRefCount::SafeRelease(m_ptabdesc);
	CRefCount::SafeRelease(m_pindexdesc);
	CRefCount::SafeRelease(m_pdrgpcrOutput);
	CRefCount::SafeRelease(m_pcrsOutput);
	CRefCount::SafeRelease(m_pos);
	CRefCount::SafeRelease(m_pcrsDist);
	
	GPOS_DELETE(m_pnameAlias);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexGet::HashValue
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalIndexGet::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(COperator::HashValue(),
	                                     m_pindexdesc->MDId()->HashValue());
	ulHash = gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrOutput));
	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexGet::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalIndexGet::FMatch
	(
	COperator *pop
	)
	const
{
	return CUtils::FMatchIndex(this, pop);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexGet::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalIndexGet::PopCopyWithRemappedColumns
	(
	IMemoryPool *memory_pool,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDIndex *pmdindex = pmda->Pmdindex(m_pindexdesc->MDId());

	DrgPcr *pdrgpcrOutput = NULL;
	if (fMustExist)
	{
		pdrgpcrOutput = CUtils::PdrgpcrRemapAndCreate(memory_pool, m_pdrgpcrOutput, phmulcr);
	}
	else
	{
		pdrgpcrOutput = CUtils::PdrgpcrRemap(memory_pool, m_pdrgpcrOutput, phmulcr, fMustExist);
	}
	CName *pnameAlias = GPOS_NEW(memory_pool) CName(memory_pool, *m_pnameAlias);

	m_ptabdesc->AddRef();

	return GPOS_NEW(memory_pool) CLogicalIndexGet(memory_pool, pmdindex, m_ptabdesc, m_ulOriginOpId, pnameAlias, pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexGet::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalIndexGet::PcrsDeriveOutput
	(
	IMemoryPool *memory_pool,
	CExpressionHandle & // exprhdl
	)
{
	CColRefSet *pcrs = GPOS_NEW(memory_pool) CColRefSet(memory_pool);
	pcrs->Include(m_pdrgpcrOutput);

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexGet::PcrsDeriveOuter
//
//	@doc:
//		Derive outer references
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalIndexGet::PcrsDeriveOuter
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl
	)
{
	return PcrsDeriveOuterIndexGet(memory_pool, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexGet::FInputOrderSensitive
//
//	@doc:
//		Is input order sensitive
//
//---------------------------------------------------------------------------
BOOL
CLogicalIndexGet::FInputOrderSensitive() const
{
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexGet::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalIndexGet::PxfsCandidates
	(
	IMemoryPool *memory_pool
	)
const
{
	CXformSet *pxfs = GPOS_NEW(memory_pool) CXformSet(memory_pool);

	(void) pxfs->ExchangeSet(CXform::ExfIndexGet2IndexScan);

	return pxfs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexGet::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalIndexGet::PstatsDerive
	(
	IMemoryPool *memory_pool,
	CExpressionHandle &exprhdl,
	DrgPstat *pdrgpstatCtxt
	)
	const
{
	return CStatisticsUtils::PstatsIndexGet(memory_pool, exprhdl, pdrgpstatCtxt);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalIndexGet::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalIndexGet::OsPrint
	(
	IOstream &os
	)
	const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	os << SzId() << " ";
	// index name
	os << "  Index Name: (";
	m_pindexdesc->Name().OsPrint(os);
	// table alias name
	os <<")";
	os << ", Table Name: (";
	m_pnameAlias->OsPrint(os);
	os <<")";
	os << ", Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
	os << "]";

	return os;
}

// EOF
