//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		COptCtxt.cpp
//
//	@doc:
//		Implementation of optimizer context
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CAutoP.h"
#include "gpos/sync/CAutoMutex.h"

#include "naucrates/traceflags/traceflags.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CDefaultComparator.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/cost/ICostModel.h"
#include "gpopt/eval/IConstExprEvaluator.h"
#include "gpopt/optimizer/COptimizerConfig.h"

using namespace gpopt;

// value of the first value part id
ULONG COptCtxt::m_ulFirstValidPartId = 1;

//---------------------------------------------------------------------------
//	@function:
//		COptCtxt::COptCtxt
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
COptCtxt::COptCtxt
	(
	IMemoryPool *memory_pool,
	CColumnFactory *pcf,
	CMDAccessor *pmda,
	IConstExprEvaluator *pceeval,
	COptimizerConfig *optimizer_config
	)
	:
	CTaskLocalStorageObject(CTaskLocalStorage::EtlsidxOptCtxt),
	m_memory_pool(memory_pool),
	m_pcf(pcf),
	m_pmda(pmda),
	m_pceeval(pceeval),
	m_pcomp(GPOS_NEW(m_memory_pool) CDefaultComparator(pceeval)),
	m_auPartId(m_ulFirstValidPartId),
	m_pcteinfo(NULL),
	m_pdrgpcrSystemCols(NULL),
	m_poconf(optimizer_config),
	m_fDMLQuery(false)
{
	GPOS_ASSERT(NULL != memory_pool);
	GPOS_ASSERT(NULL != pcf);
	GPOS_ASSERT(NULL != pmda);
	GPOS_ASSERT(NULL != pceeval);
	GPOS_ASSERT(NULL != m_pcomp);
	GPOS_ASSERT(NULL != optimizer_config);
	GPOS_ASSERT(NULL != optimizer_config->Pcm());
	
	m_pcteinfo = GPOS_NEW(m_memory_pool) CCTEInfo(m_memory_pool);
	m_pcm = optimizer_config->Pcm();
}


//---------------------------------------------------------------------------
//	@function:
//		COptCtxt::~COptCtxt
//
//	@doc:
//		dtor
//		Does not de-allocate memory pool!
//
//---------------------------------------------------------------------------
COptCtxt::~COptCtxt()
{
	GPOS_DELETE(m_pcf);
	GPOS_DELETE(m_pcomp);
	m_pceeval->Release();
	m_pcteinfo->Release();
	m_poconf->Release();
	CRefCount::SafeRelease(m_pdrgpcrSystemCols);
}


//---------------------------------------------------------------------------
//	@function:
//		COptCtxt::PoctxtCreate
//
//	@doc:
//		Factory method for optimizer context
//
//---------------------------------------------------------------------------
COptCtxt *
COptCtxt::PoctxtCreate
	(
	IMemoryPool *memory_pool,
	CMDAccessor *pmda,
	IConstExprEvaluator *pceeval,
	COptimizerConfig *optimizer_config
	)
{
	GPOS_ASSERT(NULL != optimizer_config);

	// CONSIDER:  - 1/5/09; allocate column factory out of given mem pool
	// instead of having it create its own;
	CColumnFactory *pcf = GPOS_NEW(memory_pool) CColumnFactory;

	COptCtxt *poctxt = NULL;
	{
		// safe handling of column factory; since it owns a pool that would be
		// leaked if below allocation fails
		CAutoP<CColumnFactory> a_pcf;
		a_pcf = pcf;
		a_pcf.Value()->Initialize();

		poctxt = GPOS_NEW(memory_pool) COptCtxt(memory_pool, pcf, pmda, pceeval, optimizer_config);

		// detach safety
		(void) a_pcf.Reset();
	}
	return poctxt;
}


//---------------------------------------------------------------------------
//	@function:
//		COptCtxt::FAllEnforcersEnabled
//
//	@doc:
//		Return true if all enforcers are enabled
//
//---------------------------------------------------------------------------
BOOL
COptCtxt::FAllEnforcersEnabled()
{
	BOOL fEnforcerDisabled =
		GPOS_FTRACE(EopttraceDisableMotions) ||
		GPOS_FTRACE(EopttraceDisableMotionBroadcast) ||
		GPOS_FTRACE(EopttraceDisableMotionGather) ||
		GPOS_FTRACE(EopttraceDisableMotionHashDistribute) ||
		GPOS_FTRACE(EopttraceDisableMotionRandom) ||
		GPOS_FTRACE(EopttraceDisableMotionRountedDistribute) ||
		GPOS_FTRACE(EopttraceDisableSort) ||
		GPOS_FTRACE(EopttraceDisableSpool) ||
		GPOS_FTRACE(EopttraceDisablePartPropagation);

	return !fEnforcerDisabled;
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		COptCtxt::OsPrint
//
//	@doc:
//		debug print -- necessary to override abstract function in base class
//
//---------------------------------------------------------------------------
IOstream &
COptCtxt::OsPrint
	(
	IOstream &os
	)
	const
{
	// NOOP
	return os;
}

#endif // GPOS_DEBUG

// EOF

