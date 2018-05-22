//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		init.cpp
//
//	@doc:
//		Implementation of initialization and termination functions for
//		libgpopt.
//---------------------------------------------------------------------------

#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/task/CWorker.h"

#include "gpopt/init.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/exception.h"
#include "gpopt/xforms/CXformFactory.h"
#include "gpos/_api.h"
#include "naucrates/init.h"

using namespace gpos;
using namespace gpopt;

static IMemoryPool *pmp = NULL;


//---------------------------------------------------------------------------
//      @function:
//              gpopt_init
//
//      @doc:
//              Initialize gpopt library. To enable memory allocations
//              via a custom allocator, pass in non-NULL fnAlloc/fnFree
//              allocation/deallocation functions. If either of the parameters
//              are NULL, gpopt with be initialized with the default allocator.
//
//---------------------------------------------------------------------------
void gpopt_init()
{
	{
		CAutoMemoryPool amp;
		pmp = amp.Pmp();

		// add standard exception messages
		(void) gpopt::EresExceptionInit(pmp);
	
		// detach safety
		(void) amp.Detach();
	}

	if (GPOS_OK != gpopt::CXformFactory::Init())
	{
		return;
	}
}

//---------------------------------------------------------------------------
//      @function:
//              gpopt_terminate
//
//      @doc:
//              Destroy the memory pool
//
//---------------------------------------------------------------------------
void gpopt_terminate()
{
#ifdef GPOS_DEBUG
	CMDCache::Shutdown();

	CMemoryPoolManager::MemoryPoolMgr()->Destroy(pmp);

	CXformFactory::Pxff()->Shutdown();
#endif // GPOS_DEBUG
}

// EOF
