//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CAutoTrace.cpp
//
//	@doc:
//		Implementation of auto object for creating trace messages
//---------------------------------------------------------------------------

#include "gpos/error/CAutoTrace.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CAutoTrace::CAutoTrace
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CAutoTrace::CAutoTrace
	(
	IMemoryPool*memory_pool
	)
	:
	m_wstr(memory_pool),
	m_os(&m_wstr)
{}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTrace::~CAutoTrace
//
//	@doc:
//		Destructor prints trace message
//
//---------------------------------------------------------------------------
CAutoTrace::~CAutoTrace()
{
	if (0 < m_wstr.Length() && !ITask::Self()->GetErrCtxt()->IsPending())
	{
		GPOS_TRACE(m_wstr.GetBuffer());
	}
}

// EOF

