//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CTaskContext.cpp
//
//	@doc:
//		Task context implementation
//---------------------------------------------------------------------------

#include "gpos/common/CAutoRef.h"

#include "gpos/error/CLoggerStream.h"
#include "gpos/task/CTaskContext.h"


using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CTaskContext::CTaskContext
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CTaskContext::CTaskContext
	(
	IMemoryPool *pmp
	)
	:
	m_bitset(NULL),
	m_log_out(&CLoggerStream::m_plogStdOut),
	m_log_err(&CLoggerStream::m_plogStdErr),
	m_locale(ElocEnUS_Utf8)
{
	m_bitset = GPOS_NEW(pmp) CBitSet(pmp, EtraceSentinel);
}


//---------------------------------------------------------------------------
//	@function:
//		CTaskContext::CTaskContext
//
//	@doc:
//		used to inherit parent task's context
//
//---------------------------------------------------------------------------
CTaskContext::CTaskContext
	(
	IMemoryPool *pmp,
	const CTaskContext &task_ctxt
	)
	:
	m_bitset(NULL),
	m_log_out(task_ctxt.LogOut()),
	m_log_err(task_ctxt.LogErr()),
	m_locale(task_ctxt.Locale())
{
	// allocate bitset and union separately to guard against leaks under OOM
	CAutoRef<CBitSet> bitset;
	
	bitset = GPOS_NEW(pmp) CBitSet(pmp);
	bitset->Union(task_ctxt.m_bitset);
	
	m_bitset = bitset.Reset();
}


//---------------------------------------------------------------------------
//	@function:
//		CTaskContext::~CTaskContext
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CTaskContext::~CTaskContext()
{
    CRefCount::SafeRelease(m_bitset);
}


//---------------------------------------------------------------------------
//	@function:
//		CTaskContext::Trace
//
//	@doc:
//		Set trace flag; return original setting
//
//---------------------------------------------------------------------------
BOOL
CTaskContext::Trace
	(
	ULONG trace,
	BOOL val
	)
{
	if(val)
	{
		return m_bitset->ExchangeSet(trace);
	}
	else
	{
		return m_bitset->ExchangeClear(trace);
	}
}

// EOF

