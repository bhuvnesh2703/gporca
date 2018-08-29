//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CReqdProp.cpp
//
//	@doc:
//		Implementation of required properties
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CReqdProp.h"
#include "gpopt/operators/COperator.h"

#ifdef GPOS_DEBUG
#include "gpos/error/CAutoTrace.h"
#include "gpopt/base/COptCtxt.h"
#endif // GPOS_DEBUG
#include "gpopt/base/CUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CReqdProp::CReqdProp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CReqdProp::CReqdProp()
{}


//---------------------------------------------------------------------------
//	@function:
//		CReqdProp::~CReqdProp
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CReqdProp::~CReqdProp()
{}


#ifdef GPOS_DEBUG
CHAR *
CReqdProp::DbgPrint() const
{
	IMemoryPool *mp = COptCtxt::PoctxtFromTLS()->Pmp();
	CAutoTrace at(mp);
	at.Os() << *this;
	const WCHAR *buff = at.GetString()->GetBuffer();
	char *sz = CUtils::CreateMultiByteCharStringFromWCString(mp, const_cast< wchar_t* >(buff));
	return sz;
}
#endif // GPOS_DEBUG

// EOF
