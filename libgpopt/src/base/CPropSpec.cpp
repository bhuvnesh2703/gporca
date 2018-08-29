//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPropSpec.cpp
//
//	@doc:
//		Abstraction for specification of properties
//---------------------------------------------------------------------------

#include "gpopt/base/CPropSpec.h"

#ifdef GPOS_DEBUG
#include "gpopt/base/COptCtxt.h"
#include "gpos/error/CAutoTrace.h"
#endif
#include "gpopt/base/CUtils.h"

using namespace gpopt;

#ifdef GPOS_DEBUG
// print distribution spec
CHAR *
CPropSpec::DbgPrint() const
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
