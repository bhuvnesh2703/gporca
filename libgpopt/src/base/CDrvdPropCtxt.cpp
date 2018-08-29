//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Software, Inc.
//
//	@filename:
//		CDrvdPropCtxt.cpp
//
//	@doc:
//		Implementation of derived properties context
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CDrvdPropCtxt.h"

#ifdef GPOS_DEBUG
#include "gpos/error/CAutoTrace.h"
#endif // GPOS_DEBUG

namespace gpopt {

	IOstream &operator << (IOstream &os, CDrvdPropCtxt &drvdpropctxt)
	{
		return drvdpropctxt.OsPrint(os);
	}

#ifdef GPOS_DEBUG
	CHAR *
	CDrvdPropCtxt::DbgPrint() const
	{
		CAutoTrace at(m_mp);
		(void) this->OsPrint(at.Os());
		const WCHAR *buff = at.GetString()->GetBuffer();
		ULONG ulMaxLength = GPOS_WSZ_LENGTH(const_cast< wchar_t* >(buff)) * GPOS_SIZEOF(WCHAR) + 1;
		CHAR *sz = GPOS_NEW_ARRAY(m_mp, CHAR, ulMaxLength);
		clib::Wcstombs(sz, const_cast< wchar_t* >(buff), ulMaxLength);
		sz[ulMaxLength - 1] = '\0';
		return sz;
	}
#endif // GPOS_DEBUG

}

// EOF
