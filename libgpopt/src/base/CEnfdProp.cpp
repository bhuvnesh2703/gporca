//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Software, Inc.
//
//	@filename:
//		CEnfdProp.cpp
//
//	@doc:
//		Implementation of enforced property
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CEnfdProp.h"

#ifdef GPOS_DEBUG
#include "gpopt/base/COptCtxt.h"
#include "gpos/error/CAutoTrace.h"
#endif // GPOS_DEBUG

namespace gpopt {

	IOstream &operator << (IOstream &os, CEnfdProp &efdprop)
	{
		return efdprop.OsPrint(os);
	}

#ifdef GPOS_DEBUG
	CHAR *
	CEnfdProp::DbgPrint() const
	{
		IMemoryPool *mp = COptCtxt::PoctxtFromTLS()->Pmp();
		CAutoTrace at(mp);
		(void) this->OsPrint(at.Os());
		const WCHAR *buff = at.GetString()->GetBuffer();
		ULONG ulMaxLength = GPOS_WSZ_LENGTH(const_cast< wchar_t* >(buff)) * GPOS_SIZEOF(WCHAR) + 1;
		CHAR *sz = GPOS_NEW_ARRAY(mp, CHAR, ulMaxLength);
		clib::Wcstombs(sz, const_cast< wchar_t* >(buff), ulMaxLength);
		sz[ulMaxLength - 1] = '\0';
		return sz;
	}
#endif // GPOS_DEBUG

}

// EOF
