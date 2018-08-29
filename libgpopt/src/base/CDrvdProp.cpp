//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		DrvdPropArray.cpp
//
//	@doc:
//		Implementation of derived properties
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CDrvdProp.h"
#include "gpopt/operators/COperator.h"

#ifdef GPOS_DEBUG
#include "gpopt/base/COptCtxt.h"
#include "gpos/error/CAutoTrace.h"
#endif // GPOS_DEBUG

namespace gpopt {

	DrvdPropArray::DrvdPropArray()
	{}

	IOstream &operator << (IOstream &os, const DrvdPropArray &drvdprop)
	{
		return drvdprop.OsPrint(os);
	}

#ifdef GPOS_DEBUG
	CHAR *
	DrvdPropArray::DbgPrint() const
	{
		IMemoryPool *mp = COptCtxt::PoctxtFromTLS()->Pmp();
		CAutoTrace at(mp);
		at.Os() << *this;
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
