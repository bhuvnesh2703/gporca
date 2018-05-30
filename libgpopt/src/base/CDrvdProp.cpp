//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CDrvdProp.cpp
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

	CDrvdProp::CDrvdProp()
	{}

	IOstream &operator << (IOstream &os, const CDrvdProp &drvdprop)
	{
		return drvdprop.OsPrint(os);
	}

#ifdef GPOS_DEBUG
	void
	CDrvdProp::DbgPrint() const
	{
		IMemoryPool *memory_pool = COptCtxt::PoctxtFromTLS()->Pmp();
		CAutoTrace at(memory_pool);
		at.Os() << *this;
	}
#endif // GPOS_DEBUG
}

// EOF
