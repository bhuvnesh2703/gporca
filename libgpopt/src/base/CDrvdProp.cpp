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
#include "gpopt/base/COptCtxt.h"
#include "gpos/error/CAutoTrace.h"

namespace gpopt {

  //---------------------------------------------------------------------------
  //	@function:
  //		CDrvdProp::CDrvdProp
  //
  //	@doc:
  //		ctor
  //
  //---------------------------------------------------------------------------
  CDrvdProp::CDrvdProp()
  {}

  IOstream &operator << (IOstream &os, CDrvdProp &drvdprop)
  {
    return drvdprop.OsPrint(os);
  }
	
void
CDrvdProp::DbgPrint()
{
	IMemoryPool *pmp = COptCtxt::PoctxtFromTLS()->Pmp();
	CAutoTrace at(pmp);
	at.Os() << *this;
}

}

// EOF
