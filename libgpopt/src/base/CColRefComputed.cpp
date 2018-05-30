//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CColRefComputed.cpp
//
//	@doc:
//		Implementation of column reference class for computed columns
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CColRefComputed.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CColRefComputed::CColRefComputed
//
//	@doc:
//		ctor
//		takes ownership of string; verify string is properly formatted
//
//---------------------------------------------------------------------------
CColRefComputed::CColRefComputed
	(
	const IMDType *pmdtype,
	INT type_modifier,
	ULONG ulId,
	const CName *pname
	)
	:
	CColRef(pmdtype, type_modifier, ulId, pname)
{
	GPOS_ASSERT(NULL != pmdtype);
	GPOS_ASSERT(pmdtype->MDId()->IsValid());
	GPOS_ASSERT(NULL != pname);
}


//---------------------------------------------------------------------------
//	@function:
//		CColRefComputed::~CColRefComputed
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CColRefComputed::~CColRefComputed()
{}


// EOF

