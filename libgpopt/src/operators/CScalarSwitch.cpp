//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarSwitch.cpp
//
//	@doc:
//		Implementation of scalar switch operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CScalarSwitch.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/mdcache/CMDAccessorUtils.h"


using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarSwitch::CScalarSwitch
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarSwitch::CScalarSwitch
	(
	IMemoryPool *pmp,
	IMDId *pmdidType
	)
	:
	CScalar(pmp),
	m_pmdidType(pmdidType),
	m_fBoolReturnType(false)
{
	GPOS_ASSERT(pmdidType->IsValid());

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	m_fBoolReturnType = CMDAccessorUtils::FBoolType(pmda, m_pmdidType);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarSwitch::~CScalarSwitch
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScalarSwitch::~CScalarSwitch()
{
	m_pmdidType->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarSwitch::HashValue
//
//	@doc:
//		Operator specific hash function; combined hash of operator id and
//		return type id
//
//---------------------------------------------------------------------------
ULONG
CScalarSwitch::HashValue() const
{
	return gpos::CombineHashes(COperator::HashValue(), m_pmdidType->HashValue());
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarSwitch::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarSwitch::FMatch
	(
	COperator *pop
	)
	const
{
	if(pop->Eopid() == Eopid())
	{
		CScalarSwitch *popScSwitch = CScalarSwitch::PopConvert(pop);

		// match if return types are identical
		return popScSwitch->MDIdType()->Equals(m_pmdidType);
	}

	return false;
}


// EOF

