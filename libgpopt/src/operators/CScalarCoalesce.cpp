//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarCoalesce.cpp
//
//	@doc:
//		Implementation of scalar coalesce operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/COptCtxt.h"

#include "gpopt/mdcache/CMDAccessorUtils.h"

#include "gpopt/operators/CScalarCoalesce.h"
#include "gpopt/operators/CExpressionHandle.h"

#include "naucrates/md/IMDTypeBool.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarCoalesce::CScalarCoalesce
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarCoalesce::CScalarCoalesce
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
//		CScalarCoalesce::~CScalarCoalesce
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScalarCoalesce::~CScalarCoalesce()
{
	m_pmdidType->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCoalesce::HashValue
//
//	@doc:
//		Operator specific hash function; combined hash of operator id and
//		return type id
//
//---------------------------------------------------------------------------
ULONG
CScalarCoalesce::HashValue() const
{
	return gpos::CombineHashes(COperator::HashValue(), m_pmdidType->HashValue());
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCoalesce::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarCoalesce::FMatch
	(
	COperator *pop
	)
	const
{
	if(pop->Eopid() == Eopid())
	{
		CScalarCoalesce *popScCoalesce = CScalarCoalesce::PopConvert(pop);

		// match if return types are identical
		return popScCoalesce->MDIdType()->Equals(m_pmdidType);
	}

	return false;
}


// EOF

