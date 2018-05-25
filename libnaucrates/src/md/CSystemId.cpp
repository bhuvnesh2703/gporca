//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSystemId.cpp
//
//	@doc:
//		Implementation of system identifiers
//---------------------------------------------------------------------------


#include "naucrates/md/CSystemId.h"

using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CSystemId::CSystemId
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CSystemId::CSystemId
	(
	IMDId::EMDIdType emdidt,
	const WCHAR *wsz,
	ULONG ulLength
	)
	:
	m_emdidt(emdidt)
{
	GPOS_ASSERT(GPDXL_SYSID_LENGTH >= ulLength);

	if (ulLength > 0)
	{
		clib::WcStrNCpy(m_wsz, wsz, ulLength);
	}
	
	// ensure string is terminated
	m_wsz[ulLength] = WCHAR_EOS;
}

//---------------------------------------------------------------------------
//	@function:
//		CSystemId::CSystemId
//
//	@doc:
//		Copy constructor
//
//---------------------------------------------------------------------------
CSystemId::CSystemId
	(
	const CSystemId &sysid
	)
	:
	m_emdidt(sysid.Emdidt())
{
	clib::WcStrNCpy(m_wsz, sysid.GetBuffer(), GPDXL_SYSID_LENGTH);
}

//---------------------------------------------------------------------------
//	@function:
//		CSystemId::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CSystemId::Equals
	(
	const CSystemId &sysid
	)
	const
{
	ULONG ulLength = GPOS_WSZ_LENGTH(m_wsz);
	return ulLength == GPOS_WSZ_LENGTH(sysid.m_wsz) &&
			0 == clib::WcStrNCmp(m_wsz, sysid.m_wsz, ulLength);
}

//---------------------------------------------------------------------------
//	@function:
//		CSystemId::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CSystemId::HashValue() const
{
	return gpos::HashByteArray((BYTE*) m_wsz, GPOS_WSZ_LENGTH(m_wsz) * GPOS_SIZEOF(WCHAR));
}

// EOF
