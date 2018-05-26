//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLDatumOid.cpp
//
//	@doc:
//		Implementation of DXL datum of type oid
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLDatumOid.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumOid::CDXLDatumOid
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLDatumOid::CDXLDatumOid
	(
	IMemoryPool *pmp,
	IMDId *pmdidType,
	BOOL fNull,
	OID oidVal
	)
	:
	CDXLDatum(pmp, pmdidType, IDefaultTypeModifier, fNull, 4 /*ulLength*/ ),
	m_oidVal(oidVal)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumOid::OidValue
//
//	@doc:
//		Return the oid value
//
//---------------------------------------------------------------------------
OID
CDXLDatumOid::OidValue() const
{
	return m_oidVal;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumOid::Serialize
//
//	@doc:
//		Serialize datum in DXL format
//
//---------------------------------------------------------------------------
void
CDXLDatumOid::Serialize
	(
	CXMLSerializer *xml_serializer
	)
{
	m_pmdidType->Serialize(xml_serializer, CDXLTokens::PstrToken(EdxltokenTypeId));
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenIsNull), m_fNull);
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenIsByValue), FByValue());

	if (!m_fNull)
	{
		xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenValue), m_oidVal);
	}
}

// EOF
