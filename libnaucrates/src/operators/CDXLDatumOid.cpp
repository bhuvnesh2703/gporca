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
	IMemoryPool *memory_pool,
	IMDId *mdid_type,
	BOOL is_null,
	OID oidVal
	)
	:
	CDXLDatum(memory_pool, mdid_type, IDefaultTypeModifier, is_null, 4 /*length*/ ),
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
	m_mdid_type->Serialize(xml_serializer, CDXLTokens::PstrToken(EdxltokenTypeId));
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenIsNull), m_is_null);
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenIsByValue), IsPassedByValue());

	if (!m_is_null)
	{
		xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenValue), m_oidVal);
	}
}

// EOF
