//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLColDescr.cpp
//
//	@doc:
//		Implementation of DXL column descriptors
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLColDescr.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/md/CMDIdGPDB.h"

using namespace gpos;
using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::CDXLColDescr
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLColDescr::CDXLColDescr
	(
	IMemoryPool *memory_pool,
	CMDName *md_name,
	ULONG column_id,
	INT attr_no,
	IMDId *column_mdid_type,
	INT type_modifier,
	BOOL is_dropped,
	ULONG width
	)
	:
	m_memory_pool(memory_pool),
	m_md_name(md_name),
	m_column_id(column_id),
	m_attr_no(attr_no),
	m_column_mdid_type(column_mdid_type),
	m_type_modifier(type_modifier),
	m_is_dropped(is_dropped),
	m_column_width(width)
{
	GPOS_ASSERT_IMP(m_is_dropped, 0 == m_md_name->Pstr()->Length());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::~CDXLColDescr
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLColDescr::~CDXLColDescr()
{
	m_column_mdid_type->Release();
	GPOS_DELETE(m_md_name);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::Pmdname
//
//	@doc:
//		Returns the column name
//
//---------------------------------------------------------------------------
const CMDName *
CDXLColDescr::Pmdname() const
{
	return m_md_name;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::Id
//
//	@doc:
//		Returns the column Id
//
//---------------------------------------------------------------------------
ULONG
CDXLColDescr::Id() const
{
	return m_column_id;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::AttrNum
//
//	@doc:
//		Returns the column attribute number in GPDB
//
//---------------------------------------------------------------------------
INT
CDXLColDescr::AttrNum() const
{
	return m_attr_no;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::MDIdType
//
//	@doc:
//		Returns the type id for this column
//
//---------------------------------------------------------------------------
IMDId *
CDXLColDescr::MDIdType() const
{
	return m_column_mdid_type;
}

INT
CDXLColDescr::TypeModifier() const
{
	return m_type_modifier;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::IsDropped
//
//	@doc:
//		Is the column dropped from the relation
//
//---------------------------------------------------------------------------
BOOL
CDXLColDescr::IsDropped() const
{
	return m_is_dropped;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::Width
//
//	@doc:
//		Returns the width of the column
//
//---------------------------------------------------------------------------
ULONG
CDXLColDescr::Width() const
{
	return m_column_width;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColDescr::SerializeToDXL
//
//	@doc:
//		Serializes the column descriptor into DXL format
//
//---------------------------------------------------------------------------
void
CDXLColDescr::SerializeToDXL
	(
	CXMLSerializer *xml_serializer
	)
	const
{
	const CWStringConst *pstrTokenColDescr = CDXLTokens::PstrToken(EdxltokenColDescr);
	
	xml_serializer->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrTokenColDescr);
	
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenColId), m_column_id);
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenAttno), m_attr_no);
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenColName), m_md_name->Pstr());
	m_column_mdid_type->Serialize(xml_serializer, CDXLTokens::PstrToken(EdxltokenTypeId));

	if (IDefaultTypeModifier != TypeModifier())
	{
		xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenTypeMod), TypeModifier());
	}

	if (m_is_dropped)
	{
		xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenColDropped), m_is_dropped);
	}

	if (ULONG_MAX != m_column_width)
	{
		xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenColWidth), m_column_width);
	}
	
	xml_serializer->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrTokenColDescr);

	GPOS_CHECK_ABORT;
}

// EOF
