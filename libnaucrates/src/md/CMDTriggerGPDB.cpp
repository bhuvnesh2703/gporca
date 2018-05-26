//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDTriggerGPDB.cpp
//
//	@doc:
//		Implementation of the class for representing GPDB-specific triggers
//		in the MD cache
//---------------------------------------------------------------------------


#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/CMDTriggerGPDB.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpmd;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CMDTriggerGPDB::CMDTriggerGPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDTriggerGPDB::CMDTriggerGPDB
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	CMDName *pmdname,
	IMDId *pmdidRel,
	IMDId *pmdidFunc,
	INT iType,
	BOOL fEnabled
	)
	:
	m_pmp(pmp),
	m_pmdid(pmdid),
	m_pmdname(pmdname),
	m_pmdidRel(pmdidRel),
	m_pmdidFunc(pmdidFunc),
	m_iType(iType),
	m_fEnabled(fEnabled)
{
	GPOS_ASSERT(m_pmdid->IsValid());
	GPOS_ASSERT(m_pmdidRel->IsValid());
	GPOS_ASSERT(m_pmdidFunc->IsValid());
	GPOS_ASSERT(0 <= iType);

	m_pstr = CDXLUtils::SerializeMDObj(m_pmp, this, false /*fSerializeHeader*/, false /*indentation*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTriggerGPDB::~CMDTriggerGPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDTriggerGPDB::~CMDTriggerGPDB()
{
	m_pmdid->Release();
	m_pmdidRel->Release();
	m_pmdidFunc->Release();
	GPOS_DELETE(m_pmdname);
	GPOS_DELETE(m_pstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTriggerGPDB::FRow
//
//	@doc:
//		Does trigger execute on a row-level
//
//---------------------------------------------------------------------------
BOOL
CMDTriggerGPDB::FRow() const
{
	return (m_iType & GPMD_TRIGGER_ROW);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTriggerGPDB::FBefore
//
//	@doc:
//		Is this a before trigger
//
//---------------------------------------------------------------------------
BOOL
CMDTriggerGPDB::FBefore() const
{
	return (m_iType & GPMD_TRIGGER_BEFORE);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTriggerGPDB::Insert
//
//	@doc:
//		Is this an insert trigger
//
//---------------------------------------------------------------------------
BOOL
CMDTriggerGPDB::Insert() const
{
	return (m_iType & GPMD_TRIGGER_INSERT);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTriggerGPDB::FDelete
//
//	@doc:
//		Is this a delete trigger
//
//---------------------------------------------------------------------------
BOOL
CMDTriggerGPDB::FDelete() const
{
	return (m_iType & GPMD_TRIGGER_DELETE);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTriggerGPDB::FUpdate
//
//	@doc:
//		Is this an update trigger
//
//---------------------------------------------------------------------------
BOOL
CMDTriggerGPDB::FUpdate() const
{
	return (m_iType & GPMD_TRIGGER_UPDATE);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTriggerGPDB::Serialize
//
//	@doc:
//		Serialize trigger metadata in DXL format
//
//---------------------------------------------------------------------------
void
CMDTriggerGPDB::Serialize
	(
	CXMLSerializer *xml_serializer
	)
	const
{
	xml_serializer->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
						CDXLTokens::PstrToken(EdxltokenGPDBTrigger));

	m_pmdid->Serialize(xml_serializer, CDXLTokens::PstrToken(EdxltokenMdid));
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenName), m_pmdname->Pstr());
	m_pmdidRel->Serialize(xml_serializer, CDXLTokens::PstrToken(EdxltokenRelationMdid));
	m_pmdidFunc->Serialize(xml_serializer, CDXLTokens::PstrToken(EdxltokenFuncId));

	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenGPDBTriggerRow), FRow());
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenGPDBTriggerBefore), FBefore());
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenGPDBTriggerInsert), Insert());
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenGPDBTriggerDelete), FDelete());
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenGPDBTriggerUpdate), FUpdate());
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenGPDBTriggerEnabled), m_fEnabled);

	xml_serializer->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
						CDXLTokens::PstrToken(EdxltokenGPDBTrigger));
}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMDTriggerGPDB::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void
CMDTriggerGPDB::DebugPrint
	(
	IOstream &os
	)
	const
{
	os << "Trigger id: ";
	m_pmdid->OsPrint(os);
	os << std::endl;

	os << "Trigger name: " << (Mdname()).Pstr()->Wsz() << std::endl;

	os << "Trigger relation id: ";
	m_pmdidRel->OsPrint(os);
	os << std::endl;

	os << "Trigger function id: ";
	m_pmdidFunc->OsPrint(os);
	os << std::endl;

	if (FRow())
	{
		os << "Trigger level: Row" << std::endl;
	}
	else
	{
		os << "Trigger level: Table" << std::endl;
	}

	if (FBefore())
	{
		os << "Trigger timing: Before" << std::endl;
	}
	else
	{
		os << "Trigger timing: After" << std::endl;
	}

	os << "Trigger statement type(s): [ ";
	if (Insert())
	{
		os << "Insert ";
	}

	if (FDelete())
	{
		os << "Delete ";
	}

	if (FUpdate())
	{
		os << "Update ";
	}
	os << "]" << std::endl;

	if (m_fEnabled)
	{
		os << "Trigger enabled: Yes" << std::endl;
	}
	else
	{
		os << "Trigger enabled: No" << std::endl;
	}
}

#endif // GPOS_DEBUG

// EOF
