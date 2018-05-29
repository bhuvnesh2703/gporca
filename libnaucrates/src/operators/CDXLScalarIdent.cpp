//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarIdent.cpp
//
//	@doc:
//		Implementation of DXL scalar identifier operators
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarIdent.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpos;
using namespace gpopt;
using namespace gpdxl;



//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarIdent::CDXLScalarIdent
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarIdent::CDXLScalarIdent
	(
	IMemoryPool *pmp,
	CDXLColRef *pdxlcr
	)
	:
	CDXLScalar(pmp),
	m_pdxlcr(pdxlcr)
{
	GPOS_ASSERT(NULL != m_pdxlcr);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarIdent::~CDXLScalarIdent
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLScalarIdent::~CDXLScalarIdent()
{
	m_pdxlcr->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarIdent::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarIdent::Edxlop() const
{
	return EdxlopScalarIdent;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarIdent::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarIdent::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarIdent);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarIdent::Pdxlcr
//
//	@doc:
//		Return column reference of the identifier operator
//
//---------------------------------------------------------------------------
const CDXLColRef *
CDXLScalarIdent::Pdxlcr() const
{
	return m_pdxlcr;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarIdent::MDIdType
//
//	@doc:
//		Return the id of the column type
//
//---------------------------------------------------------------------------
IMDId *
CDXLScalarIdent::MDIdType() const
{
	return m_pdxlcr->MDIdType();
}

INT
CDXLScalarIdent::TypeModifier() const
{
	return m_pdxlcr->TypeModifier();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarIdent::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarIdent::SerializeToDXL
	(
	CXMLSerializer *xml_serializer,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	
	xml_serializer->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
		
	// add col name and col id
	const CWStringConst *strCName = (m_pdxlcr->Pmdname())->Pstr(); 

	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenColId), m_pdxlcr->Id());
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenColName), strCName);
	m_pdxlcr->MDIdType()->Serialize(xml_serializer, CDXLTokens::PstrToken(EdxltokenTypeId));

	if (IDefaultTypeModifier != TypeModifier())
	{
		xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenTypeMod), TypeModifier());
	}

	pdxln->SerializeChildrenToDXL(xml_serializer);

	xml_serializer->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);	
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarIdent::FBoolean
//
//	@doc:
//		Does the operator return boolean result
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarIdent::FBoolean
	(
	CMDAccessor *pmda
	)
	const
{
	return (IMDType::EtiBool == pmda->Pmdtype(m_pdxlcr->MDIdType())->Eti());
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarIdent::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarIdent::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL // fValidateChildren 
	) 
	const
{
	GPOS_ASSERT(0 == pdxln->UlArity());
	GPOS_ASSERT(m_pdxlcr->MDIdType()->IsValid());
	GPOS_ASSERT(NULL != m_pdxlcr);
}
#endif // GPOS_DEBUG

// EOF
