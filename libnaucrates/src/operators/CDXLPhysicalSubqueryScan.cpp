//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalSubqueryScan.cpp
//
//	@doc:
//		Implementation of DXL physical subquery scan operators
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLPhysicalSubqueryScan.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSubqueryScan::CDXLPhysicalSubqueryScan
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalSubqueryScan::CDXLPhysicalSubqueryScan
	(
	IMemoryPool *memory_pool,
	CMDName *pmdname
	)
	:
	CDXLPhysical(memory_pool),
	m_pmdnameAlias(pmdname)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSubqueryScan::~CDXLPhysicalSubqueryScan
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLPhysicalSubqueryScan::~CDXLPhysicalSubqueryScan()
{
	GPOS_DELETE(m_pmdnameAlias);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSubqueryScan::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalSubqueryScan::Edxlop() const
{
	return EdxlopPhysicalSubqueryScan;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSubqueryScan::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalSubqueryScan::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalSubqueryScan);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSubqueryScan::MdName
//
//	@doc:
//		Name for the subquery
//
//---------------------------------------------------------------------------
const CMDName *
CDXLPhysicalSubqueryScan::MdName()
{
	return m_pmdnameAlias;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSubqueryScan::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalSubqueryScan::SerializeToDXL
	(
	CXMLSerializer *xml_serializer,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	
	xml_serializer->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenAlias), m_pmdnameAlias->Pstr());
	
	// serialize properties
	pdxln->SerializePropertiesToDXL(xml_serializer);
	
	// serialize children
	pdxln->SerializeChildrenToDXL(xml_serializer);
		
	xml_serializer->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);		
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSubqueryScan::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLPhysicalSubqueryScan::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	// assert proj list and filter are valid
	CDXLPhysical::AssertValid(pdxln, fValidateChildren);
	
	// subquery scan has 3 children
	GPOS_ASSERT(EdxlsubqscanIndexSentinel == pdxln->Arity());
	
	CDXLNode *pdxlnChild = (*pdxln)[EdxlsubqscanIndexChild];
	GPOS_ASSERT(EdxloptypePhysical == pdxlnChild->Pdxlop()->Edxloperatortype());
	
	if (fValidateChildren)
	{
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
	
	// assert validity of table descriptor
	GPOS_ASSERT(NULL != m_pmdnameAlias);
	GPOS_ASSERT(m_pmdnameAlias->Pstr()->IsValid());
}
#endif // GPOS_DEBUG

// EOF
