//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalResult.cpp
//
//	@doc:
//		Implementation of DXL physical result operator
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLPhysicalResult.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalResult::CDXLPhysicalResult
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalResult::CDXLPhysicalResult
	(
	IMemoryPool *memory_pool
	)
	:
	CDXLPhysical(memory_pool)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalResult::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalResult::Edxlop() const
{
	return EdxlopPhysicalResult;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalResult::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalResult::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalResult);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalResult::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalResult::SerializeToDXL
	(
	CXMLSerializer *xml_serializer,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	xml_serializer->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	// serialize properties
	pdxln->SerializePropertiesToDXL(xml_serializer);

	// serialize children
	pdxln->SerializeChildrenToDXL(xml_serializer);

	xml_serializer->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalResult::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalResult::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{

	GPOS_ASSERT(EdxlresultIndexSentinel >= pdxln->Arity());
	
	// check that one time filter is valid
	CDXLNode *pdxlnOneTimeFilter = (*pdxln)[EdxlresultIndexOneTimeFilter];
	GPOS_ASSERT(EdxlopScalarOneTimeFilter == pdxlnOneTimeFilter->Pdxlop()->Edxlop());
	
	if (fValidateChildren)
	{
		pdxlnOneTimeFilter->Pdxlop()->AssertValid(pdxlnOneTimeFilter, fValidateChildren);
	}
	
	if (EdxlresultIndexSentinel == pdxln->Arity())
	{
		CDXLNode *pdxlnChild = (*pdxln)[EdxlresultIndexChild];
		GPOS_ASSERT(EdxloptypePhysical == pdxlnChild->Pdxlop()->Edxloperatortype());

		if (fValidateChildren)
		{
			pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
		}
	}

}
#endif // GPOS_DEBUG

// EOF
