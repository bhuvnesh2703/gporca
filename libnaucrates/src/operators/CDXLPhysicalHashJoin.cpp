//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalHashJoin.cpp
//
//	@doc:
//		Implementation of DXL physical hash join operator
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLPhysicalHashJoin.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalHashJoin::CDXLPhysicalHashJoin
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalHashJoin::CDXLPhysicalHashJoin
	(
	IMemoryPool *memory_pool,
	EdxlJoinType edxljt
	)
	:
	CDXLPhysicalJoin(memory_pool, edxljt)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalHashJoin::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalHashJoin::Edxlop() const
{
	return EdxlopPhysicalHashJoin;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalHashJoin::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalHashJoin::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalHashJoin);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalHashJoin::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalHashJoin::SerializeToDXL
	(
	CXMLSerializer *xml_serializer,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *element_name = PstrOpName();
	
	xml_serializer->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), element_name);
	
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenJoinType), PstrJoinTypeName());
	
	// serialize properties
	pdxln->SerializePropertiesToDXL(xml_serializer);
	
	// serialize children
	pdxln->SerializeChildrenToDXL(xml_serializer);
	
	xml_serializer->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), element_name);		
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalHashJoin::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLPhysicalHashJoin::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) const
{
	// assert proj list and filter are valid
	CDXLPhysical::AssertValid(pdxln, fValidateChildren);
	
	GPOS_ASSERT(EdxlhjIndexSentinel == pdxln->Arity());
	GPOS_ASSERT(EdxljtSentinel > Edxltype());
	
	CDXLNode *pdxlnJoinFilter = (*pdxln)[EdxlhjIndexJoinFilter];
	CDXLNode *pdxlnHashClauses = (*pdxln)[EdxlhjIndexHashCondList];
	CDXLNode *pdxlnLeft = (*pdxln)[EdxlhjIndexHashLeft];
	CDXLNode *pdxlnRight = (*pdxln)[EdxlhjIndexHashRight];

	// assert children are of right type (physical/scalar)
	GPOS_ASSERT(EdxlopScalarJoinFilter == pdxlnJoinFilter->Pdxlop()->Edxlop());
	GPOS_ASSERT(EdxlopScalarHashCondList == pdxlnHashClauses->Pdxlop()->Edxlop());
	GPOS_ASSERT(EdxloptypePhysical == pdxlnLeft->Pdxlop()->Edxloperatortype());
	GPOS_ASSERT(EdxloptypePhysical == pdxlnRight->Pdxlop()->Edxloperatortype());

	if (fValidateChildren)
	{
		pdxlnJoinFilter->Pdxlop()->AssertValid(pdxlnJoinFilter, fValidateChildren);
		pdxlnHashClauses->Pdxlop()->AssertValid(pdxlnHashClauses, fValidateChildren);
		pdxlnLeft->Pdxlop()->AssertValid(pdxlnLeft, fValidateChildren);
		pdxlnRight->Pdxlop()->AssertValid(pdxlnRight, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
