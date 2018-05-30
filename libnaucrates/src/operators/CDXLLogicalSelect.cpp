//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLLogicalSelect.cpp
//
//	@doc:
//		Implementation of DXL logical select operator
//		
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLLogicalSelect.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalSelect::CDXLLogicalSelect
//
//	@doc:
//		Construct a DXL Logical select node
//
//---------------------------------------------------------------------------
CDXLLogicalSelect::CDXLLogicalSelect
	(
	IMemoryPool *memory_pool
	)
	:CDXLLogical(memory_pool)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalSelect::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLLogicalSelect::Edxlop() const
{
	return EdxlopLogicalSelect;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalSelect::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLLogicalSelect::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenLogicalSelect);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalSelect::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLLogicalSelect::SerializeToDXL
	(
	CXMLSerializer *xml_serializer,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	xml_serializer->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	// serialize children
	pdxln->SerializeChildrenToDXL(xml_serializer);

	xml_serializer->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalSelect::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLLogicalSelect::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) const
{
	GPOS_ASSERT(2 == pdxln->Arity());

	CDXLNode *pdxlnCond = (*pdxln)[0];
	CDXLNode *pdxlnChild = (*pdxln)[1];

	GPOS_ASSERT(EdxloptypeScalar ==  pdxlnCond->Pdxlop()->Edxloperatortype());
	GPOS_ASSERT(EdxloptypeLogical == pdxlnChild->Pdxlop()->Edxloperatortype());
	
	if (fValidateChildren)
	{
		pdxlnCond->Pdxlop()->AssertValid(pdxlnCond, fValidateChildren);
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
