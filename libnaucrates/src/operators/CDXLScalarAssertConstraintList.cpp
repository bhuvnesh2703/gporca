//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Inc.
//
//	@filename:
//		CDXLScalarAssertConstraintList.cpp
//
//	@doc:
//		Implementation of DXL scalar assert constraint lists
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarAssertConstraintList.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAssertConstraintList::CDXLScalarAssertConstraintList
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarAssertConstraintList::CDXLScalarAssertConstraintList
	(
	IMemoryPool *memory_pool
	)
	:
	CDXLScalar(memory_pool)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAssertConstraintList::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarAssertConstraintList::Edxlop() const
{
	return EdxlopScalarAssertConstraintList;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAssertConstraintList::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarAssertConstraintList::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarAssertConstraintList);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAssertConstraintList::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarAssertConstraintList::SerializeToDXL
	(
	CXMLSerializer *xml_serializer,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *element_name = PstrOpName();

	xml_serializer->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), element_name);
	pdxln->SerializeChildrenToDXL(xml_serializer);
	xml_serializer->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), element_name);
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAssertConstraintList::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarAssertConstraintList::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	)
	const
{
	const ULONG ulArity = pdxln->Arity();
	GPOS_ASSERT(0 < ulArity);
	
	for (ULONG ul = 0; ul < ulArity; ++ul)
	{
		CDXLNode *pdxlnChild = (*pdxln)[ul];
		GPOS_ASSERT(EdxlopScalarAssertConstraint == pdxlnChild->Pdxlop()->Edxlop());

		if (fValidateChildren)
		{
			pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
		}
	}
}
#endif // GPOS_DEBUG

// EOF
