//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarProjList.cpp
//
//	@doc:
//		Implementation of DXL projection list operators
//---------------------------------------------------------------------------
#include "naucrates/dxl/operators/CDXLScalarProjList.h"

#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarProjList::CDXLScalarProjList
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarProjList::CDXLScalarProjList
	(
	IMemoryPool *memory_pool
	)
	:
	CDXLScalar(memory_pool)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarProjList::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarProjList::Edxlop() const
{
	return EdxlopScalarProjectList;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarProjList::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarProjList::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarProjList);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarProjList::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarProjList::SerializeToDXL
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
//		CDXLScalarProjList::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarProjList::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren 
	) 
	const
{
	const ULONG ulArity = pdxln->Arity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CDXLNode *pdxlnChild = (*pdxln)[ul];
		GPOS_ASSERT(EdxlopScalarProjectElem == pdxlnChild->Pdxlop()->Edxlop());
		
		if (fValidateChildren)
		{
			pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
		}
	}
}

#endif // GPOS_DEBUG


// EOF
