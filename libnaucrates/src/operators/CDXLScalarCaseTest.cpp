//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarCaseTest.cpp
//
//	@doc:
//		Implementation of DXL case test
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarCaseTest.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpopt;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::CDXLScalarCaseTest
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarCaseTest::CDXLScalarCaseTest
	(
	IMemoryPool *memory_pool,
	IMDId *mdid_type
	)
	:
	CDXLScalar(memory_pool),
	m_mdid_type(mdid_type)
{
	GPOS_ASSERT(m_mdid_type->IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::~CDXLScalarCaseTest
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarCaseTest::~CDXLScalarCaseTest()
{
	m_mdid_type->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarCaseTest::Edxlop() const
{
	return EdxlopScalarCaseTest;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarCaseTest::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarCaseTest);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::MDIdType
//
//	@doc:
//		Return type id
//
//---------------------------------------------------------------------------
IMDId *
CDXLScalarCaseTest::MDIdType() const
{
	return m_mdid_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarCaseTest::SerializeToDXL
	(
	CXMLSerializer *xml_serializer,
	const CDXLNode * //pdxln
	)
	const
{
	const CWStringConst *element_name = PstrOpName();

	xml_serializer->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), element_name);
	m_mdid_type->Serialize(xml_serializer, CDXLTokens::PstrToken(EdxltokenTypeId));
	xml_serializer->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), element_name);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::FBoolean
//
//	@doc:
//		Does the operator return a boolean result
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarCaseTest::FBoolean
	(
	CMDAccessor *pmda
	)
	const
{
	return (IMDType::EtiBool == pmda->Pmdtype(m_mdid_type)->Eti());
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCaseTest::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarCaseTest::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL // fValidateChildren
	)
	const
{
	GPOS_ASSERT(0 == pdxln->Arity());
	GPOS_ASSERT(m_mdid_type->IsValid());
}
#endif // GPOS_DEBUG

// EOF
