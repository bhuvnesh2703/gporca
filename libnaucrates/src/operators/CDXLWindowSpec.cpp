//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLWindowSpec.cpp
//
//	@doc:
//		Implementation of DXL window specification in the DXL
//		representation of the logical query tree
//
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLWindowSpec.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowSpec::CDXLWindowSpec
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLWindowSpec::CDXLWindowSpec
	(
	IMemoryPool *memory_pool,
	ULongPtrArray *pdrgpulPartCol,
	CMDName *pmdname,
	CDXLNode *sort_col_list_dxl,
	CDXLWindowFrame *pdxlwf
	)
	:
	m_memory_pool(memory_pool),
	m_pdrgpulPartCol(pdrgpulPartCol),
	m_mdname(pmdname),
	m_sort_col_list_dxl(sort_col_list_dxl),
	m_pdxlwf(pdxlwf)
{
	GPOS_ASSERT(NULL != m_memory_pool);
	GPOS_ASSERT(NULL != m_pdrgpulPartCol);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowSpec::~CDXLWindowSpec
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLWindowSpec::~CDXLWindowSpec()
{
	m_pdrgpulPartCol->Release();
	CRefCount::SafeRelease(m_pdxlwf);
	CRefCount::SafeRelease(m_sort_col_list_dxl);
	GPOS_DELETE(m_mdname);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLWindowSpec::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLWindowSpec::SerializeToDXL
	(
	CXMLSerializer *xml_serializer
	)
	const
{
	const CWStringConst *element_name = CDXLTokens::PstrToken(EdxltokenWindowSpec);
	xml_serializer->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), element_name);

	GPOS_ASSERT(NULL != m_pdrgpulPartCol);

	// serialize partition keys
	CWStringDynamic *pstrPartCols = CDXLUtils::Serialize(m_memory_pool, m_pdrgpulPartCol);
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenPartKeys), pstrPartCols);
	GPOS_DELETE(pstrPartCols);

	if (NULL != m_mdname)
	{
		xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenAlias), m_mdname->Pstr());
	}

	// serialize sorting columns
	if (NULL != m_sort_col_list_dxl)
	{
		m_sort_col_list_dxl->SerializeToDXL(xml_serializer);
	}

	// serialize window frames
	if (NULL != m_pdxlwf)
	{
		m_pdxlwf->SerializeToDXL(xml_serializer);
	}

	xml_serializer->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), element_name);
}

// EOF
