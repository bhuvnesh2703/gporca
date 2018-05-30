//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalDML.cpp
//
//	@doc:
//		Implementation of DXL physical DML operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLDirectDispatchInfo.h"
#include "naucrates/dxl/operators/CDXLPhysicalDML.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDML::CDXLPhysicalDML
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLPhysicalDML::CDXLPhysicalDML
	(
	IMemoryPool *memory_pool,
	const EdxlDmlType edxldmltype,
	CDXLTableDescr *pdxltabdesc,
	ULongPtrArray *pdrgpul,
	ULONG ulAction,
	ULONG ulOid,
	ULONG ulCtid,
	ULONG ulSegmentId,
	BOOL fPreserveOids,
	ULONG ulTupleOid,
	CDXLDirectDispatchInfo *pdxlddinfo,
	BOOL fInputSorted
	)
	:
	CDXLPhysical(memory_pool),
	m_edxldmltype(edxldmltype),
	m_pdxltabdesc(pdxltabdesc),
	m_pdrgpul(pdrgpul),
	m_ulAction(ulAction),
	m_ulOid(ulOid),
	m_ulCtid(ulCtid),
	m_ulSegmentId(ulSegmentId),
	m_fPreserveOids(fPreserveOids),
	m_ulTupleOid(ulTupleOid),
	m_pdxlddinfo(pdxlddinfo),
	m_fInputSorted(fInputSorted)
{
	GPOS_ASSERT(EdxldmlSentinel > edxldmltype);
	GPOS_ASSERT(NULL != pdxltabdesc);
	GPOS_ASSERT(NULL != pdrgpul);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDML::~CDXLPhysicalDML
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLPhysicalDML::~CDXLPhysicalDML()
{
	m_pdxltabdesc->Release();
	m_pdrgpul->Release();
	CRefCount::SafeRelease(m_pdxlddinfo);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDML::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalDML::Edxlop() const
{
	return EdxlopPhysicalDML;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDML::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalDML::PstrOpName() const
{
	switch (m_edxldmltype)
	{
		case Edxldmlinsert:
				return CDXLTokens::PstrToken(EdxltokenPhysicalDMLInsert);
		case Edxldmldelete:
				return CDXLTokens::PstrToken(EdxltokenPhysicalDMLDelete);
		case Edxldmlupdate:
				return CDXLTokens::PstrToken(EdxltokenPhysicalDMLUpdate);
		default:
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDML::SerializeToDXL
//
//	@doc:
//		Serialize function descriptor in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalDML::SerializeToDXL
	(
	CXMLSerializer *xml_serializer,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	xml_serializer->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	CWStringDynamic *pstrCols = CDXLUtils::Serialize(m_memory_pool, m_pdrgpul);
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenColumns), pstrCols);
	GPOS_DELETE(pstrCols);

	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenActionColId), m_ulAction);
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenOidColId), m_ulOid);
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenCtidColId), m_ulCtid);
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenGpSegmentIdColId), m_ulSegmentId);
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenInputSorted), m_fInputSorted);
	
	if (Edxldmlupdate == m_edxldmltype)
	{
		xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenUpdatePreservesOids), m_fPreserveOids);
	}

	if (m_fPreserveOids)
	{
		xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenTupleOidColId), m_ulTupleOid);
	}
	
	pdxln->SerializePropertiesToDXL(xml_serializer);

	if (NULL != m_pdxlddinfo)
	{
		m_pdxlddinfo->Serialize(xml_serializer);
	}
	else
	{
		// TODO:  - Oct 22, 2014; clean this code once the direct dispatch code for DML and SELECT is unified
		xml_serializer->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenDirectDispatchInfo));
		xml_serializer->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenDirectDispatchInfo));
	}
	
	// serialize project list
	(*pdxln)[0]->SerializeToDXL(xml_serializer);

	// serialize table descriptor
	m_pdxltabdesc->SerializeToDXL(xml_serializer);
	
	// serialize physical child
	(*pdxln)[1]->SerializeToDXL(xml_serializer);

	xml_serializer->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDML::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalDML::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	GPOS_ASSERT(2 == pdxln->UlArity());
	CDXLNode *pdxlnChild = (*pdxln)[1];
	GPOS_ASSERT(EdxloptypePhysical == pdxlnChild->Pdxlop()->Edxloperatortype());

	if (fValidateChildren)
	{
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
}

#endif // GPOS_DEBUG


// EOF
