//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerScalarWindowFrameEdge.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing a window frame
//		edge
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarWindowFrameEdge.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarWindowFrameEdge::CParseHandlerScalarWindowFrameEdge
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerScalarWindowFrameEdge::CParseHandlerScalarWindowFrameEdge
	(
	IMemoryPool *pmp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *pphRoot,
	BOOL fLeading
	)
	:
	CParseHandlerScalarOp(pmp, parse_handler_mgr, pphRoot),
	m_fLeading(fLeading)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarWindowFrameEdge::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarWindowFrameEdge::StartElement
	(
	const XMLCh* const element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const element_qname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarWindowFrameLeadingEdge), element_local_name))
	{
		GPOS_ASSERT(NULL == m_pdxln);
		EdxlFrameBoundary edxlfb = CDXLOperatorFactory::Edxlfb(attrs, EdxltokenWindowLeadingBoundary);
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarWindowFrameEdge(m_pmp, true /*fLeading*/, edxlfb));
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarWindowFrameTrailingEdge), element_local_name))
	{
		GPOS_ASSERT(NULL == m_pdxln);
		EdxlFrameBoundary edxlfb = CDXLOperatorFactory::Edxlfb(attrs, EdxltokenWindowTrailingBoundary);
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, GPOS_NEW(m_pmp) CDXLScalarWindowFrameEdge(m_pmp, false /*fLeading*/, edxlfb));
	}
	else
	{
		// we must have seen a Window Frame Edge already and initialized its corresponding node
		if (NULL == m_pdxln)
		{
			CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
		}

		// install a scalar element parser for parsing the frame edge value
		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
		m_pphm->ActivateParseHandler(pphChild);

		// store parse handler
		this->Append(pphChild);

		pphChild->startElement(element_uri, element_local_name, element_qname, attrs);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarWindowFrameEdge::EndElement
//
//	@doc:
//		Processes a Xerces end element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarWindowFrameEdge::EndElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const // element_qname
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarWindowFrameLeadingEdge), element_local_name) ||
	    0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarWindowFrameTrailingEdge), element_local_name)
	)
	{
		const ULONG ulSize = this->Length();
		if (0 < ulSize)
		{
			GPOS_ASSERT(1 == ulSize);
			// limit count node was not empty
			CParseHandlerScalarOp *pphChild = dynamic_cast<CParseHandlerScalarOp *>((*this)[0]);

			AddChildFromParseHandler(pphChild);
		}

		// deactivate handler
		m_pphm->DeactivateHandler();
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}
}

// EOF
