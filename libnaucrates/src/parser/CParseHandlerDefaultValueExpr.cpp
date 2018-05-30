//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerDefaultValueExpr.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing default value expression
//---------------------------------------------------------------------------


#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

#include "naucrates/dxl/parser/CParseHandlerDefaultValueExpr.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDefaultValueExpr::CParseHandlerDefaultValueExpr
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerDefaultValueExpr::CParseHandlerDefaultValueExpr
	(
	IMemoryPool *pmp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerScalarOp(pmp, parse_handler_mgr, pphRoot),
	m_fDefaultValueStarted(false)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDefaultValueExpr::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerDefaultValueExpr::StartElement
	(
	const XMLCh* const element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const element_qname,
	const Attributes& attrs
	)
{
	if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenColumnDefaultValue), element_local_name))
	{
		// opening tag for a default expression: assert no other tag has been seen yet
		GPOS_ASSERT(!m_fDefaultValueStarted);
		m_fDefaultValueStarted = true;
	}
	else
	{
		GPOS_ASSERT(m_fDefaultValueStarted);
		
		// install a scalar op parse handler to parse the expression
		CParseHandlerBase *pph = CParseHandlerFactory::Pph(m_pmp, element_local_name, m_pphm, this);
		
		GPOS_ASSERT(NULL != pph);

		// activate the child parse handler
		m_pphm->ActivateParseHandler(pph);
		
		// pass the startElement message for the specialized parse handler to process
		pph->startElement(element_uri, element_local_name, element_qname, attrs);
		
		// store parse handlers
		this->Append(pph);
	}

}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDefaultValueExpr::EndElement
//
//	@doc:
//		Processes a Xerces end element event
//
//---------------------------------------------------------------------------
void
CParseHandlerDefaultValueExpr::EndElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const // element_qname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenColumnDefaultValue), element_local_name))
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}

	if (0 < this->Length())
	{
		GPOS_ASSERT(1 == this->Length());
		
		// get node for default value expression from child parse handler
		CParseHandlerScalarOp *pphChild = dynamic_cast<CParseHandlerScalarOp *>((*this)[0]);
		m_pdxln = pphChild->Pdxln();
		m_pdxln->AddRef();
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF

