//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarIdent.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing
//		scalar identifier  operators.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarIdent.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarIdent::CParseHandlerScalarIdent
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerScalarIdent::CParseHandlerScalarIdent
	(
	IMemoryPool *pmp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerScalarOp(pmp, parse_handler_mgr, pphRoot),
	m_pdxlop(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarIdent::~CParseHandlerScalarIdent
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CParseHandlerScalarIdent::~CParseHandlerScalarIdent()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarIdent::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarIdent::StartElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const, // element_qname
	const Attributes& attrs
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarIdent), element_local_name))
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}
	
	// parse and create identifier operator
	m_pdxlop = (CDXLScalarIdent *) CDXLOperatorFactory::PdxlopScalarIdent(m_pphm->Pmm(), attrs);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarIdent::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarIdent::EndElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const // element_qname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarIdent), element_local_name))
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}
	
	// construct scalar ident node
	GPOS_ASSERT(NULL != m_pdxlop);
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp);
	m_pdxln->SetOperator(m_pdxlop);
			
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF

