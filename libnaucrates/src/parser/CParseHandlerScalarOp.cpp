//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarOp.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing scalar operators.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarOp::CParseHandlerScalarOp
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerScalarOp::CParseHandlerScalarOp
	(
	IMemoryPool *memory_pool,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
	:
	CParseHandlerOp(memory_pool, parse_handler_mgr, parse_handler_root)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarOp::~CParseHandlerScalarOp
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CParseHandlerScalarOp::~CParseHandlerScalarOp()
{
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarOp::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag for a scalar operator.
//		The function serves as a dispatcher for invoking the correct processing
//		function of the actual operator type.
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarOp::StartElement
	(
	const XMLCh* const element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const element_qname,
	const Attributes& attrs
	)
{
	// instantiate the parse handler
	CParseHandlerBase *pph = CParseHandlerFactory::GetParseHandler(m_memory_pool, element_local_name, m_parse_handler_mgr, this);
	
	GPOS_ASSERT(NULL != pph);
	
	// activate the specialized parse handler
	m_parse_handler_mgr->ReplaceHandler(pph, m_parse_handler_root);
	
	// pass the startElement message for the specialized parse handler to process
	pph->startElement(element_uri, element_local_name, element_qname, attrs);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarOp::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag.
//		This function should never be called. Instead, the endElement function
//		of the parse handler for the actual physical operator type is called.
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarOp::EndElement
	(
	const XMLCh* const, //= element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const // element_qname,
	)
{
	CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_parse_handler_mgr->Pmm(), element_local_name);
	GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
}


// EOF

