//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerPhysicalOp.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing physical operators.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalOp::CParseHandlerPhysicalOp
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerPhysicalOp::CParseHandlerPhysicalOp
	(
	IMemoryPool *pmp, 
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerOp(pmp, parse_handler_mgr, pphRoot)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalOp::~CParseHandlerPhysicalOp
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CParseHandlerPhysicalOp::~CParseHandlerPhysicalOp()
{
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalOp::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag of a physical operator node.
//		This function serves as a dispatcher for invoking the correct processing
//		function for the respective operator type.
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalOp::StartElement
	(
	const XMLCh* const element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const element_qname,
	const Attributes& attrs
	)
{
	// instantiate the parse handler
	CParseHandlerBase *pph = CParseHandlerFactory::Pph(m_pmp, element_local_name, m_pphm, this);

	GPOS_ASSERT(NULL != pph);

	// activate the parse handler
	m_pphm->ReplaceHandler(pph, m_pphRoot);
		
	// pass the startElement message for the specialized parse handler to process
	pph->startElement(element_uri, element_local_name, element_qname, attrs);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalOp::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag.
//		This function should never be called. Instead, the endElement function
//		of the parse handler for the actual physical operator type is called.
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalOp::EndElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const, // element_local_name,
	const XMLCh* const // element_qname
	)
{
	GPOS_ASSERT(!"Invalid call of endElement inside CParseHandlerPhysicalOp");
}



// EOF

