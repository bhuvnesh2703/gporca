//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarComp.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing scalar
//		comparison operators.
//---------------------------------------------------------------------------


#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

#include "naucrates/dxl/parser/CParseHandlerScalarComp.h"
using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarComp::CParseHandlerScalarComp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerScalarComp::CParseHandlerScalarComp
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
//		CParseHandlerScalarComp::~CParseHandlerScalarComp
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerScalarComp::~CParseHandlerScalarComp()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarComp::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarComp::StartElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const, // element_qname
	const Attributes& attrs
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarComp), element_local_name))
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}
	
	// parse and create comparison operator
	m_pdxlop = (CDXLScalarComp *) CDXLOperatorFactory::PdxlopScalarCmp(m_pphm->Pmm(), attrs);
	
	// create and activate the parse handler for the children nodes in reverse
	// order of their expected appearance
	
	// parse handler for right scalar node
	CParseHandlerBase *pphRight = CParseHandlerFactory::Pph(m_memory_pool, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
	m_pphm->ActivateParseHandler(pphRight);
	
	// parse handler for left scalar node
	CParseHandlerBase *pphLeft = CParseHandlerFactory::Pph(m_memory_pool, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
	m_pphm->ActivateParseHandler(pphLeft);
	
	// store parse handlers
	this->Append(pphLeft);
	this->Append(pphRight);

}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarComp::EndElement
//
//	@doc:
//		Processes a Xerces end element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarComp::EndElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const // element_qname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarComp), element_local_name))
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}

	// construct node from the created child nodes
	m_pdxln = GPOS_NEW(m_memory_pool) CDXLNode(m_memory_pool, m_pdxlop);

	CParseHandlerScalarOp *pphLeft = dynamic_cast<CParseHandlerScalarOp *>((*this)[0]);
	CParseHandlerScalarOp *pphRight = dynamic_cast<CParseHandlerScalarOp *>((*this)[1]);

	// add constructed children
	AddChildFromParseHandler(pphLeft);
	AddChildFromParseHandler(pphRight);
	
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF

