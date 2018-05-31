//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerTableDescr.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing a table descriptor portion
//		of a table scan node.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerTableDescr.h"

#include "naucrates/dxl/parser/CParseHandlerColDescr.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTableDescr::CParseHandlerTableDescr
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerTableDescr::CParseHandlerTableDescr
	(
	IMemoryPool *memory_pool,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
	:
	CParseHandlerBase(memory_pool, parse_handler_mgr, parse_handler_root),
	m_pdxltabdesc(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTableDescr::~CParseHandlerTableDescr
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CParseHandlerTableDescr::~CParseHandlerTableDescr()
{
	CRefCount::SafeRelease(m_pdxltabdesc);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTableDescr::Pdxltabdesc
//
//	@doc:
//		Returns the table descriptor constructed by the parse handler
//
//---------------------------------------------------------------------------
CDXLTableDescr *
CParseHandlerTableDescr::Pdxltabdesc()
{
	return m_pdxltabdesc;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTableDescr::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerTableDescr::StartElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const, // element_qname
	const Attributes& attrs
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenTableDescr), element_local_name))
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_parse_handler_mgr->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}
	
	// parse table name from attributes
	m_pdxltabdesc = CDXLOperatorFactory::Pdxltabdesc(m_parse_handler_mgr->Pmm(), attrs);
		
	// install column descriptor parsers
	CParseHandlerBase *pphColDescr = CParseHandlerFactory::Pph(m_memory_pool, CDXLTokens::XmlstrToken(EdxltokenColumns), m_parse_handler_mgr, this);
	m_parse_handler_mgr->ActivateParseHandler(pphColDescr);
	
	// store parse handler
	this->Append(pphColDescr);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTableDescr::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerTableDescr::EndElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const // element_qname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenTableDescr), element_local_name))
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_parse_handler_mgr->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}
	
	// construct node from the created child nodes
	
	GPOS_ASSERT(1 == this->Length());
	
	// assemble the properties container from the cost
	CParseHandlerColDescr *pphColDescr = dynamic_cast<CParseHandlerColDescr *>((*this)[0]);
	
	GPOS_ASSERT(NULL != pphColDescr->GetColumnDescrDXLArray());
	
	ColumnDescrDXLArray *pdrgpdxlcd = pphColDescr->GetColumnDescrDXLArray();
	pdrgpdxlcd->AddRef();
	m_pdxltabdesc->SetColumnDescriptors(pdrgpdxlcd);
			
	// deactivate handler
	m_parse_handler_mgr->DeactivateHandler();
}

// EOF

