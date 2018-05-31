//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CParseHandlerScalarArrayRefIndexList.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing arrayref indexes
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarArrayRefIndexList.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/operators/CDXLScalarArrayRefIndexList.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarArrayRefIndexList::CParseHandlerScalarArrayRefIndexList
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerScalarArrayRefIndexList::CParseHandlerScalarArrayRefIndexList
	(
	IMemoryPool *memory_pool,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
	:
	CParseHandlerScalarOp(memory_pool, parse_handler_mgr, parse_handler_root)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarArrayRefIndexList::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarArrayRefIndexList::StartElement
	(
	const XMLCh* const element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const element_qname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarArrayRefIndexList), element_local_name))
	{
		// start the index list
		const XMLCh *xmlszOpType = CDXLOperatorFactory::XmlstrFromAttrs
															(
															attrs,
															EdxltokenScalarArrayRefIndexListBound,
															EdxltokenScalarArrayRefIndexList
															);

		CDXLScalarArrayRefIndexList::EIndexListBound eilb = CDXLScalarArrayRefIndexList::EilbUpper;
		if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarArrayRefIndexListLower), xmlszOpType))
		{
			eilb = CDXLScalarArrayRefIndexList::EilbLower;
		}
		else if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarArrayRefIndexListUpper), xmlszOpType))
		{
			GPOS_RAISE
				(
				gpdxl::ExmaDXL,
				gpdxl::ExmiDXLInvalidAttributeValue,
				CDXLTokens::PstrToken(EdxltokenScalarArrayRefIndexListBound)->GetBuffer(),
				CDXLTokens::PstrToken(EdxltokenScalarArrayRefIndexList)->GetBuffer()
				);
		}

		m_pdxln = GPOS_NEW(m_memory_pool) CDXLNode (m_memory_pool, GPOS_NEW(m_memory_pool) CDXLScalarArrayRefIndexList(m_memory_pool, eilb));
	}
	else
	{
		// we must have seen a index list already and initialized the index list node
		GPOS_ASSERT(NULL != m_pdxln);

		// parse scalar child
		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_memory_pool, CDXLTokens::XmlstrToken(EdxltokenScalar), m_parse_handler_mgr, this);
		m_parse_handler_mgr->ActivateParseHandler(pphChild);

		// store parse handler
		this->Append(pphChild);

		pphChild->startElement(element_uri, element_local_name, element_qname, attrs);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarArrayRefIndexList::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarArrayRefIndexList::EndElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const // element_qname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarArrayRefIndexList), element_local_name))
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_parse_handler_mgr->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}

	// add constructed children from child parse handlers
	const ULONG ulSize = this->Length();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CParseHandlerScalarOp *pphChild = dynamic_cast<CParseHandlerScalarOp *>((*this)[ul]);
		AddChildFromParseHandler(pphChild);
	}

	// deactivate handler
	m_parse_handler_mgr->DeactivateHandler();
}

// EOF
