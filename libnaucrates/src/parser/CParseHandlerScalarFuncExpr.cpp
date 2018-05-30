//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarFuncExpr.cpp
//
//	@doc:
//		
//		Implementation of the SAX parse handler class for parsing scalar FuncExpr.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

#include "naucrates/dxl/parser/CParseHandlerScalarFuncExpr.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarFuncExpr::CParseHandlerScalarFuncExpr
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerScalarFuncExpr::CParseHandlerScalarFuncExpr
	(
	IMemoryPool *pmp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerScalarOp(pmp, parse_handler_mgr, pphRoot),
	m_fInsideFuncExpr(false)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarFuncExpr::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarFuncExpr::StartElement
	(
	const XMLCh* const element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const element_qname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarFuncExpr), element_local_name))
	{
		if(!m_fInsideFuncExpr)
		{
			// parse and create scalar FuncExpr
			CDXLScalarFuncExpr *pdxlop = (CDXLScalarFuncExpr*) CDXLOperatorFactory::PdxlopFuncExpr(m_pphm->Pmm(), attrs);

			// construct node from the created scalar FuncExpr
			m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);

			m_fInsideFuncExpr = true;
		}
		else
		{
			// This is to support nested FuncExpr
			CParseHandlerBase *pphFunc = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarFuncExpr), m_pphm, this);
			m_pphm->ActivateParseHandler(pphFunc);

			// store parse handlers
			this->Append(pphFunc);

			pphFunc->startElement(element_uri, element_local_name, element_qname, attrs);
		}
	}
	else
	{
		GPOS_ASSERT(m_fInsideFuncExpr);

		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
		m_pphm->ActivateParseHandler(pphChild);

		// store parse handlers
		this->Append(pphChild);

		pphChild->startElement(element_uri, element_local_name, element_qname, attrs);

	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarFuncExpr::EndElement
//
//	@doc:
//		Processes a Xerces end element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarFuncExpr::EndElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const // element_qname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarFuncExpr), element_local_name))
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}

	const ULONG ulSize = this->Length();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CParseHandlerScalarOp *pphChild = dynamic_cast<CParseHandlerScalarOp *>((*this)[ul]);
		AddChildFromParseHandler(pphChild);
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
