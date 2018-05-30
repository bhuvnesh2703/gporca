//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerPhysicalTVF.cpp
//
//	@doc:
//
//		Implementation of the SAX parse handler class for parsing table-valued
//		functions
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalTVF.h"
#include "naucrates/dxl/parser/CParseHandlerProjList.h"
#include "naucrates/dxl/parser/CParseHandlerProperties.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalTVF::CParseHandlerPhysicalTVF
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerPhysicalTVF::CParseHandlerPhysicalTVF
	(
	IMemoryPool *pmp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerPhysicalOp(pmp, parse_handler_mgr, pphRoot),
	m_pmdidFunc(NULL),
	m_pmdidRetType(NULL),
	m_pstr(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalTVF::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalTVF::StartElement
	(
	const XMLCh* const element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const element_qname,
	const Attributes &attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalTVF), element_local_name))
	{
		// parse function id
		m_pmdidFunc = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenFuncId, EdxltokenPhysicalTVF);

		// parse function name
		const XMLCh *xmlszFuncName = CDXLOperatorFactory::XmlstrFromAttrs
																(
																attrs,
																EdxltokenName,
																EdxltokenPhysicalTVF
																);

		CWStringDynamic *pstrFuncName = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), xmlszFuncName);
		m_pstr = GPOS_NEW(m_pmp) CWStringConst(m_pmp, pstrFuncName->GetBuffer());
		GPOS_DELETE(pstrFuncName);

		// parse return type
		m_pmdidRetType = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenTypeId, EdxltokenPhysicalTVF);

		// parse handler for the proj list
		CParseHandlerBase *pphPrL = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarProjList), m_pphm, this);
		m_pphm->ActivateParseHandler(pphPrL);

		//parse handler for the properties of the operator
		CParseHandlerBase *pphProp = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenProperties), m_pphm, this);
		m_pphm->ActivateParseHandler(pphProp);

		// store parse handlers
		this->Append(pphProp);
		this->Append(pphPrL);
	}
	else
	{
		// parse scalar child
		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
		m_pphm->ActivateParseHandler(pphChild);

		// store parse handler
		this->Append(pphChild);

		pphChild->startElement(element_uri, element_local_name, element_qname, attrs);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalTVF::EndElement
//
//	@doc:
//		Processes a Xerces end element event
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalTVF::EndElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const // element_qname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalTVF), element_local_name))
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}

	CDXLPhysicalTVF *pdxlop = GPOS_NEW(m_pmp) CDXLPhysicalTVF(m_pmp, m_pmdidFunc, m_pmdidRetType, m_pstr);
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);

	CParseHandlerProperties *pphProp = dynamic_cast<CParseHandlerProperties *>((*this)[0]);

	// set statistics and physical properties
	CParseHandlerUtils::SetProperties(m_pdxln, pphProp);

	CParseHandlerProjList *pphPrL = dynamic_cast<CParseHandlerProjList*>((*this)[1]);
	AddChildFromParseHandler(pphPrL);

	const ULONG ulSize = this->Length();
	for (ULONG ul = 2; ul < ulSize; ul++)
	{
		CParseHandlerScalarOp *pphChild = dynamic_cast<CParseHandlerScalarOp *>((*this)[ul]);
		AddChildFromParseHandler(pphChild);
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF

