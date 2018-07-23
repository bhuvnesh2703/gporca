//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal Software, Inc.
//
//	@filename:
//		CParseHandlerNLJIndexParamList.cpp
//
//	@doc:
//		
//		Implementation of the SAX parse handler class for parsing the ParamList
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

#include "naucrates/dxl/parser/CParseHandlerNLJIndexParamList.h"
#include "naucrates/dxl/parser/CParseHandlerNLJIndexParam.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerNLJIndexParamList::CParseHandlerNLJIndexParamList
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerNLJIndexParamList::CParseHandlerNLJIndexParamList
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
	:
	CParseHandlerBase(mp, parse_handler_mgr, parse_handler_root)
{
	m_nest_params_colrefs_array = GPOS_NEW(mp) DrgPdxlcr(mp);
	m_is_param_list = false;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerNLJIndexParamList::~CParseHandlerNLJIndexParamList
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerNLJIndexParamList::~CParseHandlerNLJIndexParamList()
{
	m_nest_params_colrefs_array->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerNLJIndexParamList::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerNLJIndexParamList::StartElement
	(
	const XMLCh* const element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const element_qname,
	const Attributes &attrs
	)
{
	if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenNLJIndexParamList), element_local_name))
	{
		// we can't have seen a paramlist already
		GPOS_ASSERT(!m_is_param_list);
		// start the paramlist
		m_is_param_list = true;
	}
	else if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenNLJIndexParam), element_local_name))
	{
		// we must have seen a paramlist already
		GPOS_ASSERT(m_is_param_list);

		// start new param
		CParseHandlerBase *nest_param_parse_handler = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenNLJIndexParam), m_pphm, this);
		m_pphm->ActivateParseHandler(nest_param_parse_handler);

		// store parse handler
		this->Append(nest_param_parse_handler);

		nest_param_parse_handler->startElement(element_uri, element_local_name, element_qname, attrs);
	}
	else
	{
		CWStringDynamic *str = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, str->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerNLJIndexParamList::EndElement
//
//	@doc:
//		Processes a Xerces end element event
//
//---------------------------------------------------------------------------
void
CParseHandlerNLJIndexParamList::EndElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const // element_qname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenNLJIndexParamList), element_local_name))
	{
		CWStringDynamic *str = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, str->Wsz());
	}

	const ULONG size = this->UlLength();
	// add constructed children from child parse handlers
	for (ULONG idx = 0; idx < size; idx++)
	{
		CParseHandlerNLJIndexParam *nest_param_parse_handler = dynamic_cast<CParseHandlerNLJIndexParam *>((*this)[idx]);

		CDXLColRef *nest_param_colref_dxl = nest_param_parse_handler->GetNestParamColRefDxl();
		nest_param_colref_dxl->AddRef();
		m_nest_params_colrefs_array->Append(nest_param_colref_dxl);
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
