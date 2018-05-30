//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CParseHandlerCtasStorageOptions.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing CTAS storage
//		options
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerCtasStorageOptions.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCtasStorageOptions::CParseHandlerCtasStorageOptions
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerCtasStorageOptions::CParseHandlerCtasStorageOptions
	(
	IMemoryPool *pmp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, parse_handler_mgr, pphRoot),
	m_pmdnameTablespace(NULL),
	m_pdxlctasopt(NULL),
	m_pdrgpctasopt(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCtasStorageOptions::~CParseHandlerCtasStorageOptions
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerCtasStorageOptions::~CParseHandlerCtasStorageOptions()
{
	CRefCount::SafeRelease(m_pdxlctasopt);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCtasStorageOptions::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerCtasStorageOptions::StartElement
	(
	const XMLCh* const , // element_uri
	const XMLCh* const element_local_name,
	const XMLCh* const , // element_qname
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCTASOptions), element_local_name))
	{
		const XMLCh *xmlszTablespace = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenTablespace));
		if (NULL != xmlszTablespace)
		{
			m_pmdnameTablespace = CDXLUtils::CreateMDNameFromXMLChar(m_pphm->Pmm(), xmlszTablespace);
		}
		
		m_ectascommit = CDXLOperatorFactory::EctascommitFromAttr(attrs);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCTASOption), element_local_name))
	{
		// parse option name and value
		ULONG ulType = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenCtasOptionType, EdxltokenCTASOption);
		CWStringBase *pstrName = CDXLOperatorFactory::PstrValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenName, EdxltokenCTASOption);
		CWStringBase *pstrValue = CDXLOperatorFactory::PstrValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenValue, EdxltokenCTASOption);
		BOOL fNull = CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenIsNull, EdxltokenCTASOption);
		
		if (NULL == m_pdrgpctasopt)
		{
			m_pdrgpctasopt = GPOS_NEW(m_memory_pool) CDXLCtasStorageOptions::DrgPctasOpt(m_memory_pool);
		}
		m_pdrgpctasopt->Append(
				GPOS_NEW(m_memory_pool) CDXLCtasStorageOptions::CDXLCtasOption(ulType, pstrName, pstrValue, fNull));
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCtasStorageOptions::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerCtasStorageOptions::EndElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const // element_qname
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCTASOptions), element_local_name))
	{
		m_pdxlctasopt = GPOS_NEW(m_memory_pool) CDXLCtasStorageOptions(m_pmdnameTablespace, m_ectascommit, m_pdrgpctasopt);
		// deactivate handler
		m_pphm->DeactivateHandler();
	}
	else if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCTASOption), element_local_name))
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerCtasStorageOptions::Pdxlctasopt
//
//	@doc:
//		Return parsed storage options
//
//---------------------------------------------------------------------------
CDXLCtasStorageOptions *
CParseHandlerCtasStorageOptions::Pdxlctasopt() const
{
	return m_pdxlctasopt;
}

// EOF
