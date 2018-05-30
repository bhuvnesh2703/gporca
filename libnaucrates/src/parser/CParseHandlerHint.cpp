//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.
//
//	@filename:
//		CParseHandlerHint.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing hint
//		configuration
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerHint.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

#include "naucrates/dxl/xml/dxltokens.h"

#include "gpopt/engine/CHint.h"

using namespace gpdxl;
using namespace gpopt;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerHint::CParseHandlerHint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerHint::CParseHandlerHint
	(
	IMemoryPool *memory_pool,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(memory_pool, parse_handler_mgr, pphRoot),
	m_phint(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerHint::~CParseHandlerHint
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerHint::~CParseHandlerHint()
{
	CRefCount::SafeRelease(m_phint);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerHint::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerHint::StartElement
	(
	const XMLCh* const , //element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const , //element_qname,
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenHint), element_local_name))
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}

	// parse hint configuration options
	ULONG ulMinNumOfPartsToRequireSortOnInsert = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenMinNumOfPartsToRequireSortOnInsert, EdxltokenHint);
	ULONG ulJoinArityForAssociativityCommutativity = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenJoinArityForAssociativityCommutativity, EdxltokenHint, true, INT_MAX);
	ULONG ulArrayExpansionThreshold = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenArrayExpansionThreshold, EdxltokenHint, true, INT_MAX);
	ULONG ulJoinOrderDPThreshold = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenJoinOrderDPThreshold, EdxltokenHint, true, JOIN_ORDER_DP_THRESHOLD);
	ULONG ulBroadcastThreshold = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenBroadcastThreshold, EdxltokenHint, true, BROADCAST_THRESHOLD);
	ULONG fEnforceConstraintsOnDML = CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenEnforceConstraintsOnDML, EdxltokenHint, true, true);

	m_phint = GPOS_NEW(m_memory_pool) CHint
								(
								ulMinNumOfPartsToRequireSortOnInsert,
								ulJoinArityForAssociativityCommutativity,
								ulArrayExpansionThreshold,
								ulJoinOrderDPThreshold,
								ulBroadcastThreshold,
								fEnforceConstraintsOnDML
								);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerHint::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerHint::EndElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const // element_qname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenHint), element_local_name))
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE( gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}

	GPOS_ASSERT(NULL != m_phint);
	GPOS_ASSERT(0 == this->Length());

	// deactivate handler
	m_pphm->DeactivateHandler();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerHint::Edxlphtype
//
//	@doc:
//		Return the type of the parse handler.
//
//---------------------------------------------------------------------------
EDxlParseHandlerType
CParseHandlerHint::Edxlphtype() const
{
	return EdxlphHint;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerHint::Phint
//
//	@doc:
//		Returns the hint configuration
//
//---------------------------------------------------------------------------
CHint *
CParseHandlerHint::Phint() const
{
	return m_phint;
}

// EOF
