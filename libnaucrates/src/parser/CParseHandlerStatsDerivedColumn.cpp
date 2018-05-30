//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerStatsDerivedColumn.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing statistics of
//		derived column.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerStatsDerivedColumn.h"
#include "naucrates/dxl/parser/CParseHandlerColStatsBucket.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;
using namespace gpnaucrates;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatsDerivedColumn::CParseHandlerStatsDerivedColumn
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerStatsDerivedColumn::CParseHandlerStatsDerivedColumn
	(
	IMemoryPool *memory_pool,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(memory_pool, parse_handler_mgr, pphRoot),
	m_ulColId(0),
	m_dWidth(CStatistics::DDefaultColumnWidth),
	m_dNullFreq(0.0),
	m_dDistinctRemain(0.0),
	m_dFreqRemain(0.0),
	m_pstatsdercol(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatsDerivedColumn::~CParseHandlerStatsDerivedColumn
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerStatsDerivedColumn::~CParseHandlerStatsDerivedColumn()
{
	m_pstatsdercol->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatsDerivedColumn::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerStatsDerivedColumn::StartElement
	(
	const XMLCh* const element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const element_qname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenStatsDerivedColumn), element_local_name))
	{
		// must have not seen a bucket yet
		GPOS_ASSERT(0 == this->Length());

		// parse column id
		m_ulColId = CDXLOperatorFactory::UlValueFromAttrs
											(
											m_pphm->Pmm(),
											attrs,
											EdxltokenColId,
											EdxltokenStatsDerivedColumn
											);

		// parse column width
		m_dWidth = CDXLOperatorFactory::DValueFromAttrs
											(
											m_pphm->Pmm(),
											attrs,
											EdxltokenWidth,
											EdxltokenStatsDerivedColumn
											);

		const XMLCh *parsed_null_freq = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenColNullFreq));
		if (NULL != parsed_null_freq)
		{
			m_dNullFreq = CDXLOperatorFactory::DValueFromXmlstr(m_pphm->Pmm(), parsed_null_freq, EdxltokenColNullFreq, EdxltokenColumnStats);
		}

		const XMLCh *parsed_distinct_remaining = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenColNdvRemain));
		if (NULL != parsed_distinct_remaining)
		{
			m_dDistinctRemain = CDXLOperatorFactory::DValueFromXmlstr(m_pphm->Pmm(), parsed_distinct_remaining, EdxltokenColNdvRemain, EdxltokenColumnStats);
		}

		const XMLCh *parsed_freq_remaining = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenColFreqRemain));
		if (NULL != parsed_freq_remaining)
		{
			m_dFreqRemain = CDXLOperatorFactory::DValueFromXmlstr(m_pphm->Pmm(), parsed_freq_remaining, EdxltokenColFreqRemain, EdxltokenColumnStats);
		}
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenColumnStatsBucket), element_local_name))
	{
		// install a parse handler for the given element
		CParseHandlerBase *pph = CParseHandlerFactory::Pph(m_memory_pool, CDXLTokens::XmlstrToken(EdxltokenColumnStatsBucket), m_pphm, this);
		m_pphm->ActivateParseHandler(pph);

		// store parse handler
		this->Append(pph);

		pph->startElement(element_uri, element_local_name, element_qname, attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerStatsDerivedColumn::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerStatsDerivedColumn::EndElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const // element_qname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenStatsDerivedColumn), element_local_name))
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}

	DrgPdxlbucket *stats_bucket_dxl_array = GPOS_NEW(m_memory_pool) DrgPdxlbucket(m_memory_pool);

	const ULONG ulBuckets = this->Length();
	// add constructed children from child parse handlers
	for (ULONG ul = 0; ul < ulBuckets; ul++)
	{
		CParseHandlerColStatsBucket *pph = dynamic_cast<CParseHandlerColStatsBucket*>((*this)[ul]);
		CDXLBucket *pdxlbucket = pph->Pdxlbucket();
		pdxlbucket->AddRef();
		stats_bucket_dxl_array->Append(pdxlbucket);
	}

	m_pstatsdercol = GPOS_NEW(m_memory_pool) CDXLStatsDerivedColumn(m_ulColId, m_dWidth, m_dNullFreq, m_dDistinctRemain, m_dFreqRemain, stats_bucket_dxl_array);

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
