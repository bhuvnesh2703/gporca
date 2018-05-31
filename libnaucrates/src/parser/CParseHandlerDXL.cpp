//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerDXL.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing a DXL document
//---------------------------------------------------------------------------

#include "gpos/task/CWorker.h"

#include "naucrates/dxl/parser/CParseHandlerDXL.h"
#include "naucrates/dxl/parser/CParseHandlerMetadata.h"
#include "naucrates/dxl/parser/CParseHandlerMDRequest.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerPlan.h"
#include "naucrates/dxl/parser/CParseHandlerQuery.h"
#include "naucrates/dxl/parser/CParseHandlerScalarExpr.h"
#include "naucrates/dxl/parser/CParseHandlerStatistics.h"
#include "naucrates/dxl/parser/CParseHandlerOptimizerConfig.h"
#include "naucrates/dxl/parser/CParseHandlerTraceFlags.h"
#include "naucrates/dxl/parser/CParseHandlerSearchStrategy.h"
#include "naucrates/dxl/parser/CParseHandlerCostParams.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

#include "gpopt/optimizer/COptimizerConfig.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::CParseHandlerDXL
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerDXL::CParseHandlerDXL
	(
	IMemoryPool *memory_pool,
	CParseHandlerManager *parse_handler_mgr
	)
	:
	CParseHandlerBase(memory_pool, parse_handler_mgr, NULL),
	m_pbs(NULL),
	m_optimizer_config(NULL),
	m_mdrequest(NULL),
	m_query_dxl_root(NULL),
	m_output_colums_dxl_array(NULL),
	m_cte_producer_dxl_array(NULL),
	m_plan_dxl_root(NULL),
	m_mdobject_array(NULL),
	m_pdrgpmdid(NULL),
	m_scalar_expr_dxl(NULL),
	m_system_id_array(NULL),
	m_stats_derived_rel_dxl_array(NULL),
	m_search_stage_array(NULL),
	m_plan_id(ULLONG_MAX),
	m_plan_space_size(ULLONG_MAX),
	m_cost_model_params(NULL)
{}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::~CParseHandlerDXL
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerDXL::~CParseHandlerDXL()
{
	CRefCount::SafeRelease(m_pbs);
	CRefCount::SafeRelease(m_optimizer_config);
	CRefCount::SafeRelease(m_mdrequest);
	CRefCount::SafeRelease(m_query_dxl_root);
	CRefCount::SafeRelease(m_output_colums_dxl_array);
	CRefCount::SafeRelease(m_cte_producer_dxl_array);
	CRefCount::SafeRelease(m_plan_dxl_root);
	CRefCount::SafeRelease(m_mdobject_array);
	CRefCount::SafeRelease(m_pdrgpmdid);
	CRefCount::SafeRelease(m_scalar_expr_dxl);
	CRefCount::SafeRelease(m_system_id_array);
	CRefCount::SafeRelease(m_stats_derived_rel_dxl_array);
	CRefCount::SafeRelease(m_search_stage_array);
	CRefCount::SafeRelease(m_cost_model_params);

}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::Pbs
//
//	@doc:
//		Returns the bitset of traceflags
//
//---------------------------------------------------------------------------
CBitSet *
CParseHandlerDXL::Pbs() const
{
	return m_pbs;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::Poc
//
//	@doc:
//		Returns the optimizer config object
//
//---------------------------------------------------------------------------
COptimizerConfig *
CParseHandlerDXL::GetOptimizerConfig() const
{
	return m_optimizer_config;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::GetQueryDXLRoot
//
//	@doc:
//		Returns the root of the DXL query constructed by this parser
//
//---------------------------------------------------------------------------
CDXLNode *
CParseHandlerDXL::GetQueryDXLRoot() const
{
	return m_query_dxl_root;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::GetOutputColumnsDXLArray
//
//	@doc:
//		Returns the list of query output objects constructed by the parser
//
//---------------------------------------------------------------------------
DrgPdxln *
CParseHandlerDXL::GetOutputColumnsDXLArray() const
{
	return m_output_colums_dxl_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::GetCTEProducerDXLArray
//
//	@doc:
//		Returns the list of CTE producers
//
//---------------------------------------------------------------------------
DrgPdxln *
CParseHandlerDXL::GetCTEProducerDXLArray() const
{
	return m_cte_producer_dxl_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::GetPlanDXLRoot
//
//	@doc:
//		Returns the root of the DXL plan constructed by this parser
//
//---------------------------------------------------------------------------
CDXLNode *
CParseHandlerDXL::GetPlanDXLRoot() const
{
	return m_plan_dxl_root;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::Pdrgpmdobj
//
//	@doc:
//		Returns the list of metadata objects constructed by the parser
//
//---------------------------------------------------------------------------
DrgPimdobj *
CParseHandlerDXL::Pdrgpmdobj() const
{
	return m_mdobject_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::GetMdIdArray
//
//	@doc:
//		Returns the list of metadata ids constructed by the parser
//
//---------------------------------------------------------------------------
DrgPmdid *
CParseHandlerDXL::GetMdIdArray() const
{
	return m_pdrgpmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::GetMiniDumper
//
//	@doc:
//		Return the md request
//
//---------------------------------------------------------------------------
CMDRequest *
CParseHandlerDXL::GetMiniDumper() const
{
	return m_mdrequest;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::GetScalarExprDXLRoot
//
//	@doc:
//		Returns the DXL node representing the parsed scalar expression
//
//---------------------------------------------------------------------------
CDXLNode *
CParseHandlerDXL::GetScalarExprDXLRoot() const
{
	return m_scalar_expr_dxl;
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::GetSystemIdArray
//
//	@doc:
//		Returns the list of source system ids for the metadata 
//
//---------------------------------------------------------------------------
DrgPsysid *
CParseHandlerDXL::GetSystemIdArray() const
{
	return m_system_id_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::GetStatsDerivedRelDXLArray
//
//	@doc:
//		Returns the list of statistics objects constructed by the parser
//
//---------------------------------------------------------------------------
DrgPdxlstatsderrel *
CParseHandlerDXL::GetStatsDerivedRelDXLArray() const
{
	return m_stats_derived_rel_dxl_array;
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::GetSearchStageArray
//
//	@doc:
//		Returns search strategy
//
//---------------------------------------------------------------------------
DrgPss *
CParseHandlerDXL::GetSearchStageArray() const
{
	return m_search_stage_array;
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::GetPlanId
//
//	@doc:
//		Returns plan id
//
//---------------------------------------------------------------------------
ULLONG
CParseHandlerDXL::GetPlanId() const
{
	return m_plan_id;
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::GetPlanId
//
//	@doc:
//		Returns plan space size
//
//---------------------------------------------------------------------------
ULLONG
CParseHandlerDXL::GetPlanSpaceSize() const
{
	return m_plan_space_size;
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::GetCostModelParams
//
//	@doc:
//		Returns cost params
//
//---------------------------------------------------------------------------
ICostModelParams *
CParseHandlerDXL::GetCostModelParams() const
{
	return m_cost_model_params;
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::IsValidStartElement
//
//	@doc:
//		Return true if given element name is valid to start DXL document
//
//---------------------------------------------------------------------------
BOOL
CParseHandlerDXL::IsValidStartElement
	(
	const XMLCh* const xmlszName
	)
{
	// names of valid start elements of DXL document
	const XMLCh *xmlstrValidStartElement [] =
		{
		CDXLTokens::XmlstrToken(EdxltokenTraceFlags),
		CDXLTokens::XmlstrToken(EdxltokenOptimizerConfig),
		CDXLTokens::XmlstrToken(EdxltokenPlan),
		CDXLTokens::XmlstrToken(EdxltokenQuery),
		CDXLTokens::XmlstrToken(EdxltokenMetadata),
		CDXLTokens::XmlstrToken(EdxltokenMDRequest),
		CDXLTokens::XmlstrToken(EdxltokenStatistics),
		CDXLTokens::XmlstrToken(EdxltokenStackTrace),
		CDXLTokens::XmlstrToken(EdxltokenSearchStrategy),
		CDXLTokens::XmlstrToken(EdxltokenCostParams),
		CDXLTokens::XmlstrToken(EdxltokenScalarExpr),
		};

	BOOL fValidStartElement = false;
	for (ULONG ul = 0; !fValidStartElement && ul < GPOS_ARRAY_SIZE(xmlstrValidStartElement); ul++)
	{
		fValidStartElement = (0 == XMLString::compareString(xmlszName, xmlstrValidStartElement[ul]));
	}

	return fValidStartElement;
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::StartElement
	(
	const XMLCh* const element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const element_qname,
	const Attributes& attrs
	)
{		
	// reset time slice counter to ignore time taken by Xerces XSD grammar loader (OPT-491)
#ifdef GPOS_DEBUG
    CWorker::Self()->ResetTimeSlice();
#endif // GPOS_DEBUG
	
	if (0 == XMLString::compareString(element_local_name, CDXLTokens::XmlstrToken(EdxltokenDXLMessage)) ||
		0 == XMLString::compareString(element_local_name, CDXLTokens::XmlstrToken(EdxltokenThread)) ||
		0 == XMLString::compareString(element_local_name, CDXLTokens::XmlstrToken(EdxltokenComment)))
	{
		// beginning of DXL document or a new thread info
		;
	}
	else
	{
		GPOS_ASSERT(IsValidStartElement(element_local_name));

		// install a parse handler for the given element
		CParseHandlerBase *pph = CParseHandlerFactory::Pph(m_memory_pool, element_local_name, m_parse_handler_mgr, this);
	
		m_parse_handler_mgr->ActivateParseHandler(pph);
			
		// store parse handler
		this->Append(pph);
		
		pph->startElement(element_uri, element_local_name, element_qname, attrs);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::EndElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const, // element_local_name,
	const XMLCh* const // element_qname
	)
{
	// ignore
}

void
CParseHandlerDXL::ProcessDocumentEnd()
{
	// retrieve plan and/or query and/or list of metadata objects from child parse handler
	for (ULONG ul = 0; ul < this->Length(); ul++)
	{
		CParseHandlerBase *pph = (*this)[ul];

		EDxlParseHandlerType edxlphtype = pph->GetParseHandlerType();

		// find parse handler for the current type
		Pfparse pf = FindParseHandler(edxlphtype);

		if (NULL != pf)
		{
			(this->*pf)(pph);
		}
	}
	
	m_parse_handler_mgr->DeactivateHandler();
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::FindParseHandler
//
//	@doc:
//		Find the parse handler function for the given type
//
//---------------------------------------------------------------------------
Pfparse
CParseHandlerDXL::FindParseHandler
	(
	EDxlParseHandlerType edxlphtype
	)
{
	SParseElem rgParseHandlers[] =
	{
		{EdxlphTraceFlags, &CParseHandlerDXL::ExtractTraceFlags},
		{EdxlphOptConfig, &CParseHandlerDXL::ExtractOptimizerConfig},
		{EdxlphPlan, &CParseHandlerDXL::ExtractDXLPlan},
		{EdxlphMetadata, &CParseHandlerDXL::ExtractMetadataObjects},
		{EdxlphStatistics, &CParseHandlerDXL::ExtractStats},
		{EdxlphQuery, &CParseHandlerDXL::ExtractDXLQuery},
		{EdxlphMetadataRequest, &CParseHandlerDXL::ExtractMDRequest},
		{EdxlphSearchStrategy, &CParseHandlerDXL::ExtractSearchStrategy},
		{EdxlphCostParams, &CParseHandlerDXL::ExtractCostParams},
		{EdxlphScalarExpr, &CParseHandlerDXL::ExtractScalarExpr},
	};

	const ULONG ulParseHandlers = GPOS_ARRAY_SIZE(rgParseHandlers);
	Pfparse pf = NULL;
	for (ULONG ul = 0; ul < ulParseHandlers; ul++)
	{
		SParseElem elem = rgParseHandlers[ul];
		if (edxlphtype == elem.edxlphtype)
		{
			pf = elem.pf;
			break;
		}
	}

	return pf;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::ExtractTraceFlags
//
//	@doc:
//		Extract traceflags
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::ExtractTraceFlags
	(
	CParseHandlerBase *pph
	)
{
	CParseHandlerTraceFlags *pphtf = (CParseHandlerTraceFlags *) pph;
	GPOS_ASSERT(NULL != pphtf);

	GPOS_ASSERT (NULL == m_pbs && "Traceflags already set");
	
	m_pbs = pphtf->Pbs();
	m_pbs->AddRef();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::ExtractOptimizerConfig
//
//	@doc:
//		Extract optimizer config
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::ExtractOptimizerConfig
	(
	CParseHandlerBase *pph
	)
{
	CParseHandlerOptimizerConfig *pphOptConfig = (CParseHandlerOptimizerConfig *) pph;
	GPOS_ASSERT(NULL != pphOptConfig);

	GPOS_ASSERT (NULL == m_pbs && "Traceflags already set");

	m_pbs = pphOptConfig->Pbs();
	m_pbs->AddRef();
	
	GPOS_ASSERT (NULL == m_optimizer_config && "Optimizer configuration already set");

	m_optimizer_config = pphOptConfig->GetOptimizerConfig();
	m_optimizer_config->AddRef();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::ExtractDXLPlan
//
//	@doc:
//		Extract a physical plan
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::ExtractDXLPlan
	(
	CParseHandlerBase *pph
	)
{
	CParseHandlerPlan *pphPlan = (CParseHandlerPlan *) pph;
	GPOS_ASSERT(NULL != pphPlan && NULL != pphPlan->Pdxln());

	m_plan_dxl_root = pphPlan->Pdxln();
	m_plan_dxl_root->AddRef();

	m_plan_id = pphPlan->UllId();
	m_plan_space_size = pphPlan->UllSpaceSize();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::ExtractMetadataObjects
//
//	@doc:
//		Extract metadata objects
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::ExtractMetadataObjects
	(
	CParseHandlerBase *pph
	)
{
	CParseHandlerMetadata *pphmd = dynamic_cast<CParseHandlerMetadata *>(pph);
	GPOS_ASSERT(NULL != pphmd && NULL != pphmd->Pdrgpmdobj());

	m_mdobject_array = pphmd->Pdrgpmdobj();
	m_mdobject_array->AddRef();
	
	m_pdrgpmdid = pphmd->GetMdIdArray();
	m_pdrgpmdid->AddRef();

	m_system_id_array = pphmd->GetSystemIdArray();

	if (NULL != m_system_id_array)
	{
		m_system_id_array->AddRef();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::ExtractStats
//
//	@doc:
//		Extract statistics
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::ExtractStats
	(
	CParseHandlerBase *pph
	)
{
	CParseHandlerStatistics *pphStats = dynamic_cast<CParseHandlerStatistics *>(pph);
	GPOS_ASSERT(NULL != pphStats);

	DrgPdxlstatsderrel *dxl_derived_rel_stats_array = pphStats->GetStatsDerivedRelDXLArray();
	GPOS_ASSERT(NULL != dxl_derived_rel_stats_array);

	dxl_derived_rel_stats_array->AddRef();
	m_stats_derived_rel_dxl_array = dxl_derived_rel_stats_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::ExtractDXLQuery
//
//	@doc:
//		Extract DXL query
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::ExtractDXLQuery
	(
	CParseHandlerBase *pph
	)
{
	CParseHandlerQuery *pphquery = dynamic_cast<CParseHandlerQuery *>(pph);
	GPOS_ASSERT(NULL != pphquery && NULL != pphquery->Pdxln());

	m_query_dxl_root = pphquery->Pdxln();
	m_query_dxl_root->AddRef();

	GPOS_ASSERT(NULL != pphquery->GetOutputColumnsDXLArray());

	m_output_colums_dxl_array = pphquery->GetOutputColumnsDXLArray();
	m_output_colums_dxl_array->AddRef();
	
	m_cte_producer_dxl_array = pphquery->GetCTEProducerDXLArray();
	m_cte_producer_dxl_array->AddRef();
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::ExtractMDRequest
//
//	@doc:
//		Extract mdids
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::ExtractMDRequest
	(
	CParseHandlerBase *pph
	)
{
	CParseHandlerMDRequest *pphMDRequest = dynamic_cast<CParseHandlerMDRequest *>(pph);
	GPOS_ASSERT(NULL != pphMDRequest && NULL != pphMDRequest->GetMdIdArray());
	
	DrgPmdid *pdrgpmdid = pphMDRequest->GetMdIdArray();
	CMDRequest::DrgPtr *pdrgptr = pphMDRequest->Pdrgptr();
	
	pdrgpmdid->AddRef();
	pdrgptr->AddRef();
	
	m_mdrequest = GPOS_NEW(m_memory_pool) CMDRequest(m_memory_pool, pdrgpmdid, pdrgptr);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::ExtractSearchStrategy
//
//	@doc:
//		Extract search strategy
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::ExtractSearchStrategy
	(
	CParseHandlerBase *pph
	)
{
	CParseHandlerSearchStrategy *pphSearchStrategy = dynamic_cast<CParseHandlerSearchStrategy *>(pph);
	GPOS_ASSERT(NULL != pphSearchStrategy && NULL != pphSearchStrategy->Pdrgppss());

	DrgPss *search_stage_array = pphSearchStrategy->Pdrgppss();

	search_stage_array->AddRef();
	m_search_stage_array = search_stage_array;
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::ExtractCostParams
//
//	@doc:
//		Extract cost params
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::ExtractCostParams
	(
	CParseHandlerBase *pph
	)
{
	CParseHandlerCostParams *pphCostParams = dynamic_cast<CParseHandlerCostParams *>(pph);
	GPOS_ASSERT(NULL != pphCostParams && NULL != pphCostParams->GetCostModelParams());

	ICostModelParams *pcp = pphCostParams->GetCostModelParams();

	pcp->AddRef();
	m_cost_model_params = pcp;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerDXL::ExtractScalarExpr
//
//	@doc:
//		Extract scalar expressions
//
//---------------------------------------------------------------------------
void
CParseHandlerDXL::ExtractScalarExpr
	(
	CParseHandlerBase *pph
	)
{
	CParseHandlerScalarExpr *pphScalarExpr = dynamic_cast<CParseHandlerScalarExpr *>(pph);
	GPOS_ASSERT(NULL != pphScalarExpr && NULL != pphScalarExpr->Pdxln());

	m_scalar_expr_dxl = pphScalarExpr->Pdxln();
	m_scalar_expr_dxl->AddRef();
}

// EOF
