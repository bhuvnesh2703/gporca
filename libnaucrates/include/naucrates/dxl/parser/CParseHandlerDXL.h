//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerDXL.h
//
//	@doc:
//		SAX parse handler class for parsing a DXL document
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerDXL_H
#define GPDXL_CParseHandlerDXL_H

#include "gpos/base.h"
#include "gpos/common/CBitSet.h"

#include "gpopt/cost/ICostModelParams.h"
#include "gpopt/search/CSearchStage.h"

#include "naucrates/dxl/parser/CParseHandlerBase.h"

#include "naucrates/md/CDXLStatsDerivedRelation.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	XERCES_CPP_NAMESPACE_USE
	
	// shorthand for functions for translating GPDB expressions into DXL nodes
	typedef void (CParseHandlerDXL::*Pfparse)(CParseHandlerBase *parse_handler_base);

	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerDXL
	//
	//	@doc:
	//		Parse handler for DXL documents.
	//		Starting point for all other parse handlers
	//
	//---------------------------------------------------------------------------
	class CParseHandlerDXL : public CParseHandlerBase
	{
		private:

			// pair of parse handler type and parse handler function
			struct SParseElem
			{
				EDxlParseHandlerType edxlphtype; // parse handler type
				Pfparse pf; // pointer to corresponding function
			};

			// traceflags
			CBitSet *m_pbs;
			
			// optimizer config
			COptimizerConfig *m_optimizer_config;
			
			// MD request
			CMDRequest *m_mdrequest;
			
			// the root of the parsed DXL query
			CDXLNode *m_query_dxl_root;
			
			// list of query output columns
			DrgPdxln *m_output_colums_dxl_array;

			// list of CTE producers
			DrgPdxln *m_cte_producer_dxl_array;

			// the root of the parsed DXL plan
			CDXLNode *m_plan_dxl_root;

			// list of parsed metadata objects
			DrgPimdobj *m_mdobject_array;
			
			// list of parsed metadata ids
			DrgPmdid *m_pdrgpmdid;

			// the root of the parsed scalar expression
			CDXLNode *m_scalar_expr_dxl;

			// list of source system ids
			DrgPsysid *m_system_id_array;
			
			// list of parsed statistics objects
			DrgPdxlstatsderrel *m_stats_derived_rel_dxl_array;

			// search strategy
			DrgPss *m_search_stage_array;

			// plan Id
			ULLONG m_plan_id;

			// plan space size
			ULLONG m_plan_space_size;

			// cost model params
			ICostModelParams *m_cost_model_params;

			// private copy ctor
			CParseHandlerDXL(const CParseHandlerDXL&);
			
			// process the start of an element
			void StartElement
				(
					const XMLCh* const element_uri, 		// URI of element's namespace
 					const XMLCh* const element_local_name,	// local part of element's name
					const XMLCh* const element_qname,		// element's qname
					const Attributes& attr				// element's attributes
				);
				
			// process the end of an element
			void EndElement
				(
					const XMLCh* const element_uri, 		// URI of element's namespace
					const XMLCh* const element_local_name,	// local part of element's name
					const XMLCh* const element_qname		// element's qname
				);
			
			// find the parse handler function for the given type
			Pfparse FindParseHandler(EDxlParseHandlerType edxlphtype);

			// extract traceflags
			void ExtractTraceFlags(CParseHandlerBase *pph);
			
			// extract optimizer config
			void ExtractOptimizerConfig(CParseHandlerBase *pph);

			// extract a physical plan
			void ExtractDXLPlan(CParseHandlerBase *pph);

			// extract metadata objects
			void ExtractMetadataObjects(CParseHandlerBase *pph);

			// extract statistics
			void ExtractStats(CParseHandlerBase *pph);

			// extract DXL query
			void ExtractDXLQuery(CParseHandlerBase *pph);

			// extract mdids of requested objects
			void ExtractMDRequest(CParseHandlerBase *pph);
			
			// extract search strategy
			void ExtractSearchStrategy(CParseHandlerBase *pph);

			// extract cost params
			void ExtractCostParams(CParseHandlerBase *pph);

            // extract a top level scalar expression
			void ExtractScalarExpr(CParseHandlerBase *pph);

			// check if given element name is valid for starting DXL document
			static
			BOOL IsValidStartElement(const XMLCh* const xmlszName);

		public:
			// ctor
			CParseHandlerDXL(IMemoryPool *memory_pool, CParseHandlerManager *parse_handler_mgr);
			
			//dtor
			virtual
			~CParseHandlerDXL();
			
			// traceflag bitset
			CBitSet *Pbs() const;
			
			// optimizer config
			COptimizerConfig *GetOptimizerConfig() const;
			
			// returns the root of the parsed DXL query
			CDXLNode *GetQueryDXLRoot() const;
			
			// returns the list of query output columns
			DrgPdxln *GetOutputColumnsDXLArray() const;

			// returns the list of CTE producers
			DrgPdxln *GetCTEProducerDXLArray() const;

			// returns the root of the parsed DXL plan
			CDXLNode *GetPlanDXLRoot() const;

			// return the list of parsed metadata objects
			DrgPimdobj *Pdrgpmdobj() const;

			// return the list of parsed metadata ids
			DrgPmdid *GetMdIdArray() const;

			// return the MD request object
			CMDRequest *GetMiniDumper() const;

			// return the root of the parsed scalar expression
			CDXLNode *GetScalarExprDXLRoot() const;

			// return the list of parsed source system id objects
			DrgPsysid *GetSystemIdArray() const;
			
			// return the list of statistics objects
			DrgPdxlstatsderrel *GetStatsDerivedRelDXLArray() const;

			// return search strategy
			DrgPss *GetSearchStageArray() const;

			// return plan id
			ULLONG GetPlanId() const;

			// return plan space size
			ULLONG GetPlanSpaceSize() const;

			// return cost params
			ICostModelParams *GetCostModelParams() const;

			// process the end of the document
			void ProcessDocumentEnd();
	};
}

#endif // !GPDXL_CParseHandlerDXL_H

// EOF
