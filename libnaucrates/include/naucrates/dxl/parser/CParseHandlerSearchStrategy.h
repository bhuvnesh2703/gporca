//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerSearchStrategy.h
//
//	@doc:
//		Parse handler for search strategy
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerSearchStrategy_H
#define GPDXL_CParseHandlerSearchStrategy_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"

#include "gpopt/search/CSearchStage.h"

namespace gpdxl
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerSearchStrategy
	//
	//	@doc:
	//		Parse handler for search strategy
	//
	//---------------------------------------------------------------------------
	class CParseHandlerSearchStrategy : public CParseHandlerBase
	{

		private:

			// search stages
			DrgPss *m_search_stage_array;

			// private ctor
			CParseHandlerSearchStrategy(const CParseHandlerSearchStrategy&);

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

		public:
			// ctor/dtor
			CParseHandlerSearchStrategy
				(
				IMemoryPool *memory_pool,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			virtual
			~CParseHandlerSearchStrategy();

			// returns the dxl representation of search stages
			DrgPss *Pdrgppss()
			{
				return m_search_stage_array;
			}

			EDxlParseHandlerType GetParseHandlerType() const
			{
				return EdxlphSearchStrategy;
			}

	};
}

#endif // !GPDXL_CParseHandlerSearchStrategy_H

// EOF
