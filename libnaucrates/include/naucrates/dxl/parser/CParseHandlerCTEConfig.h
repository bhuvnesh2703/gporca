//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CParseHandlerCTEConfig.h
//
//	@doc:
//		SAX parse handler class for parsing CTE configuration
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerCTEConfig_H
#define GPDXL_CParseHandlerCTEConfig_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"
#include "gpopt/engine/CCTEConfig.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerCTEConfig
	//
	//	@doc:
	//		SAX parse handler class for parsing CTE configuration options
	//
	//---------------------------------------------------------------------------
	class CParseHandlerCTEConfig : public CParseHandlerBase
	{
		private:

			// CTE configuration
			CCTEConfig *m_pcteconf;

			// private copy ctor
			CParseHandlerCTEConfig(const CParseHandlerCTEConfig&);

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
			// ctor
			CParseHandlerCTEConfig
				(
				IMemoryPool *memory_pool,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *pphRoot
				);

			// dtor
			virtual
			~CParseHandlerCTEConfig();

			// type of the parse handler
			virtual
			EDxlParseHandlerType Edxlphtype() const;

			// enumerator configuration
			CCTEConfig *Pcteconf() const;
	};
}

#endif // !GPDXL_CParseHandlerCTEConfig_H

// EOF
