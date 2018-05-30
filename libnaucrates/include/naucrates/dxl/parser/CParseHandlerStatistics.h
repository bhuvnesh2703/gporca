//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerStatistics.h
//
//	@doc:
//		SAX parse handler class for parsing statistics from a DXL document.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerStatistics_H
#define GPDXL_CParseHandlerStatistics_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"
#include "naucrates/md/CDXLStatsDerivedRelation.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpnaucrates;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerStatistics
	//
	//	@doc:
	//		Parse handler for statistics.
	//
	//---------------------------------------------------------------------------
	class CParseHandlerStatistics : public CParseHandlerBase
	{
		private:

			// list of derived table statistics
			DrgPdxlstatsderrel *m_pdrgpdxlstatsderrel;

			// private copy ctor
			CParseHandlerStatistics(const CParseHandlerStatistics&);

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
			CParseHandlerStatistics
				(
				IMemoryPool *memory_pool,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *pphRoot
				);

			~CParseHandlerStatistics();

			virtual
			EDxlParseHandlerType Edxlphtype() const;

			// return the list of statistics objects
			DrgPdxlstatsderrel *Pdrgpdxlstatsderrel() const;

	};
}

#endif // !GPDXL_CParseHandlerStatistics_H

// EOF
