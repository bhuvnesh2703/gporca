//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerWindowFrame.h
//
//	@doc:
//		SAX parse handler class for parsing the window frame in a window key
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerWindowFrame_H
#define GPDXL_CParseHandlerWindowFrame_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLWindowFrame.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"
#include "naucrates/dxl/xml/dxltokens.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerWindowFrame
	//
	//	@doc:
	//		SAX parse handler class for parsing the window frame in a
	//		window key.
	//
	//---------------------------------------------------------------------------
	class CParseHandlerWindowFrame : public CParseHandlerBase
	{
		private:

			// window frame specification
			EdxlFrameSpec m_edxlfs;

			// frame exclusion strategy
			EdxlFrameExclusionStrategy m_edxlfes;

			// window frame generated by the parse handler
			CDXLWindowFrame *m_window_frame;

			// private copy ctor
			CParseHandlerWindowFrame(const CParseHandlerWindowFrame&);

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
			CParseHandlerWindowFrame
				(
				IMemoryPool *memory_pool,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *pphRoot
				);

			// window frame generated by the parse handler
			CDXLWindowFrame *GetWindowFrame()
			{
				return m_window_frame;
			}
	};
}

#endif // !GPDXL_CParseHandlerWindowFrame_H

// EOF
