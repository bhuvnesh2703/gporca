//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerLogicalSetOp.h
//
//	@doc:
//		Parse handler for parsing a logical set operator
//
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerLogicalSetOp_H
#define GPDXL_CParseHandlerLogicalSetOp_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerLogicalOp.h"
#include "naucrates/dxl/operators/CDXLLogicalSetOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerLogicalSetOp
	//
	//	@doc:
	//		Parse handler for parsing a logical set operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerLogicalSetOp : public CParseHandlerLogicalOp
	{
		private:

			// set operation type
			EdxlSetOpType m_edxlsetop;

			// array of input column id arrays
			ULongPtrArray2D *m_pdrgpdrgpulInputColIds;

			// do the columns across inputs need to be casted
			BOOL m_fCastAcrossInputs;

			// private copy ctor
			CParseHandlerLogicalSetOp(const CParseHandlerLogicalSetOp &);

			// return the set operation type
			EdxlSetOpType Edxlsetop(const XMLCh* const element_local_name);

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
			CParseHandlerLogicalSetOp
				(
				IMemoryPool *memory_pool,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *pphRoot
				);

			// dtor
			~CParseHandlerLogicalSetOp();
	};
}

#endif // !GPDXL_CParseHandlerLogicalSetOp_H

// EOF
