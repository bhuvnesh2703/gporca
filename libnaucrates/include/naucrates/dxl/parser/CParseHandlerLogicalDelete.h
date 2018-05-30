//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerLogicalDelete.h
//
//	@doc:
//		Parse handler for parsing a logical delete operator
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerLogicalDelete_H
#define GPDXL_CParseHandlerLogicalDelete_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerLogicalOp.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerLogicalDelete
	//
	//	@doc:
	//		Parse handler for parsing a logical delete operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerLogicalDelete : public CParseHandlerLogicalOp
	{
		private:

			// ctid column id
			ULONG m_ulCtid;

			// segmentId column id
			ULONG m_ulSegmentId;

			// delete col ids
			ULongPtrArray *m_pdrgpulDelete;

			// private copy ctor
			CParseHandlerLogicalDelete(const CParseHandlerLogicalDelete &);

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
			CParseHandlerLogicalDelete
				(
				IMemoryPool *pmp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerLogicalDelete_H

// EOF
