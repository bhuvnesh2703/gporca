//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc
//
//	@filename:
//		CParseHandlerPhysicalCTAS.h
//
//	@doc:
//		Parse handler for parsing a physical CTAS operator
//		
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerPhysicalCTAS_H
#define GPDXL_CParseHandlerPhysicalCTAS_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerPhysicalCTAS
	//
	//	@doc:
	//		Parse handler for parsing a physical CTAS operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerPhysicalCTAS : public CParseHandlerPhysicalOp
	{
		private:
			
			// schema name
			CMDName *m_pmdnameSchema;
		
			// table name
			CMDName *m_pmdname;
	
			// list of distribution column positions		
			ULongPtrArray *m_pdrgpulDistr;
			
			// list of source column ids		
			ULongPtrArray *m_pdrgpulSource;

			// list of vartypmod
			IntPtrArray *m_pdrgpiVarTypeMod;
			
			// is this a temporary table
			BOOL m_fTemporary;
			
			// does table have oids
			BOOL m_fHasOids; 
			
			// distribution policy
			IMDRelation::Ereldistrpolicy m_ereldistrpolicy;
			
			// storage type
			IMDRelation::Erelstoragetype m_erelstorage;
		
			// private copy ctor
			CParseHandlerPhysicalCTAS(const CParseHandlerPhysicalCTAS &);

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
			CParseHandlerPhysicalCTAS
				(
				IMemoryPool *pmp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerPhysicalCTAS_H

// EOF
