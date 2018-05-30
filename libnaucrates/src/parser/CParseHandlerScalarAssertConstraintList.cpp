//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Inc.
//
//	@filename:
//		CParseHandlerScalarAssertConstraintList.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing scalar 
//		assert operator constraint lists
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarAssertConstraintList.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarAssertConstraintList::CParseHandlerScalarAssertConstraintList
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerScalarAssertConstraintList::CParseHandlerScalarAssertConstraintList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerScalarOp(pmp, parse_handler_mgr, pphRoot),
	m_pdxlop(NULL),
	m_pdxlopAssertConstraint(NULL),
	m_pdrgpdxlnAssertConstraints(NULL)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarAssertConstraintList::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarAssertConstraintList::StartElement
	(
	const XMLCh* const , // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const , // element_qname,
	const Attributes& attrs
	)
{	
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarAssertConstraintList), element_local_name))
	{
		GPOS_ASSERT(NULL == m_pdxlop);
		GPOS_ASSERT(NULL == m_pdxlopAssertConstraint);
		GPOS_ASSERT(NULL == m_pdrgpdxlnAssertConstraints);
		
		m_pdxlop = GPOS_NEW(m_memory_pool) CDXLScalarAssertConstraintList(m_memory_pool);
		m_pdrgpdxlnAssertConstraints = GPOS_NEW(m_memory_pool) DrgPdxln(m_memory_pool);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarAssertConstraint), element_local_name))
	{
		GPOS_ASSERT(NULL != m_pdxlop);
		GPOS_ASSERT(NULL == m_pdxlopAssertConstraint);
				
		// parse error message
		CWStringDynamic *pstrErrorMsg = CDXLOperatorFactory::PstrValueFromAttrs
										(
										m_pphm->Pmm(), 
										attrs, 
										EdxltokenErrorMessage, 
										EdxltokenScalarAssertConstraint
										);
		m_pdxlopAssertConstraint = GPOS_NEW(m_memory_pool) CDXLScalarAssertConstraint(m_memory_pool, pstrErrorMsg);
		
		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_memory_pool, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
		m_pphm->ActivateParseHandler(pphChild);

		this->Append(pphChild);	
	}	
	else
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarAssertConstraintList::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarAssertConstraintList::EndElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const // element_qname
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarAssertConstraintList), element_local_name))
	{
		GPOS_ASSERT(NULL != m_pdxlop);
		GPOS_ASSERT(NULL != m_pdrgpdxlnAssertConstraints);
		
		// assemble final assert predicate node
		m_pdxln = GPOS_NEW(m_memory_pool) CDXLNode(m_memory_pool, m_pdxlop, m_pdrgpdxlnAssertConstraints);

#ifdef GPOS_DEBUG
	m_pdxlop->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG

		// deactivate handler
		m_pphm->DeactivateHandler();	
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarAssertConstraint), element_local_name))
	{
		GPOS_ASSERT(NULL != m_pdxlopAssertConstraint);

		CParseHandlerScalarOp *pphChild = dynamic_cast<CParseHandlerScalarOp*>((*this)[this->Length() - 1]);
		CDXLNode *pdxlnChild = pphChild->Pdxln();
		GPOS_ASSERT(NULL != pdxlnChild);
		pdxlnChild->AddRef();
		
		CDXLNode *pdxlnAssertConstraint = GPOS_NEW(m_memory_pool) CDXLNode(m_memory_pool, m_pdxlopAssertConstraint, pdxlnChild);
		m_pdrgpdxlnAssertConstraints->Append(pdxlnAssertConstraint);
		m_pdxlopAssertConstraint = NULL;
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}
}

// EOF
