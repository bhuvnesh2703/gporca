//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CParseHandlerMDRelationCtas.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing CTAS
//		relation metadata.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerMDRelationCtas.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataColumns.h"
#include "naucrates/dxl/parser/CParseHandlerCtasStorageOptions.h"

#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/md/CMDRelationCtasGPDB.h"

#include "naucrates/dxl/operators/CDXLCtasStorageOptions.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDRelationCtas::CParseHandlerMDRelationCtas
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerMDRelationCtas::CParseHandlerMDRelationCtas
	(
	IMemoryPool *pmp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerMDRelation(pmp, parse_handler_mgr, pphRoot),
	m_pdrgpiVarTypeMod(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDRelationCtas::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDRelationCtas::StartElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const, // element_qname
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenRelationCTAS), element_local_name))
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}

	// parse main relation attributes: name, id, distribution policy and keys
	ParseRelationAttributes(attrs, EdxltokenRelation);

	GPOS_ASSERT(IMDId::EmdidGPDBCtas == m_pmdid->Emdidt());

	const XMLCh *xmlszSchema = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenSchema));
	if (NULL != xmlszSchema)
	{
		m_pmdnameSchema = CDXLUtils::CreateMDNameFromXMLChar(m_pphm->Pmm(), xmlszSchema);
	}
	
	// parse whether relation is temporary
	m_fTemporary = CDXLOperatorFactory::FValueFromAttrs
											(
											m_pphm->Pmm(),
											attrs,
											EdxltokenRelTemporary,
											EdxltokenRelation
											);

	// parse whether relation has oids
	const XMLCh *xmlszHasOids = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenRelHasOids));
	if (NULL != xmlszHasOids)
	{
		m_fHasOids = CDXLOperatorFactory::FValueFromXmlstr(m_pphm->Pmm(), xmlszHasOids, EdxltokenRelHasOids, EdxltokenRelation);
	}

	// parse storage type
	const XMLCh *xmlszStorageType = CDXLOperatorFactory::XmlstrFromAttrs
															(
															attrs,
															EdxltokenRelStorageType,
															EdxltokenRelation
															);
	m_erelstorage = CDXLOperatorFactory::ErelstoragetypeFromXmlstr(xmlszStorageType);

	// parse vartypemod
	const XMLCh *xmlszVarTypeMod = CDXLOperatorFactory::XmlstrFromAttrs
															(
															attrs,
															EdxltokenVarTypeModList,
															EdxltokenRelation
															);
	m_pdrgpiVarTypeMod = CDXLOperatorFactory::PdrgpiFromXMLCh
						(
						m_pphm->Pmm(),
						xmlszVarTypeMod,
						EdxltokenVarTypeModList,
						EdxltokenRelation
						);

	//parse handler for the storage options
	CParseHandlerBase *pphCTASOptions = CParseHandlerFactory::Pph(m_memory_pool, CDXLTokens::XmlstrToken(EdxltokenCTASOptions), m_pphm, this);
	m_pphm->ActivateParseHandler(pphCTASOptions);
	
	// parse handler for the columns
	CParseHandlerBase *pphColumns = CParseHandlerFactory::Pph(m_memory_pool, CDXLTokens::XmlstrToken(EdxltokenMetadataColumns), m_pphm, this);
	m_pphm->ActivateParseHandler(pphColumns);
	
	// store parse handlers
	this->Append(pphColumns);
	this->Append(pphCTASOptions);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDRelationCtas::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDRelationCtas::EndElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const // element_qname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenRelationCTAS), element_local_name))
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}

	CParseHandlerMetadataColumns *pphMdCol = dynamic_cast<CParseHandlerMetadataColumns *>((*this)[0]);
	CParseHandlerCtasStorageOptions *pphCTASOptions = dynamic_cast<CParseHandlerCtasStorageOptions *>((*this)[1]);

	GPOS_ASSERT(NULL != pphMdCol->Pdrgpmdcol());
	GPOS_ASSERT(NULL != pphCTASOptions->Pdxlctasopt());

	DrgPmdcol *pdrgpmdcol = pphMdCol->Pdrgpmdcol();
	CDXLCtasStorageOptions *pdxlctasopt = pphCTASOptions->Pdxlctasopt();

	pdrgpmdcol->AddRef();
	pdxlctasopt->AddRef();

	m_pimdobj = GPOS_NEW(m_memory_pool) CMDRelationCtasGPDB
								(
									m_memory_pool,
									m_pmdid,
									m_pmdnameSchema,
									m_pmdname,
									m_fTemporary,
									m_fHasOids,
									m_erelstorage,
									m_ereldistrpolicy,
									pdrgpmdcol,
									m_pdrgpulDistrColumns,
									m_pdrgpdrgpulKeys,									
									pdxlctasopt,
									m_pdrgpiVarTypeMod
								);

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
