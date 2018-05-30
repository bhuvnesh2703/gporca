//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc.
//
//	@filename:
//		CParseHandlerMDArrayCoerceCast.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing metadata for
//		GPDB array coerce cast functions
//---------------------------------------------------------------------------

#include "naucrates/md/CMDArrayCoerceCastGPDB.h"

#include "naucrates/dxl/parser/CParseHandlerMDArrayCoerceCast.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

// ctor
CParseHandlerMDArrayCoerceCast::CParseHandlerMDArrayCoerceCast
	(
	IMemoryPool *pmp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerMetadataObject(pmp, parse_handler_mgr, pphRoot)
{}

// invoked by Xerces to process an opening tag
void
CParseHandlerMDArrayCoerceCast::StartElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const, // element_qname
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBArrayCoerceCast), element_local_name))
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}

	// parse func name
	const XMLCh *xmlszFuncName = CDXLOperatorFactory::XmlstrFromAttrs
														(
														attrs,
														EdxltokenName,
														EdxltokenGPDBArrayCoerceCast
														);

	CMDName *pmdname = CDXLUtils::CreateMDNameFromXMLChar(m_pphm->Pmm(), xmlszFuncName);

	// parse cast properties
	IMDId *pmdid = CDXLOperatorFactory::PmdidFromAttrs
									(
									m_pphm->Pmm(),
									attrs,
									EdxltokenMdid,
									EdxltokenGPDBArrayCoerceCast
									);

	IMDId *pmdidSrc = CDXLOperatorFactory::PmdidFromAttrs
									(
									m_pphm->Pmm(),
									attrs,
									EdxltokenGPDBCastSrcType,
									EdxltokenGPDBArrayCoerceCast
									);

	IMDId *pmdidDest = CDXLOperatorFactory::PmdidFromAttrs
									(
									m_pphm->Pmm(),
									attrs,
									EdxltokenGPDBCastDestType,
									EdxltokenGPDBArrayCoerceCast
									);

	IMDId *pmdidCastFunc = CDXLOperatorFactory::PmdidFromAttrs
									(
									m_pphm->Pmm(),
									attrs,
									EdxltokenGPDBCastFuncId,
									EdxltokenGPDBArrayCoerceCast
									);

	// parse whether func returns a set
	BOOL fBinaryCoercible = CDXLOperatorFactory::FValueFromAttrs
									(
									m_pphm->Pmm(),
									attrs,
									EdxltokenGPDBCastBinaryCoercible,
									EdxltokenGPDBArrayCoerceCast
									);

	// parse coercion path type
	IMDCast::EmdCoercepathType eCoercePathType = (IMDCast::EmdCoercepathType)
													CDXLOperatorFactory::IValueFromAttrs
															(
															m_pphm->Pmm(),
															attrs,
															EdxltokenGPDBCastCoercePathType,
															EdxltokenGPDBArrayCoerceCast
															);

	INT iTypeModifier = CDXLOperatorFactory::IValueFromAttrs
							(
							m_pphm->Pmm(),
							attrs,
							EdxltokenTypeMod,
							EdxltokenGPDBArrayCoerceCast,
							true,
							IDefaultTypeModifier
							);

	BOOL fIsExplicit =CDXLOperatorFactory::FValueFromAttrs
									(
									m_pphm->Pmm(),
									attrs,
									EdxltokenIsExplicit,
									EdxltokenGPDBArrayCoerceCast
									);

	EdxlCoercionForm edcf = (EdxlCoercionForm) CDXLOperatorFactory::IValueFromAttrs
																		(
																		m_pphm->Pmm(),
																		attrs,
																		EdxltokenCoercionForm,
																		EdxltokenGPDBArrayCoerceCast
																		);

	INT iLoc = CDXLOperatorFactory::IValueFromAttrs
							(
							m_pphm->Pmm(),
							attrs,
							EdxltokenLocation,
							EdxltokenGPDBArrayCoerceCast
							);

	m_pimdobj = GPOS_NEW(m_memory_pool) CMDArrayCoerceCastGPDB(m_memory_pool, pmdid, pmdname, pmdidSrc, pmdidDest, fBinaryCoercible, pmdidCastFunc, eCoercePathType, iTypeModifier, fIsExplicit, edcf, iLoc);
}

// invoked by Xerces to process a closing tag
void
CParseHandlerMDArrayCoerceCast::EndElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const // element_qname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBArrayCoerceCast), element_local_name))
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
