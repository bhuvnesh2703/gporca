//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CParseHandlerMDGPDBFunc.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing metadata for
//		GPDB functions.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerMDGPDBFunc.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBFunc::CParseHandlerMDGPDBFunc
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerMDGPDBFunc::CParseHandlerMDGPDBFunc
	(
	IMemoryPool *memory_pool,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerMetadataObject(memory_pool, parse_handler_mgr, pphRoot),
	m_pmdid(NULL),
	m_pmdname(NULL),
	m_pmdidTypeResult(NULL),
	m_pdrgpmdidTypes(NULL),
	m_efuncstbl(CMDFunctionGPDB::EfsSentinel)
{}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBFunc::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDGPDBFunc::StartElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const, // element_qname
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFunc), element_local_name))
	{
		// parse func name
		const XMLCh *xmlszFuncName = CDXLOperatorFactory::XmlstrFromAttrs
															(
															attrs,
															EdxltokenName,
															EdxltokenGPDBFunc
															);

		CWStringDynamic *pstrFuncName = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), xmlszFuncName);
		
		// create a copy of the string in the CMDName constructor
		m_pmdname = GPOS_NEW(m_memory_pool) CMDName(m_memory_pool, pstrFuncName);
		
		GPOS_DELETE(pstrFuncName);

		// parse metadata id info
		m_pmdid = CDXLOperatorFactory::PmdidFromAttrs
										(
										m_pphm->Pmm(),
										attrs,
										EdxltokenMdid,
										EdxltokenGPDBFunc
										);
		
		// parse whether func returns a set
		m_fReturnsSet = CDXLOperatorFactory::FValueFromAttrs
												(
												m_pphm->Pmm(),
												attrs,
												EdxltokenGPDBFuncReturnsSet,
												EdxltokenGPDBFunc
												);
		// parse whether func is strict
		m_fStrict = CDXLOperatorFactory::FValueFromAttrs
											(
											m_pphm->Pmm(),
											attrs,
											EdxltokenGPDBFuncStrict,
											EdxltokenGPDBFunc
											);
		
		// parse func stability property
		const XMLCh *xmlszStbl = CDXLOperatorFactory::XmlstrFromAttrs
														(
														attrs,
														EdxltokenGPDBFuncStability,
														EdxltokenGPDBFunc
														);
		
		m_efuncstbl = EFuncStability(xmlszStbl);

		// parse func data access property
		const XMLCh *xmlszDataAcc = CDXLOperatorFactory::XmlstrFromAttrs
														(
														attrs,
														EdxltokenGPDBFuncDataAccess,
														EdxltokenGPDBFunc
														);

		m_efuncdataacc = EFuncDataAccess(xmlszDataAcc);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFuncResultTypeId), element_local_name))
	{
		// parse result type
		GPOS_ASSERT(NULL != m_pmdname);

		m_pmdidTypeResult = CDXLOperatorFactory::PmdidFromAttrs
													(
													m_pphm->Pmm(),
													attrs,
													EdxltokenMdid,
													EdxltokenGPDBFuncResultTypeId
													);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenOutputCols), element_local_name))
	{
		// parse output column type
		GPOS_ASSERT(NULL != m_pmdname);
		GPOS_ASSERT(NULL == m_pdrgpmdidTypes);

		const XMLCh *xmlszTypes = CDXLOperatorFactory::XmlstrFromAttrs
															(
															attrs,
															EdxltokenTypeIds,
															EdxltokenOutputCols
															);

		m_pdrgpmdidTypes = CDXLOperatorFactory::PdrgpmdidFromXMLCh
													(
													m_pphm->Pmm(),
													xmlszTypes,
													EdxltokenTypeIds,
													EdxltokenOutputCols
													);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBFunc::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDGPDBFunc::EndElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const // element_qname
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFunc), element_local_name))
	{
		// construct the MD func object from its part
		GPOS_ASSERT(m_pmdid->IsValid() && NULL != m_pmdname);
		
		m_imd_obj = GPOS_NEW(m_memory_pool) CMDFunctionGPDB(m_memory_pool,
												m_pmdid,
												m_pmdname,
												m_pmdidTypeResult,
												m_pdrgpmdidTypes,
												m_fReturnsSet,
												m_efuncstbl,
												m_efuncdataacc,
												m_fStrict);
		
		// deactivate handler
		m_pphm->DeactivateHandler();

	}
	else if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFuncResultTypeId), element_local_name) &&
			0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenOutputCols), element_local_name))
	{
		CWStringDynamic *pstr = CDXLUtils::CreateDynamicStringFromXMLChArray(m_pphm->Pmm(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->GetBuffer());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBFunc::EFuncStability
//
//	@doc:
//		Parses function stability property from XML string
//
//---------------------------------------------------------------------------
CMDFunctionGPDB::EFuncStbl 
CParseHandlerMDGPDBFunc::EFuncStability
	(
	const XMLCh *xmlsz
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFuncStable), xmlsz))
	{
		return CMDFunctionGPDB::EfsStable;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFuncImmutable), xmlsz))
	{
		return CMDFunctionGPDB::EfsImmutable;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFuncVolatile), xmlsz))
	{
		return CMDFunctionGPDB::EfsVolatile;
	}

	GPOS_RAISE
		(
		gpdxl::ExmaDXL,
		gpdxl::ExmiDXLInvalidAttributeValue,
		CDXLTokens::PstrToken(EdxltokenGPDBFuncStability)->GetBuffer(),
		CDXLTokens::PstrToken(EdxltokenGPDBFunc)->GetBuffer()
		);

	return CMDFunctionGPDB::EfsSentinel;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBFunc::EFuncDataAccess
//
//	@doc:
//		Parses function data access property from XML string
//
//---------------------------------------------------------------------------
CMDFunctionGPDB::EFuncDataAcc
CParseHandlerMDGPDBFunc::EFuncDataAccess
	(
	const XMLCh *xmlsz
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFuncNoSQL), xmlsz))
	{
		return CMDFunctionGPDB::EfdaNoSQL;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFuncContainsSQL), xmlsz))
	{
		return CMDFunctionGPDB::EfdaContainsSQL;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFuncReadsSQLData), xmlsz))
	{
		return CMDFunctionGPDB::EfdaReadsSQLData;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFuncModifiesSQLData), xmlsz))
	{
		return CMDFunctionGPDB::EfdaModifiesSQLData;
	}

	GPOS_RAISE
		(
		gpdxl::ExmaDXL,
		gpdxl::ExmiDXLInvalidAttributeValue,
		CDXLTokens::PstrToken(EdxltokenGPDBFuncDataAccess)->GetBuffer(),
		CDXLTokens::PstrToken(EdxltokenGPDBFunc)->GetBuffer()
		);

	return CMDFunctionGPDB::EfdaSentinel;
}

// EOF
