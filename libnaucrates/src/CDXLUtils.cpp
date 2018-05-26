//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLUtils.cpp
//
//	@doc:
//		Implementation of the utility methods for parsing and searializing DXL.
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CAutoTimer.h"
#include "gpos/common/CAutoRef.h"
#include "gpos/common/CAutoRg.h"

#include "gpos/io/ioutils.h"
#include "gpos/io/CFileReader.h"
#include "gpos/io/COstreamString.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/task/CWorker.h"
#include "gpos/task/CTraceFlagIter.h"

#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/optimizer/COptimizerConfig.h"

#include "naucrates/base/CQueryToDXLResult.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/parser/CParseHandlerDXL.h"
#include "naucrates/dxl/parser/CParseHandlerPlan.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerDummy.h"
#include "naucrates/dxl/xml/CDXLMemoryManager.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/base/COptCtxt.h"

#include "naucrates/md/CMDRequest.h"
#include "naucrates/md/CDXLStatsDerivedRelation.h"

#include "naucrates/traceflags/traceflags.h"

#include <xercesc/sax2/SAX2XMLReader.hpp>
#include <xercesc/sax2/XMLReaderFactory.hpp>
#include <xercesc/framework/MemBufInputSource.hpp>
#include <xercesc/util/XMLString.hpp>
#include <xercesc/util/Base64.hpp>

using namespace gpdxl;
using namespace gpmd;
using namespace gpos;
using namespace gpopt;

XERCES_CPP_NAMESPACE_USE



//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::GetParseHandlerForDXLString
//
//	@doc:
//		Start the parsing of the given DXL string and return the top-level parser.
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
CParseHandlerDXL *
CDXLUtils::GetParseHandlerForDXLString
	(
	IMemoryPool *memory_pool,
	const CHAR *dxl_string,
	const CHAR *xsd_file_path
	)
{
	GPOS_ASSERT(NULL != memory_pool);
	// we need to disable OOM simulation here, otherwise xerces throws ABORT signal
	CAutoTraceFlag atf(EtraceSimulateOOM, false);
	CAutoTraceFlag atf2(EtraceSimulateAbort, false);

	// setup own memory manager
	CDXLMemoryManager *memory_manager = GPOS_NEW(memory_pool) CDXLMemoryManager(memory_pool);
	SAX2XMLReader* pxmlreader = XMLReaderFactory::createXMLReader(memory_manager);

#ifdef GPOS_DEBUG
	CWorker::Self()->ResetTimeSlice();
#endif // GPOS_DEBUG

	XMLCh *xmlszXSDPath = NULL;

	if (NULL != xsd_file_path)
	{
		// setup XSD validation
		pxmlreader->setFeature(XMLUni::fgSAX2CoreValidation, true);
		pxmlreader->setFeature(XMLUni::fgXercesDynamic, false);
		pxmlreader->setFeature(XMLUni::fgSAX2CoreNameSpaces, true);
		pxmlreader->setFeature(XMLUni::fgXercesSchema, true);
		
		pxmlreader->setFeature(XMLUni::fgXercesSchemaFullChecking, true);
		pxmlreader->setFeature(XMLUni::fgSAX2CoreNameSpacePrefixes, true);
		
		xmlszXSDPath = XMLString::transcode(xsd_file_path, memory_manager);
		pxmlreader->setProperty(XMLUni::fgXercesSchemaExternalSchemaLocation, (void*) xmlszXSDPath);
	}
			
	CParseHandlerManager *pphm = GPOS_NEW(memory_pool) CParseHandlerManager(memory_manager, pxmlreader);

	CParseHandlerDXL *pphdxl = CParseHandlerFactory::Pphdxl(memory_pool, pphm);

	pphm->ActivateParseHandler(pphdxl);
		
	MemBufInputSource *pmbis = new(memory_manager) MemBufInputSource(
				(const XMLByte*) dxl_string,
				strlen(dxl_string),
				"dxl test",
				false,
				memory_manager
	    	);

#ifdef GPOS_DEBUG
	CWorker::Self()->ResetTimeSlice();
#endif // GPOS_DEBUG

	try
	{
		pxmlreader->parse(*pmbis);
	}
	catch (const XMLException&)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLXercesParseError);
		return NULL;
	}
	catch (const SAXParseException&)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLXercesParseError);
		return NULL;
	}
	catch (const SAXException&)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLXercesParseError);
		return NULL;
	}


#ifdef GPOS_DEBUG
	CWorker::Self()->ResetTimeSlice();
#endif // GPOS_DEBUG

	GPOS_CHECK_ABORT;

	// cleanup
	delete pxmlreader;
	delete pmbis;
	GPOS_DELETE(pphm);
	GPOS_DELETE(memory_manager);
	delete xmlszXSDPath;

	// reset time slice counter as unloading deleting Xerces SAX2 readers seems to take a lot of time (OPT-491)
#ifdef GPOS_DEBUG
	CWorker::Self()->ResetTimeSlice();
#endif // GPOS_DEBUG

	GPOS_CHECK_ABORT;

	return pphdxl;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::GetParseHandlerForDXLFile
//
//	@doc:
//		Start the parsing of the given DXL string and return the top-level parser.
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
CParseHandlerDXL *
CDXLUtils::GetParseHandlerForDXLFile
	(
	IMemoryPool *memory_pool,
	const CHAR *dxl_filename,
	const CHAR *xsd_file_path
	)
{
	GPOS_ASSERT(NULL != memory_pool);
		
	// setup own memory manager
	CDXLMemoryManager mm(memory_pool);
	SAX2XMLReader* pxmlreader = NULL;
	{
		// we need to disable OOM simulation here, otherwise xerces throws ABORT signal
		CAutoTraceFlag atf(EtraceSimulateOOM, false);
		CAutoTraceFlag atf2(EtraceSimulateAbort, false);

		pxmlreader = XMLReaderFactory::createXMLReader(&mm);
	}
	
	XMLCh *xmlszXSDPath = NULL;
	
	if (NULL != xsd_file_path)
	{
		// setup XSD validation
		pxmlreader->setFeature(XMLUni::fgSAX2CoreValidation, true);
		pxmlreader->setFeature(XMLUni::fgXercesDynamic, false);
		pxmlreader->setFeature(XMLUni::fgSAX2CoreNameSpaces, true);
		pxmlreader->setFeature(XMLUni::fgXercesSchema, true);
		
		pxmlreader->setFeature(XMLUni::fgXercesSchemaFullChecking, true);
		pxmlreader->setFeature(XMLUni::fgSAX2CoreNameSpacePrefixes, true);
		
		xmlszXSDPath = XMLString::transcode(xsd_file_path, &mm);
		pxmlreader->setProperty(XMLUni::fgXercesSchemaExternalSchemaLocation, (void*) xmlszXSDPath);
	}

	CParseHandlerManager phm(&mm, pxmlreader);
	CParseHandlerDXL *pph = CParseHandlerFactory::Pphdxl(memory_pool, &phm);
	phm.ActivateParseHandler(pph);
	GPOS_CHECK_ABORT;

	try
	{
		CAutoTraceFlag atf(EtraceSimulateOOM, false);
		CAutoTraceFlag atf2(EtraceSimulateAbort, false);
		GPOS_CHECK_ABORT;

		pxmlreader->parse(dxl_filename);

		// reset time slice
#ifdef GPOS_DEBUG
	    CWorker::Self()->ResetTimeSlice();
#endif // GPOS_DEBUG
	}
	catch (const XMLException&)
	{
		GPOS_DELETE(pph);
		delete pxmlreader;
		delete[] xmlszXSDPath;
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLXercesParseError);

		return NULL;
	}
	catch (const SAXParseException&ex)
	{
		GPOS_DELETE(pph);
		delete pxmlreader;
		delete[] xmlszXSDPath;
		
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLXercesParseError);

		return NULL;
	}
	catch (const SAXException&)
	{
		GPOS_DELETE(pph);
		delete pxmlreader;
		delete[] xmlszXSDPath;
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLXercesParseError);

		return NULL;
	}

	GPOS_CHECK_ABORT;

	// cleanup
	delete pxmlreader;
	
	// reset time slice counter as unloading deleting Xerces SAX2 readers seems to take a lot of time (OPT-491)
#ifdef GPOS_DEBUG
    CWorker::Self()->ResetTimeSlice();
#endif // GPOS_DEBUG
    
	delete[] xmlszXSDPath;
	
	return pph;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::GetParseHandlerForDXLString
//
//	@doc:
//		Start the parsing of the given DXL string and return the top-level parser.
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
CParseHandlerDXL *
CDXLUtils::GetParseHandlerForDXLString
	(
	IMemoryPool *memory_pool,
	const CWStringBase *dxl_string,
	const CHAR *xsd_file_path
	)
{
	CAutoRg<CHAR> a_sz;
	a_sz = CreateMultiByteCharStringFromWCString(memory_pool, dxl_string->Wsz());
	CParseHandlerDXL *pphdxl = GetParseHandlerForDXLString(memory_pool, a_sz.Rgt(), xsd_file_path);
	return pphdxl;
}



//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::GetPlanDXLNode
//
//	@doc:
//		Parse DXL string into a DXL plan tree.
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
CDXLNode *
CDXLUtils::GetPlanDXLNode
	(
	IMemoryPool *memory_pool,
	const CHAR *dxl_string,
	const CHAR *xsd_file_path,
	ULLONG *plan_id,
	ULLONG *plan_space_size
	)
{
	GPOS_ASSERT(NULL != memory_pool);
	GPOS_ASSERT(NULL != plan_id);
	GPOS_ASSERT(NULL != plan_space_size);

	// create and install a parse handler for the DXL document
	CParseHandlerDXL *pphdxl = GetParseHandlerForDXLString(memory_pool, dxl_string, xsd_file_path);
	CAutoP<CParseHandlerDXL> a_pphdxl(pphdxl);
	
	GPOS_ASSERT(NULL != a_pphdxl.Value());
	
	// collect plan info from dxl parse handler
	CDXLNode *pdxlnRoot = a_pphdxl->PdxlnPlan();
	*plan_id = a_pphdxl->UllPlanId();
	*plan_space_size = a_pphdxl->UllPlanSpaceSize();
	
	GPOS_ASSERT(NULL != pdxlnRoot);
	
#ifdef GPOS_DEBUG
	pdxlnRoot->Pdxlop()->AssertValid(pdxlnRoot, true /* fValidateChildren */);
#endif
	
	pdxlnRoot->AddRef();
	
	return pdxlnRoot;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::ParseQueryToQueryDXLTree
//
//	@doc:
//		Parse DXL string representing the query into
//		1. a DXL tree representing the query
//		2. a DXL tree representing the query output
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
CQueryToDXLResult *
CDXLUtils::ParseQueryToQueryDXLTree
	(
	IMemoryPool *memory_pool,
	const CHAR *dxl_string,
	const CHAR *xsd_file_path
	)
{
	GPOS_ASSERT(NULL != memory_pool);

	// create and install a parse handler for the DXL document
	CParseHandlerDXL *pphdxl = GetParseHandlerForDXLString(memory_pool, dxl_string, xsd_file_path);
	CAutoP<CParseHandlerDXL> a_pphdxl(pphdxl);

	// collect dxl tree of the query from dxl parse handler
	CDXLNode *pdxlnRoot = pphdxl->PdxlnQuery();
	GPOS_ASSERT(NULL != pdxlnRoot);
	
#ifdef GPOS_DEBUG
	pdxlnRoot->Pdxlop()->AssertValid(pdxlnRoot, true /* fValidateChildren */);
#endif
		
	pdxlnRoot->AddRef();

	// collect the list of query output columns from the dxl parse handler
	GPOS_ASSERT(NULL != pphdxl->PdrgpdxlnOutputCols());
	DrgPdxln *pdrgpdxlnQO = pphdxl->PdrgpdxlnOutputCols();
	pdrgpdxlnQO->AddRef();

	// collect the list of CTEs
	DrgPdxln *pdrgpdxlnCTE = pphdxl->PdrgpdxlnCTE();
	GPOS_ASSERT(NULL != pdrgpdxlnCTE);
	pdrgpdxlnCTE->AddRef();

	CQueryToDXLResult *ptrOutput = GPOS_NEW(memory_pool) CQueryToDXLResult(pdxlnRoot, pdrgpdxlnQO, pdrgpdxlnCTE);

	return ptrOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::ParseDXLToScalarExprDXLNode
//
//	@doc:
//		Parse a scalar expression as a top level node in a "ScalarExpr" tag.
//---------------------------------------------------------------------------
CDXLNode *
CDXLUtils::ParseDXLToScalarExprDXLNode
	(
	IMemoryPool *memory_pool,
	const CHAR *dxl_string,
	const CHAR *xsd_file_path
	)
{
	GPOS_ASSERT(NULL != memory_pool);

	// create and install a parse handler for the DXL document
	CAutoP<CParseHandlerDXL> a_pphdxl(GetParseHandlerForDXLString(memory_pool, dxl_string, xsd_file_path));

	// collect dxl tree of the query from dxl parse handler
	CDXLNode *pdxlnRoot = a_pphdxl->PdxlnScalarExpr();
	GPOS_ASSERT(NULL != pdxlnRoot);
	pdxlnRoot->AddRef();

	return pdxlnRoot;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::ParseDXLToIMDObjectArray
//
//	@doc:
//		Parse a list of metadata objects from the given DXL string.
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
DrgPimdobj *
CDXLUtils::ParseDXLToIMDObjectArray
	(
	IMemoryPool *memory_pool,
	const CHAR *dxl_string,
	const CHAR *xsd_file_path
	)
{
	GPOS_ASSERT(NULL != memory_pool);

	// create and install a parse handler for the DXL document
	CParseHandlerDXL *pphdxl = GetParseHandlerForDXLString(memory_pool, dxl_string, xsd_file_path);
	CAutoP<CParseHandlerDXL> a_pphdxl(pphdxl);
	
	// collect metadata objects from dxl parse handler
	DrgPimdobj *pdrgpmdobj = pphdxl->Pdrgpmdobj();
	pdrgpmdobj->AddRef();
	
	return pdrgpmdobj;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::ParseDXLToMDId
//
//	@doc:
//		Parse an mdid from a DXL metadata document
//
//---------------------------------------------------------------------------
IMDId *
CDXLUtils::ParseDXLToMDId
	(
	IMemoryPool *memory_pool,
	const CWStringBase *dxl_string,
	const CHAR *xsd_file_path
	)
{
	GPOS_ASSERT(NULL != memory_pool);

	// create and install a parse handler for the DXL document
	CParseHandlerDXL *pphdxl = GetParseHandlerForDXLString(memory_pool, dxl_string, xsd_file_path);
	CAutoP<CParseHandlerDXL> a_pphdxl(pphdxl);
	
	// collect metadata objects from dxl parse handler
	DrgPmdid *pdrgpmdid = pphdxl->Pdrgpmdid();
	
	GPOS_ASSERT(1 == pdrgpmdid->Size());
	
	IMDId *mdid = (*pdrgpmdid)[0];
	mdid->AddRef();

	return mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::ParseDXLToMDRequest
//
//	@doc:
//		Parse a metadata request.
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
CMDRequest *
CDXLUtils::ParseDXLToMDRequest
	(
	IMemoryPool *memory_pool,
	const CHAR *dxl_string,
	const CHAR *xsd_file_path
	)
{
	GPOS_ASSERT(NULL != memory_pool);

	// create and install a parse handler for the DXL document
	CParseHandlerDXL *pphdxl = GetParseHandlerForDXLString(memory_pool, dxl_string, xsd_file_path);
	CAutoP<CParseHandlerDXL> a_pphdxl(pphdxl);

	// collect metadata ids from dxl parse handler
	CMDRequest *md_request = pphdxl->GetMiniDumper();
	GPOS_ASSERT(NULL != md_request);
	md_request->AddRef();

	return md_request;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::ParseDXLToMDRequest
//
//	@doc:
//		Parse an MD request from the given DXL string.
//		Same as above but with a wide-character input
//
//---------------------------------------------------------------------------
CMDRequest *
CDXLUtils::ParseDXLToMDRequest
	(
	IMemoryPool *memory_pool,
	const WCHAR *dxl_string,
	const CHAR *xsd_file_path
	)
{
	GPOS_ASSERT(NULL != memory_pool);

	CAutoRg<CHAR> a_szDXL(CDXLUtils::CreateMultiByteCharStringFromWCString(memory_pool, dxl_string));
	
	// create and install a parse handler for the DXL document
	CMDRequest *md_request = ParseDXLToMDRequest(memory_pool, a_szDXL.Rgt(), xsd_file_path);

	return md_request;
}

// parse optimizer config DXL
COptimizerConfig *
CDXLUtils::ParseDXLToOptimizerConfig
	(
	IMemoryPool *memory_pool,
	const CHAR *dxl_string,
	const CHAR *xsd_file_path
	)
{
	GPOS_ASSERT(NULL != memory_pool);

	// create and install a parse handler for the DXL document
	CParseHandlerDXL *pphdxl = GetParseHandlerForDXLString(memory_pool, dxl_string, xsd_file_path);
	// though we could access the traceflags member of the CParseHandlerDXL
	// here we don't access them or store them anywhere,
	// so, if using this function, note that any traceflags present
	// in the DXL being parsed will be discarded
	CAutoP<CParseHandlerDXL> a_pphdxl(pphdxl);

	// collect optimizer conf from dxl parse handler
	COptimizerConfig *poconf = pphdxl->Poconf();
	GPOS_ASSERT(NULL != poconf);
	poconf->AddRef();

	return poconf;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::ParseDXLToStatsDerivedRelArray
//
//	@doc:
//		Parse a list of statistics objects from the given DXL string.
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
DrgPdxlstatsderrel *
CDXLUtils::ParseDXLToStatsDerivedRelArray
	(
	IMemoryPool *memory_pool,
	const CWStringBase *dxl_string,
	const CHAR *xsd_file_path
	)
{
	GPOS_ASSERT(NULL != memory_pool);

	// create and install a parse handler for the DXL document
	CParseHandlerDXL *pphdxl = GetParseHandlerForDXLString(memory_pool, dxl_string, xsd_file_path);
	CAutoP<CParseHandlerDXL> a_pphdxl(pphdxl);

	// collect statistics objects from dxl parse handler
	DrgPdxlstatsderrel *dxl_derived_rel_stats_array = pphdxl->Pdrgpdxlstatsderrel();
	dxl_derived_rel_stats_array->AddRef();
	
	return dxl_derived_rel_stats_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::ParseDXLToStatsDerivedRelArray
//
//	@doc:
//		Parse a list of statistics objects from the given DXL string.
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
DrgPdxlstatsderrel *
CDXLUtils::ParseDXLToStatsDerivedRelArray
	(
	IMemoryPool *memory_pool,
	const CHAR *dxl_string,
	const CHAR *xsd_file_path
	)
{
	GPOS_ASSERT(NULL != memory_pool);

	// create and install a parse handler for the DXL document
	CParseHandlerDXL *pphdxl = GetParseHandlerForDXLString(memory_pool, dxl_string, xsd_file_path);
	CAutoP<CParseHandlerDXL> a_pphdxl(pphdxl);

	// collect statistics objects from dxl parse handler
	DrgPdxlstatsderrel *dxl_derived_rel_stats_array = pphdxl->Pdrgpdxlstatsderrel();
	dxl_derived_rel_stats_array->AddRef();
	
	return dxl_derived_rel_stats_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::ParseDXLToOptimizerStatisticObjArray
//
//	@doc:
//		Translate the array of dxl statistics objects to an array of 
//		optimizer statistics object.
//---------------------------------------------------------------------------
DrgPstats *
CDXLUtils::ParseDXLToOptimizerStatisticObjArray
	(
	IMemoryPool *memory_pool,
	CMDAccessor *md_accessor,
	DrgPdxlstatsderrel *dxl_derived_rel_stats_array
	)
{
	GPOS_ASSERT(NULL != dxl_derived_rel_stats_array);

	DrgPstats *pdrgpstat = GPOS_NEW(memory_pool) DrgPstats(memory_pool);
	const ULONG ulRelStat = dxl_derived_rel_stats_array->Size();
	for (ULONG ulIdxRelStat = 0; ulIdxRelStat < ulRelStat; ulIdxRelStat++)
	{
		// create hash map from colid -> histogram
		HMUlHist *phmulhist = GPOS_NEW(memory_pool) HMUlHist(memory_pool);
		
		// width hash map
		HMUlDouble *phmuldouble = GPOS_NEW(memory_pool) HMUlDouble(memory_pool);
		
		CDXLStatsDerivedRelation *pdxlstatsderrel = (*dxl_derived_rel_stats_array)[ulIdxRelStat];
		const DrgPdxlstatsdercol *pdrgpdxlstatsdercol = pdxlstatsderrel->Pdrgpdxlstatsdercol();
		
		const ULONG ulColStats = pdrgpdxlstatsdercol->Size();
		for (ULONG ulIdxColStat = 0; ulIdxColStat < ulColStats; ulIdxColStat++)
		{
			CDXLStatsDerivedColumn *dxl_derived_col_stats = (*pdrgpdxlstatsdercol)[ulIdxColStat];
			
			ULONG ulColId = dxl_derived_col_stats->UlColId();
			CDouble dWidth = dxl_derived_col_stats->DWidth();
			CDouble dNullFreq = dxl_derived_col_stats->DNullFreq();
			CDouble dDistinctRemain = dxl_derived_col_stats->DDistinctRemain();
			CDouble dFreqRemain = dxl_derived_col_stats->DFreqRemain();
			
			DrgPbucket *pdrgppbucket = CDXLUtils::ParseDXLToBucketsArray(memory_pool, md_accessor, dxl_derived_col_stats);
			CHistogram *phist = GPOS_NEW(memory_pool) CHistogram(pdrgppbucket, true /*fWellDefined*/, dNullFreq, dDistinctRemain, dFreqRemain);
			
			phmulhist->Insert(GPOS_NEW(memory_pool) ULONG(ulColId), phist);
			phmuldouble->Insert(GPOS_NEW(memory_pool) ULONG(ulColId), GPOS_NEW(memory_pool) CDouble(dWidth));
		}
		
		CDouble dRows = pdxlstatsderrel->DRows();
		CStatistics *pstats = GPOS_NEW(memory_pool) CStatistics
										(
										memory_pool,
										phmulhist,
										phmuldouble,
										dRows,
										false /* fEmpty */
										);
		//pstats->AddCardUpperBound(memory_pool, ulIdxRelStat, dRows);

		pdrgpstat->Append(pstats);
	}
	
	return pdrgpstat;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::ParseDXLToBucketsArray
//
//	@doc:
//		Extract the array of optimizer buckets from the dxl representation of
//		dxl buckets in the dxl derived column statistics object.
//---------------------------------------------------------------------------
DrgPbucket *
CDXLUtils::ParseDXLToBucketsArray
	(
	IMemoryPool *memory_pool,
	CMDAccessor *md_accessor,
	CDXLStatsDerivedColumn *dxl_derived_col_stats
	)
{
	DrgPbucket *pdrgppbucket = GPOS_NEW(memory_pool) DrgPbucket(memory_pool);
	
	const DrgPdxlbucket *pdrgpdxlbucket = dxl_derived_col_stats->Pdrgpdxlbucket();	
	const ULONG ulBuckets = pdrgpdxlbucket->Size();
	for (ULONG ul = 0; ul < ulBuckets; ul++)
	{
		CDXLBucket *pdxlbucket = (*pdrgpdxlbucket)[ul];
		
		// translate the lower and upper bounds of the bucket
		IDatum *pdatumLower = Pdatum(memory_pool, md_accessor, pdxlbucket->PdxldatumLower());
		CPoint *ppointLower = GPOS_NEW(memory_pool) CPoint(pdatumLower);
		
		IDatum *pdatumUpper = Pdatum(memory_pool, md_accessor, pdxlbucket->PdxldatumUpper());
		CPoint *ppointUpper = GPOS_NEW(memory_pool) CPoint(pdatumUpper);
		
		CBucket *pbucket = GPOS_NEW(memory_pool) CBucket
										(
										ppointLower,
										ppointUpper,
										pdxlbucket->FLowerClosed(),
										pdxlbucket->FUpperClosed(),
										pdxlbucket->DFrequency(),
										pdxlbucket->DDistinct()
										);
		
		pdrgppbucket->Append(pbucket);
	}
	
	return pdrgppbucket;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::Pdatum
//
//	@doc:
//		Translate the optimizer datum from dxl datum object
//
//---------------------------------------------------------------------------	
IDatum *
CDXLUtils::Pdatum
	(
	IMemoryPool *memory_pool,
	CMDAccessor *md_accessor,
	const CDXLDatum *pdxldatum
	 )
{
	IMDId *mdid = pdxldatum->Pmdid();
	return md_accessor->Pmdtype(mdid)->Pdatum(memory_pool, pdxldatum);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::ParseDXLToIMDObjectArray
//
//	@doc:
//		Parse a list of metadata objects from the given DXL string.
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
DrgPimdobj *
CDXLUtils::ParseDXLToIMDObjectArray
	(
	IMemoryPool *memory_pool,
	const CWStringBase *dxl_string,
	const CHAR *xsd_file_path
	)
{
	GPOS_ASSERT(NULL != memory_pool);

	// create and install a parse handler for the DXL document
	CParseHandlerDXL *pphdxl = GetParseHandlerForDXLString(memory_pool, dxl_string, xsd_file_path);
	CAutoP<CParseHandlerDXL> a_pphdxl(pphdxl);
	
	// collect metadata objects from dxl parse handler
	DrgPimdobj *pdrgpmdobj = pphdxl->Pdrgpmdobj();
	pdrgpmdobj->AddRef();
	
	return pdrgpmdobj;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::ParseDXLToIMDIdCacheObj
//
//	@doc:
//		Parse a single metadata object given its DXL representation.
// 		Returns NULL if the DXL represents no metadata objects, or the first parsed
//		object if it does.
//		If a non-empty XSD schema location is provided, the DXL is validated against
//		that schema, and an exception is thrown if the DXL does not conform.
//
//---------------------------------------------------------------------------
IMDCacheObject *
CDXLUtils::ParseDXLToIMDIdCacheObj
	(
	IMemoryPool *memory_pool,
	const CWStringBase *dxl_string,
	const CHAR *xsd_file_path
	)
{
	GPOS_ASSERT(NULL != memory_pool);

	// create and install a parse handler for the DXL document
	CAutoP<CParseHandlerDXL> a_pphdxl(GetParseHandlerForDXLString(memory_pool, dxl_string, xsd_file_path));
	
	// collect metadata objects from dxl parse handler
	DrgPimdobj *pdrgpmdobj = a_pphdxl->Pdrgpmdobj();

	if (0 == pdrgpmdobj->Size())
	{
		// no metadata objects found
		return NULL;
	}

	IMDCacheObject *pimdobjResult = (*pdrgpmdobj)[0];
	pimdobjResult->AddRef();
	
	return pimdobjResult;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeQuery
//
//	@doc:
//		Serialize a DXL Query tree into a DXL document
//
//---------------------------------------------------------------------------
void
CDXLUtils::SerializeQuery
	(
	IMemoryPool *memory_pool,
	IOstream &os,
	const CDXLNode *dxl_query_node,
	const DrgPdxln *pdrgpdxlnQueryOutput,
	const DrgPdxln *pdrgpdxlnCTE,
	BOOL fSerializeHeaderFooter,
	BOOL indentation
	)
{
	GPOS_ASSERT(NULL != memory_pool);
	GPOS_ASSERT(NULL != dxl_query_node && NULL != pdrgpdxlnQueryOutput);

	CAutoTimer at("\n[OPT]: DXL Query Serialization Time", GPOS_FTRACE(EopttracePrintOptimizationStatistics));

	CXMLSerializer xml_serializer(memory_pool, os, indentation);

	if (fSerializeHeaderFooter)
	{
		SerializeHeader(memory_pool, &xml_serializer);
	}
	
	xml_serializer.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenQuery));

	// serialize the query output columns
	xml_serializer.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenQueryOutput));
	for (ULONG ul = 0; ul < pdrgpdxlnQueryOutput->Size(); ++ul)
	{
		CDXLNode *pdxlnScId = (*pdrgpdxlnQueryOutput)[ul];
		pdxlnScId->SerializeToDXL(&xml_serializer);
	}
	xml_serializer.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenQueryOutput));

	// serialize the CTE list
	xml_serializer.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCTEList));
	const ULONG ulCTEs = pdrgpdxlnCTE->Size();
	for (ULONG ul = 0; ul < ulCTEs; ++ul)
	{
		CDXLNode *pdxlnCTE = (*pdrgpdxlnCTE)[ul];
		pdxlnCTE->SerializeToDXL(&xml_serializer);
	}
	xml_serializer.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCTEList));

	
	dxl_query_node->SerializeToDXL(&xml_serializer);

	xml_serializer.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenQuery));
	
	if (fSerializeHeaderFooter)
	{
		SerializeFooter(&xml_serializer);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeULLONG
//
//	@doc:
//		Serialize a ULLONG value
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::SerializeULLONG
	(
	IMemoryPool *memory_pool,
	ULLONG value
	)
{
	GPOS_ASSERT(NULL != memory_pool);
	CAutoTraceFlag atf(EtraceSimulateAbort, false);

	CAutoP<CWStringDynamic> string_var(GPOS_NEW(memory_pool) CWStringDynamic(memory_pool));

	// create a string stream to hold the result of serialization
	COstreamString oss(string_var.Value());
	oss << value;

	return string_var.Reset();
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializePlan
//
//	@doc:
//		Serialize a DXL tree into a DXL document
//
//---------------------------------------------------------------------------
void
CDXLUtils::SerializePlan
	(
	IMemoryPool *memory_pool,
	IOstream& os,
	const CDXLNode *pdxln,
	ULLONG plan_id,
	ULLONG plan_space_size,
	BOOL fSerializeHeaderFooter,
	BOOL indentation
	)
{
	GPOS_ASSERT(NULL != memory_pool);
	GPOS_ASSERT(NULL != pdxln);

	CAutoTimer at("\n[OPT]: DXL Plan Serialization Time", GPOS_FTRACE(EopttracePrintOptimizationStatistics));

	CXMLSerializer xml_serializer(memory_pool, os, indentation);
	
	if (fSerializeHeaderFooter)
	{
		SerializeHeader(memory_pool, &xml_serializer);
	}
	
	xml_serializer.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenPlan));

	// serialize plan id and space size attributes

	xml_serializer.AddAttribute(CDXLTokens::PstrToken(EdxltokenPlanId), plan_id);
	xml_serializer.AddAttribute(CDXLTokens::PstrToken(EdxltokenPlanSpaceSize), plan_space_size);

	pdxln->SerializeToDXL(&xml_serializer);

	xml_serializer.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenPlan));
	
	if (fSerializeHeaderFooter)
	{
		SerializeFooter(&xml_serializer);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeMetadata
//
//	@doc:
//		Serialize a list of MD objects into a DXL document and write to
//		to the provided output stream
//
//---------------------------------------------------------------------------
void
CDXLUtils::SerializeMetadata
	(
	IMemoryPool *memory_pool,
	const DrgPimdobj *pdrgpmdobj,
	IOstream &os,
	BOOL fSerializeHeaderFooter,
	BOOL indentation
	)
{
	GPOS_ASSERT(NULL != memory_pool);
	GPOS_ASSERT(NULL != pdrgpmdobj);

	CXMLSerializer xml_serializer(memory_pool, os, indentation);

	if (fSerializeHeaderFooter)
	{
		SerializeHeader(memory_pool, &xml_serializer);
	}
	
	xml_serializer.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMetadata));


	for (ULONG ul = 0; ul < pdrgpmdobj->Size(); ul++)
	{
		IMDCacheObject *pimdobj = (*pdrgpmdobj)[ul];
		pimdobj->Serialize(&xml_serializer);
	}

	xml_serializer.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMetadata));

	if (fSerializeHeaderFooter)
	{
		SerializeFooter(&xml_serializer);
	}
	
	return;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeMetadata
//
//	@doc:
//		Serialize a list of MD objects into a DXL document and write to
//		to the provided output stream
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::SerializeMetadata
	(
	IMemoryPool *memory_pool,
	const IMDId *mdid,
	BOOL fSerializeHeaderFooter,
	BOOL indentation
	)
{
	GPOS_ASSERT(NULL != memory_pool);
	GPOS_ASSERT(mdid->IsValid());

	CWStringDynamic *dxl_string = GPOS_NEW(memory_pool) CWStringDynamic(memory_pool);
	
	// create a string stream to hold the result of serialization
	COstreamString oss(dxl_string);
	
	CXMLSerializer xml_serializer(memory_pool, oss, indentation);

	if (fSerializeHeaderFooter)
	{
		SerializeHeader(memory_pool, &xml_serializer);
	}
	
	xml_serializer.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMetadata));
	
	xml_serializer.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
									CDXLTokens::PstrToken(EdxltokenMdid));				
	mdid->Serialize(&xml_serializer, CDXLTokens::PstrToken(EdxltokenValue));
	xml_serializer.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
					CDXLTokens::PstrToken(EdxltokenMdid));
	
	xml_serializer.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMetadata));

	if (fSerializeHeaderFooter)
	{
		SerializeFooter(&xml_serializer);
	}
	
	return dxl_string;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeSamplePlans
//
//	@doc:
//		Serialize a list of sample plans in the given enumerator config
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::SerializeSamplePlans
	(
	IMemoryPool *memory_pool,
	CEnumeratorConfig *enumerator_cfg,
	BOOL indentation
	)
{
	GPOS_ASSERT(NULL != memory_pool);

	CWStringDynamic *dxl_string = GPOS_NEW(memory_pool) CWStringDynamic(memory_pool);

	// create a string stream to hold the result of serialization
	COstreamString oss(dxl_string);

	CXMLSerializer xml_serializer(memory_pool, oss, indentation);
	SerializeHeader(memory_pool, &xml_serializer);

	xml_serializer.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenSamplePlans));

	const ULONG ulSize = enumerator_cfg->UlCreatedSamples();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		xml_serializer.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenSamplePlan));
		// we add 1 to plan id since id's are zero-based internally, and we reserve 0 for best plan
		xml_serializer.AddAttribute(CDXLTokens::PstrToken(EdxltokenPlanId), enumerator_cfg->UllPlanSample(ul) + 1);
		xml_serializer.AddAttribute(CDXLTokens::PstrToken(EdxltokenRelativeCost), enumerator_cfg->CostPlanSample(ul) / enumerator_cfg->CostBest());
		xml_serializer.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenSamplePlan));
	}

	xml_serializer.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenSamplePlans));

	SerializeFooter(&xml_serializer);

	return dxl_string;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeCostDistr
//
//	@doc:
//		Serialize cost distribution
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::SerializeCostDistr
	(
	IMemoryPool *memory_pool,
	CEnumeratorConfig *enumerator_cfg,
	BOOL indentation
	)
{
	GPOS_ASSERT(NULL != memory_pool);

	CWStringDynamic *dxl_string = GPOS_NEW(memory_pool) CWStringDynamic(memory_pool);

	// create a string stream to hold the result of serialization
	COstreamString oss(dxl_string);

	CXMLSerializer xml_serializer(memory_pool, oss, indentation);
	SerializeHeader(memory_pool, &xml_serializer);

	xml_serializer.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCostDistr));

	const ULONG ulSize = enumerator_cfg->UlCostDistrSize();
	for (ULLONG ul = 0; ul < ulSize; ul++)
	{
		xml_serializer.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenValue));
		xml_serializer.AddAttribute(CDXLTokens::PstrToken(EdxltokenX), enumerator_cfg->DCostDistrX(ul));
		xml_serializer.AddAttribute(CDXLTokens::PstrToken(EdxltokenY), enumerator_cfg->DCostDistrY(ul));
		xml_serializer.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenValue));
	}

	xml_serializer.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCostDistr));

	SerializeFooter(&xml_serializer);

	return dxl_string;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeMDRequest
//
//	@doc:
//		Serialize a list of mdids into a DXL MD Request document and write to
//		to the provided output stream
//
//---------------------------------------------------------------------------
void
CDXLUtils::SerializeMDRequest
	(
	IMemoryPool *memory_pool,
	CMDRequest *md_request,
	IOstream &os,
	BOOL fSerializeHeaderFooter,
	BOOL indentation
	)
{
	GPOS_ASSERT(NULL != memory_pool);
	GPOS_ASSERT(NULL != md_request);

	CXMLSerializer xml_serializer(memory_pool, os, indentation);

	if (fSerializeHeaderFooter)
	{
		SerializeHeader(memory_pool, &xml_serializer);
	}

	md_request->Serialize(&xml_serializer);

	if (fSerializeHeaderFooter)
	{
		SerializeFooter(&xml_serializer);
	}

	return;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeMDRequest
//
//	@doc:
//		Serialize a list of mdids into a DXL MD Request document and write to
//		to the provided output stream
//
//---------------------------------------------------------------------------
void
CDXLUtils::SerializeMDRequest
	(
	IMemoryPool *memory_pool,
	const IMDId *mdid,
	IOstream &os,
	BOOL fSerializeHeaderFooter,
	BOOL indentation
	)
{
	GPOS_ASSERT(NULL != memory_pool);
	GPOS_ASSERT(NULL != mdid);

	CXMLSerializer xml_serializer(memory_pool, os, indentation);

	if (fSerializeHeaderFooter)
	{
		SerializeHeader(memory_pool, &xml_serializer);
	}

	xml_serializer.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMDRequest));

	xml_serializer.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMdid));
	mdid->Serialize(&xml_serializer, CDXLTokens::PstrToken(EdxltokenValue));
	xml_serializer.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMdid));

	xml_serializer.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMDRequest));

	if (fSerializeHeaderFooter)
	{
		SerializeFooter(&xml_serializer);
	}

	return;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeStatistics
//
//	@doc:
//		Serialize a list of statistics objects into a DXL document
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::SerializeStatistics
	(
	IMemoryPool *memory_pool,
	CMDAccessor *md_accessor, 
	const DrgPstats *pdrgpstat,
	BOOL fSerializeHeaderFooter,
	BOOL indentation
	)
{
	GPOS_ASSERT(NULL != memory_pool);
	GPOS_ASSERT(NULL != pdrgpstat);
	CWStringDynamic *dxl_string = GPOS_NEW(memory_pool) CWStringDynamic(memory_pool);

	// create a string stream to hold the result of serialization
	COstreamString oss(dxl_string);

	CDXLUtils::SerializeStatistics(memory_pool, md_accessor, pdrgpstat, oss, fSerializeHeaderFooter, indentation);

	return dxl_string;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeStatistics
//
//	@doc:
//		Serialize a list of statistics objects into a DXL document and write to
//		to the provided output stream
//
//---------------------------------------------------------------------------
void
CDXLUtils::SerializeStatistics
	(
	IMemoryPool *memory_pool,
	CMDAccessor *md_accessor,
	const DrgPstats *pdrgpstat,
	IOstream &os,
	BOOL fSerializeHeaderFooter,
	BOOL indentation
	)
{
	GPOS_ASSERT(NULL != memory_pool);
	GPOS_ASSERT(NULL != pdrgpstat);

	CXMLSerializer xml_serializer(memory_pool, os, indentation);

	if (fSerializeHeaderFooter)
	{
		SerializeHeader(memory_pool, &xml_serializer);
	}

	xml_serializer.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenStatistics));

	GPOS_ASSERT(NULL != pdrgpstat);

	for (ULONG ul = 0; ul < pdrgpstat->Size(); ul++)
	{
		CStatistics *pstats = (*pdrgpstat)[ul];
		CDXLStatsDerivedRelation *pdxlstatsderrel = pstats->Pdxlstatsderrel(memory_pool, md_accessor);
		pdxlstatsderrel->Serialize(&xml_serializer);
		pdxlstatsderrel->Release();
	}

	xml_serializer.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenStatistics));

	if (fSerializeHeaderFooter)
	{
		SerializeFooter(&xml_serializer);
	}

	return;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeMetadata
//
//	@doc:
//		Serialize a list of MD objects into a DXL document
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::SerializeMetadata
	(
	IMemoryPool *memory_pool,
	const DrgPimdobj *pdrgpmdobj,
	BOOL fSerializeHeaderFooter,
	BOOL indentation
	)
{
	GPOS_ASSERT(NULL != memory_pool);
	GPOS_ASSERT(NULL != pdrgpmdobj);
	CWStringDynamic *dxl_string = GPOS_NEW(memory_pool) CWStringDynamic(memory_pool);
	
	// create a string stream to hold the result of serialization
	COstreamString oss(dxl_string);
	
	CDXLUtils::SerializeMetadata(memory_pool, pdrgpmdobj, oss, fSerializeHeaderFooter, indentation);

	return dxl_string;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeMDObj
//
//	@doc:
//		Serialize an MD object into a DXL document
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::SerializeMDObj
	(
	IMemoryPool *memory_pool,
	const IMDCacheObject *pimdobj,
	BOOL fSerializeHeaderFooter,
	BOOL indentation
	)
{
	GPOS_ASSERT(NULL != memory_pool);
	GPOS_ASSERT(NULL != pimdobj);
	CAutoTraceFlag atf2(EtraceSimulateAbort, false);

	CAutoP<CWStringDynamic> string_var(GPOS_NEW(memory_pool) CWStringDynamic(memory_pool));
	
	// create a string stream to hold the result of serialization
	COstreamString oss(string_var.Value());

	CXMLSerializer xml_serializer(memory_pool, oss, indentation);
	
	if (fSerializeHeaderFooter)
	{
		SerializeHeader(memory_pool, &xml_serializer);
		xml_serializer.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMetadata));
	}
	GPOS_CHECK_ABORT;
	
	pimdobj->Serialize(&xml_serializer);
	GPOS_CHECK_ABORT;
		
	if (fSerializeHeaderFooter)
	{
		xml_serializer.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMetadata));
		SerializeFooter(&xml_serializer);
	}
	
	GPOS_CHECK_ABORT;
	return string_var.Reset();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeScalarExpr
//
//	@doc:
//		Serialize a DXL tree representing a ScalarExpr into a DXL document
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::SerializeScalarExpr
	(
	IMemoryPool *memory_pool,
	const CDXLNode *pdxln,
	BOOL fSerializeHeaderFooter,
	BOOL indentation
	)
{
	GPOS_ASSERT(NULL != memory_pool);
	GPOS_ASSERT(NULL != pdxln);
	CWStringDynamic *dxl_string = GPOS_NEW(memory_pool) CWStringDynamic(memory_pool);

	// create a string stream to hold the result of serialization
	COstreamString oss(dxl_string);
	CXMLSerializer xml_serializer(memory_pool, oss, indentation);

	if (fSerializeHeaderFooter)
	{
		SerializeHeader(memory_pool, &xml_serializer);
	}
	xml_serializer.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenScalarExpr));

	// serialize the content of the scalar expression
	pdxln->SerializeToDXL(&xml_serializer);
	xml_serializer.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenScalarExpr));
	if (fSerializeHeaderFooter)
	{
		SerializeFooter(&xml_serializer);
	}
	return dxl_string;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeHeader
//
//	@doc:
//		Serialize the DXL document header
//
//---------------------------------------------------------------------------
void
CDXLUtils::SerializeHeader
	(
	IMemoryPool *memory_pool,
	CXMLSerializer *xml_serializer
	)
{
	GPOS_ASSERT(NULL != xml_serializer);
	
	xml_serializer->StartDocument();
	
	xml_serializer->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenDXLMessage));
	
	// add namespace specification xmlns:dxl="...."
	CWStringDynamic dstrNamespaceAttr(memory_pool);
	dstrNamespaceAttr.AppendFormat
						(
						GPOS_WSZ_LIT("%ls%ls%ls"),
						CDXLTokens::PstrToken(EdxltokenNamespaceAttr)->Wsz(),
						CDXLTokens::PstrToken(EdxltokenColon)->Wsz(),
						CDXLTokens::PstrToken(EdxltokenNamespacePrefix)->Wsz()
						);
	
	xml_serializer->AddAttribute(&dstrNamespaceAttr, CDXLTokens::PstrToken(EdxltokenNamespaceURI));
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeFooter
//
//	@doc:
//		Serialize the DXL document footer
//
//---------------------------------------------------------------------------
void
CDXLUtils::SerializeFooter
	(
	CXMLSerializer *xml_serializer
	)
{
	GPOS_ASSERT(NULL != xml_serializer);
	
	xml_serializer->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenDXLMessage));
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::CreateDynamicStringFromXMLChArray
//
//	@doc:
//		Create a GPOS string object from a Xerces XMLCh* string.
//		The function allocates the returned string in the provided memory pool
//		and it is the responsibility of the caller to release it.
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::CreateDynamicStringFromXMLChArray
	(
	CDXLMemoryManager *memory_manager,
	const XMLCh *xml_string
	)
{
	GPOS_ASSERT(NULL != memory_manager);
	GPOS_ASSERT(NULL != xml_string);
	
	IMemoryPool *memory_pool = memory_manager->Pmp();
	
	{
		CAutoTraceFlag atf(EtraceSimulateOOM, false);
		CHAR *sz = XMLString::transcode(xml_string, memory_manager);

		CWStringDynamic *dxl_string = GPOS_NEW(memory_pool) CWStringDynamic(memory_pool);
		dxl_string->AppendFormat(GPOS_WSZ_LIT("%s"), sz);
	
		// cleanup temporary buffer
		XMLString::release(&sz, memory_manager);
	
		return dxl_string;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PstrFromBase64XMLStr
//
//	@doc:
//		Create a decoded byte array from a base 64 encoded XML string.
//
//---------------------------------------------------------------------------
BYTE *
CDXLUtils::CreateStringFrom64XMLStr
	(
	CDXLMemoryManager *memory_manager,
	const XMLCh *xml_string,
	ULONG *length // output: length of constructed byte array
	)
{
	GPOS_ASSERT(NULL != memory_manager);
	GPOS_ASSERT(NULL != xml_string);

	CAutoTraceFlag atf(EtraceSimulateOOM, false);
	IMemoryPool *memory_pool = memory_manager->Pmp();

	// find out xml string length
	ULONG ulLen = XMLString::stringLen(xml_string);

	// convert XML string into array of XMLByte
	CAutoRg<XMLByte> a_dataInByte;
	a_dataInByte = (XMLByte *) GPOS_NEW_ARRAY(memory_pool, XMLByte, ulLen + 1);
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		GPOS_ASSERT(xml_string[ul] <= 256 && "XML string not in Base64 encoding");

		a_dataInByte[ul] = (XMLByte) xml_string[ul];
	}
	a_dataInByte[ulLen] = 0;

	// decode string
	XMLSize_t xmlBASize = 0;
	XMLByte *pxmlbyteOut = Base64::decode(a_dataInByte.Rgt(), &xmlBASize, memory_manager);
	*length = static_cast<ULONG>(xmlBASize);

	return static_cast<BYTE *>(pxmlbyteOut);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::CreateDynamicStringFromCharArray
//
//	@doc:
//		Create a GPOS string object from a character array.
//		The function allocates the returned string in the provided memory pool
//		and it is the responsibility of the caller to release it.
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::CreateDynamicStringFromCharArray
	(
	IMemoryPool *memory_pool,
	const CHAR *sz
	)
{
	GPOS_ASSERT(NULL != sz);
	
	CAutoP<CWStringDynamic> string_var(GPOS_NEW(memory_pool) CWStringDynamic(memory_pool));
	string_var->AppendFormat(GPOS_WSZ_LIT("%s"), sz);
	return string_var.Reset();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::CreateMDNameFromCharArray
//
//	@doc:
//		Create a GPOS string object from a character array.
//		The function allocates the returned string in the provided memory pool
//		and it is the responsibility of the caller to release it.
//
//---------------------------------------------------------------------------
CMDName *
CDXLUtils::CreateMDNameFromCharArray
	(
	IMemoryPool *memory_pool,
	const CHAR *sz
	)
{
	GPOS_ASSERT(NULL != sz);
	
	CWStringDynamic *dxl_string = CDXLUtils::CreateDynamicStringFromCharArray(memory_pool, sz);
	CMDName *pmdname = GPOS_NEW(memory_pool) CMDName(memory_pool, dxl_string);

	// CMDName ctor created a copy of the string
	GPOS_DELETE(dxl_string);
	
	return pmdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::PmdnameFromXmlsz
//
//	@doc:
//		Create a GPOS string object from a Xerces character array.
//		The function allocates the returned string in the provided memory pool
//		and it is the responsibility of the caller to release it.
//
//---------------------------------------------------------------------------
CMDName *
CDXLUtils::PmdnameFromXmlsz
	(
	CDXLMemoryManager *memory_manager,
	const XMLCh *xml_string
	)
{
	GPOS_ASSERT(NULL != xml_string);
	
	CHAR *sz = XMLString::transcode(xml_string, memory_manager);
	CMDName *pmdname = CreateMDNameFromCharArray(memory_manager->Pmp(), sz);
	
	// cleanup temporary buffer
	XMLString::release(&sz, memory_manager);
	
	return pmdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::EncodeByteArrayToString
//
//	@doc:
//		Use Base64 encoding to convert bytearray to a string.
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::EncodeByteArrayToString
	(
	IMemoryPool *memory_pool,
	const BYTE *byte,
	ULONG length
	)
{
	CAutoP<CDXLMemoryManager> a_pmm(GPOS_NEW(memory_pool) CDXLMemoryManager(memory_pool));
	CAutoP<CWStringDynamic> string_var(GPOS_NEW(memory_pool) CWStringDynamic(memory_pool));

	GPOS_ASSERT(length > 0);

	XMLSize_t outputLength = 0;
	const XMLByte *input = (const XMLByte *) byte;

	XMLSize_t inputLength = (XMLSize_t) length;

	CAutoRg<XMLByte> a_pxmlbyteBuf;

	{
		CAutoTraceFlag atf(EtraceSimulateOOM, false);
		a_pxmlbyteBuf = Base64::encode(input, inputLength, &outputLength, a_pmm.Value());
	}

	GPOS_ASSERT(NULL != a_pxmlbyteBuf.Rgt());

	// assert that last byte is 0
	GPOS_ASSERT(0 == a_pxmlbyteBuf[outputLength]);

	// there may be padded bytes. We don't need them. We zero out there bytes.
#ifdef GPOS_DEBUG
	ULONG ulNewLength = outputLength;
#endif  // GPOS_DEBUG
	while (('\n' == a_pxmlbyteBuf[outputLength]
			|| 0 == a_pxmlbyteBuf[outputLength])
			&& 0 < outputLength)
	{
		a_pxmlbyteBuf[outputLength] = 0;
#ifdef GPOS_DEBUG
		ulNewLength = outputLength;
#endif  // GPOS_DEBUG
		outputLength--;
	}
	GPOS_ASSERT(0 == a_pxmlbyteBuf[ulNewLength]);

	CHAR *szRetBuf = (CHAR *) (a_pxmlbyteBuf.Rgt());
	string_var->AppendCharArray(szRetBuf);

	return string_var.Reset();
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::DecodeByteArrayFromString
//
//	@doc:
//		Decode byte array from Base64 encoded string.
//
//---------------------------------------------------------------------------
BYTE *
CDXLUtils::DecodeByteArrayFromString
	(
	IMemoryPool *memory_pool,
	const CWStringDynamic *dxl_string,
	ULONG *length
	)
{
	CAutoP<CDXLMemoryManager> a_pmm(GPOS_NEW(memory_pool) CDXLMemoryManager(memory_pool));

	XMLSize_t xmlBASize = 0;

	const WCHAR *pwc = dxl_string->Wsz();

	// We know that the input is encoded using Base64.
	XMLSize_t srcLen = dxl_string->UlLength();

	CAutoRg<XMLByte> a_dataInByte;

	a_dataInByte = (XMLByte*) GPOS_NEW_ARRAY(memory_pool, XMLByte, srcLen+1);

	for (XMLSize_t i = 0; i < srcLen; i++)
	{
		GPOS_ASSERT(pwc[i] <= 256);
		a_dataInByte[i] = (XMLByte) pwc[i];
	}

	a_dataInByte[srcLen] = 0;

	XMLByte *pxmlba = NULL;
	{
		CAutoTraceFlag atf(EtraceSimulateOOM, false);
		pxmlba = Base64::decode(a_dataInByte.Rgt(), &xmlBASize, a_pmm.Value());
	}

	(* length) = static_cast<ULONG>(xmlBASize);

	return static_cast<BYTE *>(pxmlba);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::Serialize
//
//	@doc:
//		Serialize a list of unsigned integers into a comma-separated string
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLUtils::Serialize
	(
	IMemoryPool *memory_pool,
	const ULongPtrArray2D *pdrgpdrgpul
	)
{
	const ULONG ulLen = pdrgpdrgpul->Size();
	CWStringDynamic *pstrKeys = GPOS_NEW(memory_pool) CWStringDynamic(memory_pool);
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		ULongPtrArray *pdrgpul = (*pdrgpdrgpul)[ul];
		CWStringDynamic *pstrKeySet = CDXLUtils::Serialize(memory_pool, pdrgpul);

		pstrKeys->Append(pstrKeySet);

		if (ul < ulLen - 1)
		{
			pstrKeys->AppendFormat(GPOS_WSZ_LIT("%ls"), GPOS_WSZ_LIT(";"));
		}

		GPOS_DELETE(pstrKeySet);
	}

	return pstrKeys;
}

// Serialize a list of chars into a comma-separated string
CWStringDynamic *
CDXLUtils::SerializeToCommaSeparatedString
	(
	IMemoryPool *memory_pool,
	const CharPtrArray *pdrgsz
	)
{
	CWStringDynamic *dxl_string = GPOS_NEW(memory_pool) CWStringDynamic(memory_pool);

	ULONG length = pdrgsz->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CHAR value = *((*pdrgsz)[ul]);
		if (ul == length - 1)
		{
			// last element: do not print a comma
			dxl_string->AppendFormat(GPOS_WSZ_LIT("%c"), value);
		}
		else
		{
			dxl_string->AppendFormat(GPOS_WSZ_LIT("%c%ls"), value, CDXLTokens::PstrToken(EdxltokenComma)->Wsz());
		}
	}

	return dxl_string;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::CreateMultiByteCharStringFromWCString
//
//	@doc:
//		Converts a wide character string into a character array in the provided memory pool
//
//---------------------------------------------------------------------------
CHAR *
CDXLUtils::CreateMultiByteCharStringFromWCString
	(
	IMemoryPool *memory_pool,
	const WCHAR *wc_string
	)
{
	GPOS_ASSERT(NULL != wc_string);

	ULONG ulMaxLength = GPOS_WSZ_LENGTH(wc_string) * GPOS_SIZEOF(WCHAR) + 1;
	CHAR *sz = GPOS_NEW_ARRAY(memory_pool, CHAR, ulMaxLength);
	CAutoRg<CHAR> a_sz(sz);

#ifdef GPOS_DEBUG
	INT i = (INT)
#endif
	wcstombs(sz, wc_string, ulMaxLength);
	GPOS_ASSERT(0 <= i);

	sz[ulMaxLength - 1] = '\0';

	return a_sz.RgtReset();
}

//---------------------------------------------------------------------------
//		CDXLUtils::Read
//
//	@doc:
//		Read a given text file in a character buffer.
//		The function allocates memory from the provided memory pool, and it is
//		the responsibility of the caller to deallocate it.
//
//---------------------------------------------------------------------------
CHAR *
CDXLUtils::Read
	(
	IMemoryPool *memory_pool,
	const CHAR *filename
	)
{
	GPOS_TRACE_FORMAT("opening file %s", filename);

	CFileReader fr;
	fr.Open(filename);

	ULONG_PTR ulpFileSize = (ULONG_PTR) fr.FileSize();
	CAutoRg<CHAR> a_szBuffer(GPOS_NEW_ARRAY(memory_pool, CHAR, ulpFileSize + 1));
	
	ULONG_PTR ulpRead = fr.ReadBytesToBuffer((BYTE *) a_szBuffer.Rgt(), ulpFileSize);
	fr.Close();
	
	GPOS_ASSERT(ulpRead == ulpFileSize);
	
	a_szBuffer[ulpRead] = '\0';
		
	return a_szBuffer.RgtReset();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::SerializeBound
//
//	@doc:
//		Serialize a datum with a given tag. Result xml looks like
// 		<tag>
//			<const.../>
//		</tag>
//
//---------------------------------------------------------------------------
void
CDXLUtils::SerializeBound
	(
	IDatum *pdatum,
	const CWStringConst *dxl_string,
	CXMLSerializer *xml_serializer
	)
{
	xml_serializer->OpenElement
				(
				CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
				dxl_string
				);

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDType *pmdtype = md_accessor->Pmdtype(pdatum->Pmdid());
	CDXLScalarConstValue *pdxlop = pmdtype->PdxlopScConst(xml_serializer->Pmp(), pdatum);
	pdxlop->SerializeToDXL(xml_serializer, NULL);

	xml_serializer->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
						dxl_string);
	pdxlop->Release();
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLUtils::DebugPrintMDIdArray
//
//	@doc:
//		Print an array of mdids
//
//---------------------------------------------------------------------------
void
CDXLUtils::DebugPrintMDIdArray
	(
	IOstream &os,
	DrgPmdid *pdrgpmdid
	)
{
	ULONG ulLen = pdrgpmdid->Size();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		const IMDId *mdid = (*pdrgpmdid)[ul];
		mdid->OsPrint(os);
		os << " ";
	}

	os << std::endl;
}
#endif

// EOF

