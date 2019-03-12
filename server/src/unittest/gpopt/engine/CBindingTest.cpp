//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CBindingTest.cpp
//
//	@doc:
//		Test for checking bindings extracted for an expression
//---------------------------------------------------------------------------
#include "gpopt/engine/CEngine.h"
#include "gpopt/minidump/CMinidumperUtils.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"

#include "unittest/gpopt/engine/CBindingTest.h"
#include "unittest/gpopt/CTestUtils.h"

#define EXPECTED_BINDING 2
static
const CHAR *szQueryFile= "../data/dxl/minidump/ExtractOneBindingFromScalarGroups.mdp";

// TODO
GPOS_RESULT
CBindingTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CBindingTest::EresUnittest_Basic)
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


// TODO
GPOS_RESULT
CBindingTest::EresUnittest_Basic()
{
	CAutoMemoryPool amp(CAutoMemoryPool::ElcNone);
	IMemoryPool *mp = amp.Pmp();

	// load dump file
	CDXLMinidump *pdxlmd = CMinidumperUtils::PdxlmdLoad(mp, szQueryFile);
	GPOS_CHECK_ABORT;

	// set up MD providers
	CMDProviderMemory *pmdp = GPOS_NEW(mp) CMDProviderMemory(mp, szQueryFile);
	pmdp->AddRef();

	const CSystemIdArray *pdrgpsysid = pdxlmd->GetSysidPtrArray();
	CMDProviderArray *pdrgpmdp = GPOS_NEW(mp) CMDProviderArray(mp);
	pdrgpmdp->Append(pmdp);

	for (ULONG ul = 1; ul < pdrgpsysid->Size(); ul++)
	{
		pmdp->AddRef();
		pdrgpmdp->Append(pmdp);
	}

	CMDAccessor mda(mp, CMDCache::Pcache(), pdrgpsysid, pdrgpmdp);

	COptimizerConfig *optimizer_config = pdxlmd->GetOptimizerConfig();
	CBitSet *pbsEnabled = NULL;
	CBitSet *pbsDisabled = NULL;
	SetTraceflags(mp, pdxlmd->Pbs(), &pbsEnabled, &pbsDisabled);

	GPOS_ASSERT(NULL != optimizer_config);

	// setup opt ctx
	CAutoOptCtxt aoc
	(
	 mp,
	 &mda,
	 NULL,  /* pceeval */
	 CTestUtils::GetCostModel(mp)
	 );
	
	// translate DXL Tree -> Expr Tree
	CTranslatorDXLToExpr *pdxltr = GPOS_NEW(mp) CTranslatorDXLToExpr(mp, &mda);
	CExpression *pexprTranslated =	pdxltr->PexprTranslateQuery
	(
	 pdxlmd->GetQueryDXLRoot(),
	 pdxlmd->PdrgpdxlnQueryOutput(),
	 pdxlmd->GetCTEProducerDXLArray()
	 );
	
	gpdxl::ULongPtrArray *pdrgul = pdxltr->PdrgpulOutputColRefs();
	gpmd::CMDNameArray *pdrgpmdname = pdxltr->Pdrgpmdname();

	CQueryContext *pqc = CQueryContext::PqcGenerate(mp, pexprTranslated, pdrgul, pdrgpmdname, true /*fDeriveStats*/);

	// initialize engine and optimize query
	CEngine eng(mp);
	eng.Init(pqc, NULL /*search_stage_array*/);
	eng.Optimize();

	// extract plan
	CExpression *pexprPlan = eng.PexprExtractPlan();
	GPOS_ASSERT(NULL != pexprPlan);

	UlongPtrArray *number_of_bindings = eng.GetNumberOfBindings();
	ULONG bindings_for_xform = (ULONG) (*number_of_bindings)[0][CXform::ExfInnerJoinWithInnerSelect2IndexGetApply];

	GPOS_RESULT eres = GPOS_FAILED;

	if (bindings_for_xform == EXPECTED_BINDING)
		eres = GPOS_OK;

	// clean up
	pexprPlan->Release();
	pdrgpmdp->Release();
	GPOS_DELETE(pqc);
	GPOS_DELETE(pdxlmd);

	return eres;
}
// EOF
