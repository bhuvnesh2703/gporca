//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#include "gpopt/base/CDistributionSpecForced.h"
#include "naucrates/traceflags/traceflags.h"
#include "gpopt/operators/CPhysicalMotionRandom.h"

using namespace gpopt;

CDistributionSpecForced::CDistributionSpecForced()
{
}

BOOL CDistributionSpecForced::FMatch(const CDistributionSpec *pds) const
{
	if (pds->Edt() == EdtRandom)
	{
		const CDistributionSpecRandom *pdsRandom = CDistributionSpecRandom::PdsConvert(pds);
		if (!pdsRandom-FDuplicateSensitive())
			return true;
	}
    return pds->Edt() == Edt() || EdtAny == pds->Edt();
}

BOOL CDistributionSpecForced::FSatisfies(const CDistributionSpec *pds) const
{
    return FMatch(pds);
}

void
CDistributionSpecForced::AppendEnforcers
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CReqdPropPlan *
#ifdef GPOS_DEBUG
	prpp
#endif // GPOS_DEBUG
	,
	DrgPexpr *pdrgpexpr,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(&exprhdl !=NULL);
//	CDrvdPropPlan *pdpplan = CDrvdPropPlan::Pdpplan((&exprhdl).Pdp());
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(!GPOS_FTRACE(EopttraceDisableMotions));
	GPOS_ASSERT(this == prpp->Ped()->PdsRequired() &&
	            "required plan properties don't match enforced distribution spec");


	if (GPOS_FTRACE(EopttraceDisableMotionRandom))
	{
		// random Motion is disabled
		return;
	}
	
//	CDrvdPropPlan *pdrvdPropPlan = CDrvdPropPlan::Pdpplan(pexpr->PdpDerive());
//	GPOS_ASSERT(pdpplan != NULL);

	// add a hashed distribution enforcer
	AddRef();
	pexpr->AddRef();
	CExpression *pexprMotion = GPOS_NEW(pmp) CExpression
										(
										pmp,
										GPOS_NEW(pmp) CPhysicalMotionRandom(pmp, this),
										pexpr
										);
	pdrgpexpr->Append(pexprMotion);		
}
