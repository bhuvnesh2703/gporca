//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDistributionSpecRandom.cpp
//
//	@doc:
//		Specification of random distribution
//---------------------------------------------------------------------------

#include "naucrates/traceflags/traceflags.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CDistributionSpecRandom.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CPhysicalMotionRandom.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRandom::CDistributionSpecRandom
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDistributionSpecRandom::CDistributionSpecRandom()
	:
	m_is_duplicate_sensitive(false),
	m_fSatisfiedBySingleton(true),
	m_is_enforced_by_motion(false)
{
	if (COptCtxt::PoctxtFromTLS()->FDMLQuery())
	{
		// set duplicate sensitive flag to enforce Hash-Distribution of
		// Const Tables in DML queries
		MarkDuplicateSensitive();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRandom::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL 
CDistributionSpecRandom::Matches
	(
	const CDistributionSpec *pds
	) 
	const
{
	if (Edt() != pds->Edt())
	{
		if (pds->Edt() == EdtStrictRandom && IsEnforcedByMotion())
		{
			return true;
		}
		return false;
	}

	const CDistributionSpecRandom *pdsRandom =
			dynamic_cast<const CDistributionSpecRandom*>(pds);

	return pdsRandom->IsDuplicateSensitive() == m_is_duplicate_sensitive;
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRandom::FSatisfies
//
//	@doc:
//		Check if this distribution spec satisfies the given one
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecRandom::FSatisfies
	(
	const CDistributionSpec *pds
	)
	const
{
	if (Matches(pds))
	{
		return true;
	}
	
	if (EdtRandom == pds->Edt() && 
			(IsDuplicateSensitive() || !CDistributionSpecRandom::PdsConvert(pds)->IsDuplicateSensitive()))
	{
		return true;
	}
	
	if (EdtStrictRandom == pds->Edt() && m_is_enforced_by_motion)
	{
		return true;
	}
	return EdtAny == pds->Edt() || EdtNonSingleton == pds->Edt();
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRandom::AppendEnforcers
//
//	@doc:
//		Add required enforcers to dynamic array
//
//---------------------------------------------------------------------------
void
CDistributionSpecRandom::AppendEnforcers
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	CReqdPropPlan *
#ifdef GPOS_DEBUG
	prpp
#endif // GPOS_DEBUG
	,
	CExpressionArray *pdrgpexpr,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != mp);
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

	CDrvdPropPlan *drvd_prop_plan = CDrvdPropPlan::Pdpplan(exprhdl.Pdp());
	const CDistributionSpec *child_expr_distr_spec = drvd_prop_plan->Pds();
	if (child_expr_distr_spec->Edt() != CDistributionSpec::EdtUniversal)
	{
//		MarkEnforcedByMotion(false);
//	}
//	else
//	{
		MarkEnforcedByMotion(true);
	}
	
	// add a hashed distribution enforcer
	AddRef();
	pexpr->AddRef();
	CExpression *pexprMotion = GPOS_NEW(mp) CExpression
										(
										mp,
										GPOS_NEW(mp) CPhysicalMotionRandom(mp, this),
										pexpr
										);
	pdrgpexpr->Append(pexprMotion);		
}

BOOL
CDistributionSpecRandom::IsEnforcedByMotion() const
{
	return m_is_enforced_by_motion;
}

void
CDistributionSpecRandom::MarkEnforcedByMotion
	(
	BOOL is_enforced_by_motion
	)
{
	m_is_enforced_by_motion = is_enforced_by_motion;
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRandom::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CDistributionSpecRandom::OsPrint
	(
	IOstream &os
	)
	const
{
	return os << this->SzId();
}

// EOF

