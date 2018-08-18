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


CDistributionSpecRandom::CDistributionSpecRandom(ESpecOrigin spec_origin)
	:
	m_is_duplicate_sensitive(false),
	m_fSatisfiedBySingleton(true),
	m_is_child_universal(false),
	m_spec_origin(spec_origin)
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
	
	// random spec satisfies explicit random spec in some cases
	if (EdtStrictRandom == pds->Edt())
	{
		// if motion node enforcing random spec has a universal child,
		// the random motion node poses duplicate data hazard. To avoid duplication,
		// the random motion node is converted into a result node with hash filter in
		// dxl to plannedstmt translator.
		// if requested spec is explicit random and the node delivering random spec does not
		// have a universal child, in such cases random spec satisifies explicit random and
		// an additional random motion is not required
		if (CDistributionSpecRandom::EsoRequired == GetSpecOrigin() && !IsChildUniversal())
		{
			return true;
		}
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

	// if the child of the random motion has universal spec, mark random motion spec
	// to indicate it
	CDrvdPropPlan *drvd_prop_plan = CDrvdPropPlan::Pdpplan(exprhdl.Pdp());
	const CDistributionSpec *child_expr_distr_spec = drvd_prop_plan->Pds();
	if (child_expr_distr_spec->Edt() == CDistributionSpec::EdtUniversal)
	{
		MarkUniversalChild();
	}

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

CDistributionSpecRandom::ESpecOrigin
CDistributionSpecRandom::GetSpecOrigin() const
{
	return m_spec_origin;
}

BOOL
CDistributionSpecRandom::IsChildUniversal() const
{
	return m_is_child_universal;
}

void
CDistributionSpecRandom::MarkUniversalChild()
{
	m_is_child_universal = true;
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

