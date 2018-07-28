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
	m_fDuplicateSensitive(false),
	m_fSatisfiedBySingleton(true),
	really_duplicate_sensitive(false)
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
//		CDistributionSpecRandom::FMatch
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL 
CDistributionSpecRandom::FMatch
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

	return pdsRandom->FDuplicateSensitive() == m_fDuplicateSensitive;
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
	if (FMatch(pds))
	{
		return true;
	}
	
	
	if (EdtRandom == pds->Edt() && 
			(FDuplicateSensitive() || !CDistributionSpecRandom::PdsConvert(pds)->FDuplicateSensitive()))
	{
		return true;
	}
	
	if (EdtForced == pds->Edt() && !FReallyDuplicateSensitive())
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
//	if (&exprhdl)
	GPOS_ASSERT(NULL != &exprhdl);
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

	// add a hashed distribution enforcer
	AddRef();
	pexpr->AddRef();
	if (exprhdl.Pdp())
		GPOS_ASSERT(&exprhdl);
	CDrvdPropPlan *pdpplan = CDrvdPropPlan::Pdpplan(exprhdl.Pdp());
	const CDistributionSpec *pspec = pdpplan->Pds();
	if (pspec->Edt() == CDistributionSpec::EdtUniversal)
		this->MarkReallyDuplicateSensitive();
	CPhysicalMotionRandom *pRandom = GPOS_NEW(pmp) CPhysicalMotionRandom(pmp, this);

	
	CExpression *pexprMotion = GPOS_NEW(pmp) CExpression
										(
										pmp,
										pRandom,
										pexpr
										);
	pdrgpexpr->Append(pexprMotion);		
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
	
	os << this->SzId() << ": [ ";
	
	if (m_fDuplicateSensitive)
	{
		os <<  ", duplicate sensitive";
	}
	
	if (really_duplicate_sensitive)
	{
		os <<  ", really duplicate sensitive";
	}
	else
	{
		os <<  ", not really duplicate sensitive";
	}
	
	
	if (!m_fSatisfiedBySingleton)
	{
		os << ", across-segments";
	}
	
	os <<  " ]";
	
	return os;
}

// EOF

