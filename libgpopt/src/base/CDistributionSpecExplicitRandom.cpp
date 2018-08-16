//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal Software, Inc.
//---------------------------------------------------------------------------

#include "naucrates/traceflags/traceflags.h"
#include "gpopt/operators/CPhysicalMotionRandom.h"
#include "gpopt/base/CDistributionSpecExplicitRandom.h"

using namespace gpopt;

CDistributionSpecExplicitRandom::CDistributionSpecExplicitRandom()
	:
	CDistributionSpecRandom(CDistributionSpecRandom::EsoSentinel)
{}

BOOL
CDistributionSpecExplicitRandom::Matches
	(
	const CDistributionSpec *pds
	)
const
{
	return Edt() == pds->Edt();
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
CDistributionSpecExplicitRandom::FSatisfies
	(
	const CDistributionSpec *pds
	)
const
{
	return Matches(pds);
}
