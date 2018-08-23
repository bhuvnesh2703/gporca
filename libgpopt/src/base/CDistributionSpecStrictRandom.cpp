//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#include "gpopt/base/CDistributionSpecStrictRandom.h"

using namespace gpopt;

CDistributionSpecStrictRandom::CDistributionSpecStrictRandom
	(
	BOOL is_enforced_by_motion
	)
	:
	CDistributionSpecRandom(is_enforced_by_motion)
{
}

BOOL CDistributionSpecStrictRandom::Matches(const CDistributionSpec *pds) const
{
    return pds->Edt() == Edt();
}

BOOL CDistributionSpecStrictRandom::FSatisfies(const CDistributionSpec *pds) const
{
    return Matches(pds) || pds->Edt() == EdtAny;
}
