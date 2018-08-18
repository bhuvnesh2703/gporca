//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#include "gpopt/base/CDistributionSpecStrictRandom.h"

using namespace gpopt;

CDistributionSpecStrictRandom::CDistributionSpecStrictRandom()
{
}

BOOL CDistributionSpecStrictRandom::Matches(const CDistributionSpec *pds) const
{
	if (pds->Edt() == Edt())
	{
		return true;
	}

	return false;
}

BOOL CDistributionSpecStrictRandom::FSatisfies(const CDistributionSpec *pds) const
{
    return Matches(pds);
}
