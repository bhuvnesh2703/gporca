//	Greenplum Database
//	Copyright (C) 2018 Pivotal Software, Inc.

#ifndef GPOPT_CDistributionSpecExplicitRandom_H
#define GPOPT_CDistributionSpecExplicitRandom_H

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecRandom.h"

namespace gpopt
{
    using namespace gpos;

    //---------------------------------------------------------------------------
    //	@class:
    //		CDistributionSpecExplicitRandom
    //
    //	@doc:
    //		Class for representing explicit random distribution.
    //
    //---------------------------------------------------------------------------
    class CDistributionSpecExplicitRandom : public CDistributionSpecRandom
    {
    public:

        //ctor
        CDistributionSpecExplicitRandom();

        // accessor
        virtual
        EDistributionType Edt() const
        {
            return CDistributionSpec::EdtExplicitRandom;
        }

        virtual
        const CHAR *SzId() const
        {
            return "ExplicitRandom";
        }

		// return true if distribution spec can be derived
		virtual
		BOOL FDerivable() const
		{
			return false;
		}

		// does this distribution match the given one
		virtual
		BOOL Matches(const CDistributionSpec *pds) const;

		// does current distribution satisfy the given one
		virtual
		BOOL FSatisfies(const CDistributionSpec *pds) const;
    }; // class CDistributionSpecExplicitRandom
}

#endif // !GPOPT_CDistributionSpecExplicitRandom_H

// EOF
