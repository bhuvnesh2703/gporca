//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#ifndef GPOPT_CDistributionSpecForced_H
#define GPOPT_CDistributionSpecForced_H

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecRandom.h"

namespace gpopt
{
    using namespace gpos;

    //---------------------------------------------------------------------------
    //	@class:
    //		CDistributionSpecForced
    //
    //	@doc:
    //		Class for representing forced random distribution.
    //
    //---------------------------------------------------------------------------
    class CDistributionSpecForced : public CDistributionSpecRandom
    {
    public:

        //ctor
        CDistributionSpecForced();

        // accessor
        virtual
        EDistributionType Edt() const
        {
            return CDistributionSpec::EdtForced;
        }

        virtual
        const CHAR *SzId() const
        {
            return "Forced";
        }

        // does this distribution match the given one
        virtual
        BOOL FMatch(const CDistributionSpec *pds) const;

        // does this distribution satisfy the given one
        virtual
        BOOL FSatisfies(const CDistributionSpec *pds) const;
		
		// append enforcers to dynamic array for the given plan properties
		virtual
		void AppendEnforcers(IMemoryPool *pmp, CExpressionHandle &exprhdl, CReqdPropPlan *prpp, DrgPexpr *pdrgpexpr, CExpression *pexpr);
    }; // class CDistributionSpecForced
}

#endif // !GPOPT_CDistributionSpecForced_H

// EOF
