//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.


#ifndef GPOPT_CDistributionSpecHashedNoOp_H
#define GPOPT_CDistributionSpecHashedNoOp_H

#include "gpopt/base/CDistributionSpecHashed.h"

namespace gpopt
{
	class CDistributionSpecHashedNoOp : public CDistributionSpecHashed
	{
		public:
			CDistributionSpecHashedNoOp
			(
			DrgPexpr *pdrgpexr
			);

			virtual EDistributionType Edt() const;

			virtual BOOL FMatch(const CDistributionSpec *pds) const;

			virtual const CHAR *SzId() const
			{
				return "HASHED NO-OP";
			}

			virtual void
			AppendEnforcers
			(
			IMemoryPool *memory_pool,
			CExpressionHandle &exprhdl,
			CReqdPropPlan *prpp,
			DrgPexpr *pdrgpexpr,
			CExpression *pexpr
			);
	};
}

#endif
