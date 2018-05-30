//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		CLimitStatsProcessor.h
//
//	@doc:
//		Compute statistics for limit operation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CLimitStatsProcessor_H
#define GPNAUCRATES_CLimitStatsProcessor_H

#include "gpopt/operators/ops.h"
#include "gpopt/optimizer/COptimizerConfig.h"

#include "naucrates/statistics/CStatisticsUtils.h"

namespace gpnaucrates
{
	using namespace gpos;

	class CLimitStatsProcessor
	{
		public:

		// limit
		static
		CStatistics *PstatsLimit(IMemoryPool *memory_pool, const CStatistics *pstatsInput, CDouble dLimitRows);
	};
}

#endif // !GPNAUCRATES_CLimitStatsProcessor_H

// EOF

