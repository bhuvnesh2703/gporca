//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		CLeftOuterJoinStatsProcessor.h
//
//	@doc:
//		Processor for computing statistics for Left Outer Join
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CLeftOuterJoinStatsProcessor_H
#define GPNAUCRATES_CLeftOuterJoinStatsProcessor_H

#include "naucrates/statistics/CJoinStatsProcessor.h"

namespace gpnaucrates
{
	class CLeftOuterJoinStatsProcessor : public CJoinStatsProcessor
	{
	private:
		// create a new hash map of histograms from the results of the inner join and the histograms of the outer child
		static
		UlongHistogramHashMap *MakeLOJHistogram
				(
				IMemoryPool *memory_pool,
				const CStatistics *outer_stats,
				const CStatistics *inner_side_stats,
				CStatistics *inner_join_stats,
				StatsPredJoinArray *join_preds_stats,
				CDouble num_rows_inner_join,
				CDouble *result_rows_LASJ
				);
		// helper method to add histograms of the inner side of a LOJ
		static
		void AddHistogramsLOJInner
				(
				IMemoryPool *memory_pool,
				const CStatistics *inner_join_stats,
				ULongPtrArray *inner_colids_with_stats,
				CDouble num_rows_LASJ,
				CDouble num_rows_inner_join,
				UlongHistogramHashMap *LOJ_histograms
				);

	public:
		static
		CStatistics *CalcLOJoinStatsStatic
				(
				IMemoryPool *memory_pool,
				const IStatistics *outer_stats,
				const IStatistics *inner_side_stats,
				StatsPredJoinArray *join_preds_stats
				);
	};
}

#endif // !GPNAUCRATES_CLeftOuterJoinStatsProcessor_H

// EOF

