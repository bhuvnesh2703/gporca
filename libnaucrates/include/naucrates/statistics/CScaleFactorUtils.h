//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2014 Pivotal Inc.
//
//	@filename:
//		CScaleFactorUtils.h
//
//	@doc:
//		Utility functions for computing scale factors used in stats computation
//---------------------------------------------------------------------------
#ifndef GPOPT_CScaleFactorUtils_H
#define GPOPT_CScaleFactorUtils_H

#include "gpos/base.h"
#include "gpopt/engine/CStatisticsConfig.h"

#include "naucrates/statistics/CHistogram.h"
#include "naucrates/statistics/CStatisticsUtils.h"

namespace gpnaucrates
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CScaleFactorUtils
	//
	//	@doc:
	//		Utility functions for computing scale factors used in stats computation
	//
	//---------------------------------------------------------------------------
	class CScaleFactorUtils
	{
		public:

			struct SOIDPair
			{
				// mdid of the outer table
				IMDId *m_mdid_outer;

				// mdid of the inner table
				IMDId *m_mdid_inner;

				//ctor
				SOIDPair
				(
				 IMDId *mdid_outer,
				 IMDId *mdid_inner
				)
				:
				m_mdid_outer(mdid_outer),
				m_mdid_inner(mdid_inner)
				{}

				// hash map requirements
				static
				ULONG HashValue(const SOIDPair *oid_pair)
				{
					return CombineHashes(oid_pair->m_mdid_outer->HashValue(),oid_pair->m_mdid_inner->HashValue());
				}
				static
				BOOL Equals(const SOIDPair *first,
							const SOIDPair *second)
				{ return (first->m_mdid_outer == second->m_mdid_outer) && (first->m_mdid_inner == second->m_mdid_inner); }
			};

			struct SJoinCondition
			{
				// scale factor
				CDouble m_scale_factor;

				// mdid pair for the predicate
				SOIDPair m_oid_pair;

				//ctor
				SJoinCondition
				(
				 CDouble scale_factor,
				 IMDId *mdid_outer,
				 IMDId *mdid_inner
				 )
				:
				m_scale_factor(scale_factor),
				m_oid_pair(SOIDPair(mdid_outer, mdid_inner))
				{}
			};

			typedef CDynamicPtrArray<SJoinCondition, CleanupDelete> SJoinConditionArray;

			typedef CHashMap<SOIDPair, CDoubleArray, SOIDPair::HashValue, SOIDPair::Equals, CleanupDelete<SOIDPair>, CleanupRelease<CDoubleArray> > OIDPairToScaleFactorArrayMap;
		
			// calculate the cumulative join scaling factor
			static
			CDouble CumulativeJoinScaleFactor(CMemoryPool *mp, const CStatisticsConfig *stats_config, SJoinConditionArray *join_conds_scale_factors);

			// return scaling factor of the join predicate after apply damping
			static
			CDouble DampedJoinScaleFactor(const CStatisticsConfig *stats_config, ULONG num_columns);

			// return scaling factor of the filter after apply damping
			static
			CDouble DampedFilterScaleFactor(const CStatisticsConfig *stats_config, ULONG num_columns);

			// return scaling factor of the group by predicate after apply damping
			static
			CDouble DampedGroupByScaleFactor(const CStatisticsConfig *stats_config, ULONG num_columns);

			// sort the array of scaling factor
			static
			void SortScalingFactor(CDoubleArray *scale_factors, BOOL is_descending);

			// calculate the cumulative scaling factor for conjunction after applying damping multiplier
			static
			CDouble CalcScaleFactorCumulativeConj(const CStatisticsConfig *stats_config, CDoubleArray *scale_factors);

			// calculate the cumulative scaling factor for disjunction after applying damping multiplier
			static
			CDouble CalcScaleFactorCumulativeDisj(const CStatisticsConfig *stats_config, CDoubleArray *scale_factors, CDouble tota_rows);

			// comparison function in descending order
			static
			INT DescendingOrderCmpFunc(const void *val1, const void *val2);

			static
			INT DescendingOrderCmpJoinFunc(const void *val1, const void *val2);

			// comparison function in ascending order
			static
			INT AscendingOrderCmpFunc(const void *val1, const void *val2);

			// helper function for double comparison
			static
			INT DoubleCmpFunc(const CDouble *double_data1, const CDouble *double_data2, BOOL is_descending);

			// default scaling factor of LIKE predicate
			static
			const CDouble DDefaultScaleFactorLike;

			// default scaling factor of join predicate
			static
			const CDouble DefaultJoinPredScaleFactor;

			// default scaling factor of non-equality (<, <=, >=, <=) join predicate
			// Note: scale factor of InEquality (!= also denoted as <>) is computed from scale factor of equi-join
			static
			const CDouble DefaultInequalityJoinPredScaleFactor;

			// invalid scale factor
			static
			const CDouble InvalidScaleFactor;
	}; // class CScaleFactorUtils
}

#endif // !GPOPT_CScaleFactorUtils_H

// EOF
