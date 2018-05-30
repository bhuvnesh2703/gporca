//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		COptimizationContext.h
//
//	@doc:
//		Optimization context object stores properties required to hold
//		on the plan generated by the optimizer
//---------------------------------------------------------------------------
#ifndef GPOPT_COptimizationContext_H
#define GPOPT_COptimizationContext_H

#include "gpos/base.h"

#include "gpopt/base/CReqdPropRelational.h"
#include "gpopt/base/CReqdPropPlan.h"
#include "gpopt/search/CJobQueue.h"
#include "naucrates/statistics/IStatistics.h"
#include "gpos/task/CAutoTraceFlag.h"

#define GPOPT_INVALID_OPTCTXT_ID	ULONG_MAX

namespace gpopt
{
	using namespace gpos;

	// forward declarations
	class CGroup;
	class CGroupExpression;
	class CCostContext;
	class COptimizationContext;
	class CDrvdPropPlan;

	// optimization context pointer definition
	typedef  COptimizationContext * OPTCTXT_PTR;

	// array of optimization contexts
	typedef CDynamicPtrArray<COptimizationContext, CleanupRelease> DrgPoc;

	//---------------------------------------------------------------------------
	//	@class:
	//		COptimizationContext
	//
	//	@doc:
	//		Optimization context
	//
	//---------------------------------------------------------------------------
	class COptimizationContext : public CRefCount
	{

		public:

			// states of optimization context
			enum EState
			{
				estUnoptimized,		// initial state

				estOptimizing,		// ongoing optimization
				estOptimized,		// done optimization

				estSentinel
			};

		private:

			// memory pool
			IMemoryPool *m_memory_pool;

			// private copy ctor
			COptimizationContext(const COptimizationContext &);

			// unique id within owner group, used for debugging
			ULONG m_ulId;

			// back pointer to owner group, used for debugging
			CGroup *m_pgroup;

			// required plan properties
			CReqdPropPlan *m_prpp;

			// required relational properties -- used for stats computation during costing
			CReqdPropRelational *m_prprel;

			// stats of previously optimized expressions
			DrgPstat *m_pdrgpstatCtxt;

			// index of search stage where context is generated
			ULONG m_ulSearchStageIndex;

			// best cost context under the optimization context
			CCostContext *m_pccBest;

			// optimization context state
			EState m_estate;

			// is there a multi-stage Agg plan satisfying required properties
			BOOL m_fHasMultiStageAggPlan;

			// context's optimization job queue
			CJobQueue m_jqOptimization;

			// internal matching function
			BOOL FMatchSortColumns(const COptimizationContext *poc) const;

			// private dummy ctor; used for creating invalid context
			COptimizationContext()
				:
				m_memory_pool(NULL),
				m_ulId(GPOPT_INVALID_OPTCTXT_ID),
				m_pgroup(NULL),
				m_prpp(NULL),
				m_prprel(NULL),
				m_pdrgpstatCtxt(NULL),
				m_ulSearchStageIndex(0),
				m_pccBest(NULL),
				m_estate(estUnoptimized),
				m_fHasMultiStageAggPlan(false)
			{};

			// check if Agg node should be optimized for the given context
			static
			BOOL FOptimizeAgg(IMemoryPool *memory_pool, CGroupExpression *pgexprParent, CGroupExpression *pgexprAgg, COptimizationContext *poc, ULONG ulSearchStages);

			// check if Sort node should be optimized for the given context
			static
			BOOL FOptimizeSort(IMemoryPool *memory_pool, CGroupExpression *pgexprParent, CGroupExpression *pgexprSort, COptimizationContext *poc, ULONG ulSearchStages);

			// check if Motion node should be optimized for the given context
			static
			BOOL FOptimizeMotion(IMemoryPool *memory_pool, CGroupExpression *pgexprParent, CGroupExpression *pgexprMotion, COptimizationContext *poc, ULONG ulSearchStages);

			// check if NL join node should be optimized for the given context
			static
			BOOL FOptimizeNLJoin(IMemoryPool *memory_pool, CGroupExpression *pgexprParent, CGroupExpression *pgexprMotion, COptimizationContext *poc, ULONG ulSearchStages);

		public:

			// ctor
			COptimizationContext
				(
				IMemoryPool *memory_pool,
				CGroup *pgroup,
				CReqdPropPlan *prpp,
				CReqdPropRelational *prprel, // required relational props -- used during stats derivation
				DrgPstat *pdrgpstatCtxt, // stats of previously optimized expressions
				ULONG ulSearchStageIndex
				)
				:
				m_memory_pool(memory_pool),
				m_ulId(GPOPT_INVALID_OPTCTXT_ID),
				m_pgroup(pgroup),
				m_prpp(prpp),
				m_prprel(prprel),
				m_pdrgpstatCtxt(pdrgpstatCtxt),
				m_ulSearchStageIndex(ulSearchStageIndex),
				m_pccBest(NULL),
				m_estate(estUnoptimized),
				m_fHasMultiStageAggPlan(false)
			{
				GPOS_ASSERT(NULL != pgroup);
				GPOS_ASSERT(NULL != prpp);
				GPOS_ASSERT(NULL != prprel);
				GPOS_ASSERT(NULL != pdrgpstatCtxt);
			}

			// dtor
			virtual
			~COptimizationContext();

			// best group expression accessor
			CGroupExpression *PgexprBest() const;

			// match optimization contexts
			BOOL FMatch(const COptimizationContext *poc) const;

			// get id
			ULONG UlId() const
			{
				return m_ulId;
			}

			// group accessor
			CGroup *Pgroup() const
			{
				return m_pgroup;
			}

			// required plan properties accessor
			CReqdPropPlan *Prpp() const
			{
				return m_prpp;
			}

			// required relatoinal properties accessor
			CReqdPropRelational *Prprel() const
			{
				return m_prprel;
			}

			// stats of previously optimized expressions
			DrgPstat *Pdrgpstat() const
			{
				return m_pdrgpstatCtxt;
			}

			// search stage index accessor
			ULONG UlSearchStageIndex() const
			{
				return m_ulSearchStageIndex;
			}

			// best cost context accessor
			CCostContext *PccBest() const
			{
				return m_pccBest;
			}

			// optimization job queue accessor
			CJobQueue *PjqOptimization()
			{
				return &m_jqOptimization;
			}

			// state accessor
			EState Est() const
			{
				return m_estate;
			}

			// is there a multi-stage Agg plan satisfying required properties
			BOOL FHasMultiStageAggPlan() const
			{
				return m_fHasMultiStageAggPlan;
			}

			// set optimization context id
			void SetId
				(
				ULONG ulId
				)
			{
				GPOS_ASSERT(m_ulId == GPOPT_INVALID_OPTCTXT_ID);

				m_ulId = ulId;
			}

			// set optimization context state
			void SetState
				(
				EState estNewState
				)
			{
				GPOS_ASSERT(estNewState == (EState) (m_estate + 1));

				m_estate = estNewState;
			}

			// set best cost context
			void SetBest(CCostContext *pcc);

			// comparison operator for hashtables
			BOOL operator ==
				(
				const COptimizationContext &oc
				)
				const
			{
				return oc.FMatch(this);
			}

			// debug print
			virtual
			IOstream &OsPrint(IOstream &os,
					const CHAR *szPrefix) const;

			// check equality of optimization contexts
			static
			BOOL Equals
				(
				const COptimizationContext &ocLeft,
				const COptimizationContext &ocRight
				)
			{
				return ocLeft == ocRight;
			}

			// hash function for optimization context
			static
			ULONG HashValue
				(
				const COptimizationContext& oc
				)
			{
				GPOS_ASSERT(NULL != oc.Prpp());

				return oc.Prpp()->HashValue();
			}

			// equality function for cost contexts hash table
			static
			BOOL Equals
				(
				const OPTCTXT_PTR &pocLeft,
				const OPTCTXT_PTR &pocRight
				)
			{
				if (pocLeft == m_pocInvalid || pocRight == m_pocInvalid)
				{
					return pocLeft == m_pocInvalid && pocRight == m_pocInvalid;
				}

				return *pocLeft == *pocRight;
			}

			// hash function for cost contexts hash table
			static
			ULONG HashValue
				(
				const OPTCTXT_PTR& poc
				)
			{
				GPOS_ASSERT(m_pocInvalid != poc);

				return HashValue(*poc);
			}

			// hash function used for computing stats during costing
			static
			ULONG UlHashForStats
				(
				const COptimizationContext *poc
				)
			{
				GPOS_ASSERT(m_pocInvalid != poc);

				return HashValue(*poc);
			}

			// equality function used for computing stats during costing
			static
			BOOL FEqualForStats
				(
				const COptimizationContext *pocLeft,
				const COptimizationContext *pocRight
				);

			// return true if given group expression should be optimized under given context
			static
			BOOL FOptimize(IMemoryPool *memory_pool, CGroupExpression *pgexprParent, CGroupExpression *pgexprChild, COptimizationContext *pocChild, ULONG ulSearchStages);

			// compare array of contexts based on context ids
			static
			BOOL FEqualContextIds(DrgPoc *pdrgpocFst, DrgPoc *pdrgpocSnd);

			// compute required properties to CTE producer based on plan properties of CTE consumer
			static
			CReqdPropPlan *PrppCTEProducer(IMemoryPool *memory_pool, COptimizationContext *poc, ULONG ulSearchStages);

			// link for optimization context hash table in CGroup
			SLink m_link;

			// invalid optimization context, needed for hash table iteration
			static
			const COptimizationContext m_ocInvalid;

			// invalid optimization context pointer, needed for cost contexts hash table iteration
			static
			const OPTCTXT_PTR m_pocInvalid;

#ifdef GPOS_DEBUG
			// debug print; for interactive debugging sessions only
			void DbgPrint();
#endif // GPOS_DEBUG


	}; // class COptimizationContext
}


#endif // !GPOPT_COptimizationContext_H

// EOF
