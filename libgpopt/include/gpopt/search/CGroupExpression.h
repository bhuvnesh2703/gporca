//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2011 EMC Corp.
//
//	@filename:
//		CGroupExpression.h
//
//	@doc:
//		Equivalent of CExpression inside Memo structure
//---------------------------------------------------------------------------
#ifndef GPOPT_CGroupExpression_H
#define GPOPT_CGroupExpression_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CList.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/spinlock.h"
#include "gpopt/base/CCostContext.h"
#include "gpopt/engine/CPartialPlan.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/search/CBinding.h"
#include "gpopt/search/CGroup.h"
#include "gpopt/xforms/CXform.h"

#define GPOPT_INVALID_GEXPR_ID gpos::ulong_max

namespace gpopt
{

	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CGroupExpression
	//
	//	@doc:
	//		Expression representation inside Memo structure
	//
	//---------------------------------------------------------------------------
	class CGroupExpression : public CRefCount
	{

		public:

#ifdef GPOS_DEBUG
			// debug print; for interactive debugging sessions only
			void DbgPrint();
#endif 		// GPOS_DEBUG

			// states of a group expression
			enum EState
			{
				estUnexplored, 		// initial state

				estExploring, 		// ongoing exploration
				estExplored, 		// done exploring

				estImplementing, 	// ongoing implementation
				estImplemented, 	// done implementing

				estSentinel
			};

			// type definition of cost context hash table
			typedef
				CSyncHashtable<
					CCostContext, // entry
					OPTCTXT_PTR, // search key
					CSpinlockCC> ShtCC;

		private:

			// memory pool
			IMemoryPool *m_memory_pool;

			// definition of context hash table accessor
			typedef
				CSyncHashtableAccessByKey<
					CCostContext, // entry
					OPTCTXT_PTR, // search key
					CSpinlockCC> ShtAcc;

			// definition of context hash table iter
			typedef
				CSyncHashtableIter<
					CCostContext, // entry
					OPTCTXT_PTR, // search key
					CSpinlockCC> ShtIter;

			// definition of context hash table iter accessor
			typedef
				CSyncHashtableAccessByIter<
					CCostContext, // entry
					OPTCTXT_PTR, // search key
					CSpinlockCC> ShtAccIter;

			// map of partial plans to their costs
			typedef CHashMap<CPartialPlan, CCost, CPartialPlan::HashValue, CPartialPlan::Equals,
						CleanupRelease<CPartialPlan>, CleanupDelete<CCost> > PartialPlanCostMap;


			// expression id
			ULONG m_id;

			// duplicate group expression
			CGroupExpression *m_pgexprDuplicate;
		
			// operator class
			COperator *m_pop;
			
			// array of child groups
			DrgPgroup *m_pdrgpgroup;
			
			// sorted array of children groups for faster comparison 
			// of order-insensitive operators
			DrgPgroup *m_pdrgpgroupSorted;
			
			// back pointer to group
			CGroup *m_pgroup;
			
			// id of xform that generated group expression
			CXform::EXformId m_exfidOrigin;

			// group expression that generated current group expression via xform
			CGroupExpression *m_pgexprOrigin;

			// flag to indicate if group expression was created as a node at some
			// intermediate level when origin expression was inserted to memo
			BOOL m_fIntermediate;

			// state of group expression
			EState m_estate;

			// optimization level
			EOptimizationLevel m_eol;

			// map of partial plans to their cost lower bound
			PartialPlanCostMap *m_ppartialplancostmap;

			// hashtable of cost contexts
			ShtCC m_sht;

			// set group back pointer
			void SetGroup(CGroup *pgroup);

			// set group expression id
			void SetId(ULONG id);

			// print transformation
			void PrintXform
				(
				IMemoryPool *memory_pool,
				CXform *pxform,
				CExpression *pexpr,
				CXformResult *pxfres,
				ULONG ulNumResults
				);

			// preprocessing before applying transformation
			void PreprocessTransform
				(
				IMemoryPool *pmpLocal,
				IMemoryPool *pmpGlobal,
				CXform *pxform
				);

			// postprocessing after applying transformation
			void PostprocessTransform
				(
				IMemoryPool *pmpLocal,
				IMemoryPool *pmpGlobal,
				CXform *pxform
				);

			// costing scheme
			CCost CostCompute(IMemoryPool *memory_pool, CCostContext *pcc) const;

			// set optimization level of group expression
			void SetOptimizationLevel();

			// check validity of group expression
			BOOL FValidContext(IMemoryPool *memory_pool, COptimizationContext *poc, DrgPoc *pdrgpocChild);
			
			// remove cost context in hash table
			CCostContext *PccRemove(COptimizationContext *poc, ULONG ulOptReq);

			// insert given context in hash table only if a better context does not exist, return the context that is kept it in hash table
			CCostContext *PccInsertBest(CCostContext *pcc);

			// print group expression cost contexts
			IOstream & OsPrintCostContexts(IOstream &os, const CHAR *szPrefix);

			// private copy ctor
			CGroupExpression(const CGroupExpression &);

			//private dummy ctor; used for creating invalid gexpr
			CGroupExpression()
				:
				m_memory_pool(NULL),
				m_id(GPOPT_INVALID_GEXPR_ID),
				m_pop(NULL),
				m_pdrgpgroup(NULL),
				m_pdrgpgroupSorted(NULL),
				m_pgroup(NULL),
				m_exfidOrigin(CXform::ExfInvalid),
				m_pgexprOrigin(NULL),
				m_fIntermediate(false),
				m_estate(estUnexplored),
				m_eol(EolLow),
				m_ppartialplancostmap(NULL)
			{};

						
		public:

			// ctor
			CGroupExpression
				(
				IMemoryPool *memory_pool,
				COperator *pop,
				DrgPgroup *pdrgpgroup,
				CXform::EXformId exfid,
				CGroupExpression *pgexprOrigin,
				BOOL fIntermediate
				);

			// dtor
			~CGroupExpression();
			
			// duplicate group expression accessor
			CGroupExpression *PgexprDuplicate() const
			{
				return m_pgexprDuplicate;
			}

			// set duplicate group expression
			void SetDuplicate
				(
				CGroupExpression *pgexpr
				)
			{
				GPOS_ASSERT(NULL != pgexpr);

				m_pgexprDuplicate = pgexpr;
			}

			// cleanup cost contexts
			void CleanupContexts();

			// check if cost context already exists in group expression hash table
			BOOL FCostContextExists(COptimizationContext *poc, DrgPoc *pdrgpoc);

			// compute and store expression's cost under a given context
			CCostContext *PccComputeCost(IMemoryPool *memory_pool, COptimizationContext *poc, ULONG ulOptReq, DrgPoc *pdrgpoc, BOOL fPruned, CCost costLowerBound);

			// compute a cost lower bound for plans, rooted by current group expression, and satisfying the given required properties
			CCost CostLowerBound(IMemoryPool *memory_pool, CReqdPropPlan *prppInput, CCostContext *pccChild, ULONG child_index);

			// initialize group expression
			void Init(CGroup *pgroup, ULONG id);

			// reset group expression
			void Reset
				(
				CGroup *pgroup,
				ULONG id
				)
			{
				m_pgroup = pgroup;
				m_id = id;
			}

			// optimization level accessor
			EOptimizationLevel Eol() const
			{
				return m_eol;
			}

			// shorthand to access children
			CGroup *operator [] 
				(
				ULONG ulPos
				)
				const
			{
				GPOS_ASSERT(NULL != m_pdrgpgroup);

				CGroup *pgroup = (*m_pdrgpgroup)[ulPos];

				// during optimization, the operator returns the duplicate group;
				// in exploration and implementation the group may contain
				// group expressions that have not been processed yet;
				if (0 == pgroup->UlGExprs())
				{
					GPOS_ASSERT(pgroup->FDuplicateGroup());
					return pgroup->PgroupDuplicate();
				}
				return pgroup;
			};

			// arity function
			ULONG Arity() const
			{
				return m_pdrgpgroup->Size();
			}
			
			// accessor for operator
			COperator *Pop() const
			{
				return m_pop;
			}

			// accessor for id
			ULONG Id() const
			{
				return m_id;
			}

			// accessor for containing group
			CGroup *Pgroup() const
			{
				return m_pgroup;
			}

			// origin xform
			CXform::EXformId ExfidOrigin() const
			{
				return m_exfidOrigin;
			}

			// origin group expression
			CGroupExpression *PgexprOrigin() const
			{
				return m_pgexprOrigin;
			}

			// cost contexts hash table accessor
			ShtCC &Sht()
			{
				return m_sht;
			}

			// comparison operator for hashtables
			BOOL operator == 
				(
				const CGroupExpression &gexpr
				)
				const
			{
				return gexpr.Matches(this);
			}
			
			 // equality function for hash table
			static
			BOOL Equals
				(
				const CGroupExpression &gexprLeft,
				const CGroupExpression &gexprRight
				)
			{
				return gexprLeft == gexprRight;
			}

			// match group expression against given operator and its children
			BOOL Matches(const CGroupExpression *) const;
			
			// match non-scalar children of group expression against given children of passed expression
			BOOL FMatchNonScalarChildren(const CGroupExpression *pgexpr) const;

			// hash function
			ULONG HashValue() const
			{
				return HashValue(m_pop, m_pdrgpgroup);
			}
			
			// static hash function for operator and group references
			static
			ULONG HashValue(COperator *pop, DrgPgroup *drgpgroup);
			
			// static hash function for group expression
			static
			ULONG HashValue(const CGroupExpression&);

			// transform group expression
			void Transform
				(
				IMemoryPool *memory_pool,
				IMemoryPool *pmpLocal,
				CXform *pxform,
				CXformResult *pxfres,
				ULONG *pulElapsedTime
				);

			// set group expression state
			void SetState(EState estNewState);

			// reset group expression state
			void ResetState();

			// check if group expression has been explored
			BOOL FExplored() const
			{
				return (estExplored <= m_estate);
			}

			// check if group expression has been implemented
			BOOL FImplemented() const
			{
				return (estImplemented == m_estate);
			}

			// check if transition to the given state is completed
			BOOL FTransitioned(EState estate) const;

			DrgPgroup *Pdrgpgroup() const
			{
				return m_pdrgpgroup;
			}

			// lookup cost context in hash table
			CCostContext *PccLookup(COptimizationContext *poc, ULONG ulOptReq);

			// lookup all cost contexts matching given optimization context
			DrgPcc *PdrgpccLookupAll(IMemoryPool *memory_pool, COptimizationContext *poc);

			// insert a cost context in hash table
			CCostContext *PccInsert(CCostContext *pcc);

			// derive statistics recursively on a given group expression
			IStatistics *PstatsRecursiveDerive
				(
				IMemoryPool *pmpLocal,
				IMemoryPool *pmpGlobal,
				CReqdPropRelational *prprel,
				StatsArray *stats_ctxt,
				BOOL fComputeRootStats = true
				);

			// print driver
			IOstream &OsPrint(IOstream &os, const CHAR * = "");

			// link for list in Group
			SLink m_linkGroup;

			// link for group expression hash table
			SLink m_linkMemo;

			// invalid group expression
			static
			const CGroupExpression m_gexprInvalid;

	}; // class CGroupExpression
	
}


#endif // !GPOPT_CGroupExpression_H

// EOF
