//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformApply2Join.h
//
//	@doc:
//		Base class for transforming Apply to Join
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformApply2Join_H
#define GPOPT_CXformApply2Join_H

#include "gpos/base.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPredicateUtils.h"

#include "gpopt/xforms/CDecorrelator.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformApply2Join
	//
	//	@doc:
	//		Transform Apply into Join by decorrelating the inner side
	//
	//---------------------------------------------------------------------------
	template<class TApply, class TJoin>
	class CXformApply2Join : public CXformExploration
	{

		private:

			// check if we can create a correlated apply expression from the given expression
			static
			BOOL FCanCreateCorrelatedApply(IMemoryPool *, CExpression *pexprApply)
            {
                GPOS_ASSERT(NULL != pexprApply);

                COperator::EOperatorId op_id = pexprApply->Pop()->Eopid();

                // consider only Inner/Outer/Left (Anti) Semi Apply here,
                // correlated left anti semi apply (with ALL/NOT-IN semantics) can only be generated by SubqueryHandler
                return
                COperator::EopLogicalInnerApply == op_id ||
                COperator::EopLogicalLeftOuterApply == op_id ||
                COperator::EopLogicalLeftSemiApply == op_id ||
                COperator::EopLogicalLeftSemiApplyIn == op_id ||
                COperator::EopLogicalLeftAntiSemiApply == op_id;
            }

			// create correlated apply expression
			static
			void CreateCorrelatedApply(IMemoryPool *memory_pool, CExpression *pexprApply, CXformResult *pxfres)
            {
                if (!FCanCreateCorrelatedApply(memory_pool, pexprApply))
                {
                    return;
                }

                CExpression *pexprInner = (*pexprApply)[1];
                CExpression *pexprOuter = (*pexprApply)[0];
                CExpression *pexprScalar = (*pexprApply)[2];

                pexprOuter->AddRef();
                pexprInner->AddRef();
                pexprScalar->AddRef();
                CExpression *pexprResult = NULL;

                TApply *popApply = TApply::PopConvert(pexprApply->Pop());
                ColRefArray *colref_array = popApply->PdrgPcrInner();
                GPOS_ASSERT(NULL != colref_array);
                GPOS_ASSERT(1 == colref_array->Size());

                colref_array->AddRef();
                COperator::EOperatorId eopidSubq = popApply->EopidOriginSubq();
                COperator::EOperatorId op_id = pexprApply->Pop()->Eopid();
                switch (op_id)
                {
                    case COperator::EopLogicalInnerApply:
                        pexprResult = CUtils::PexprLogicalApply<CLogicalInnerCorrelatedApply>(memory_pool, pexprOuter, pexprInner, colref_array, eopidSubq, pexprScalar);
                        break;

                    case COperator::EopLogicalLeftOuterApply:
                        pexprResult = CUtils::PexprLogicalApply<CLogicalLeftOuterCorrelatedApply>(memory_pool, pexprOuter, pexprInner, colref_array, eopidSubq, pexprScalar);
                        break;

                    case COperator::EopLogicalLeftSemiApply:
                        pexprResult = CUtils::PexprLogicalApply<CLogicalLeftSemiCorrelatedApply>(memory_pool, pexprOuter, pexprInner, colref_array, eopidSubq, pexprScalar);
                        break;

                    case COperator::EopLogicalLeftSemiApplyIn:
                        pexprResult = CUtils::PexprLogicalCorrelatedQuantifiedApply<CLogicalLeftSemiCorrelatedApplyIn>(memory_pool, pexprOuter, pexprInner, colref_array, eopidSubq, pexprScalar);
                        break;

                    case COperator::EopLogicalLeftAntiSemiApply:
                        pexprResult = CUtils::PexprLogicalApply<CLogicalLeftAntiSemiCorrelatedApply>(memory_pool, pexprOuter, pexprInner, colref_array, eopidSubq, pexprScalar);
                        break;

                    default:
                        GPOS_ASSERT(!"Unexpected Apply operator");
                        return;
                }

                pxfres->Add(pexprResult);
            }

			// private copy ctor
			CXformApply2Join(const CXformApply2Join &);
			
		protected:

			// helper function to attempt decorrelating Apply's inner child
			static
			BOOL FDecorrelate(IMemoryPool *memory_pool, CExpression *pexprApply, CExpression **ppexprInner, ExpressionArray **ppdrgpexpr)
            {
                GPOS_ASSERT(NULL != pexprApply);
                GPOS_ASSERT(NULL != ppexprInner);
                GPOS_ASSERT(NULL != ppdrgpexpr);

                *ppdrgpexpr = GPOS_NEW(memory_pool) ExpressionArray(memory_pool);

                CExpression *pexprPredicateOrig = (*pexprApply)[2];

                // add original predicate to array
                pexprPredicateOrig->AddRef();
                (*ppdrgpexpr)->Append(pexprPredicateOrig);

                // since properties of inner child have been copied from
                // groups that may had subqueries that were decorrelated later, we reset
                // properties here to allow re-computing them during decorrelation
                (*pexprApply)[1]->ResetDerivedProperties();

                // decorrelate inner child
                if (!CDecorrelator::FProcess(memory_pool, (*pexprApply)[1], false /*fEqualityOnly*/, ppexprInner, *ppdrgpexpr))
                {
                    // decorrelation filed
                    (*ppdrgpexpr)->Release();
                    return false;
                }

                // check for valid semi join correlations
                if ((COperator::EopLogicalLeftSemiJoin == pexprApply->Pop()->Eopid() ||
                     COperator::EopLogicalLeftAntiSemiJoin == pexprApply->Pop()->Eopid()) &&
                    !CPredicateUtils::FValidSemiJoinCorrelations(memory_pool, (*pexprApply)[0], (*ppexprInner), (*ppdrgpexpr))
                    )
                {
                    (*ppdrgpexpr)->Release();

                    return false;
                }

                return true;
            }

			// helper function to decorrelate apply expression and insert alternative into results container
			static
			void Decorrelate(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexprApply)
            {
                GPOS_ASSERT(CUtils::HasOuterRefs((*pexprApply)[1])
                            && "Apply's inner child must have outer references");

                if (CUtils::FHasSubqueryOrApply((*pexprApply)[1]))
                {
                    // Subquery/Apply must be unnested before reaching here
                    return;
                }

                IMemoryPool *memory_pool = pxfctxt->Pmp();
                ExpressionArray *pdrgpexpr = NULL;
                CExpression *pexprInner = NULL;
                if (!FDecorrelate(memory_pool, pexprApply, &pexprInner, &pdrgpexpr))
                {
                    // decorrelation failed, create correlated apply expression if possible
                    CreateCorrelatedApply(memory_pool, pexprApply, pxfres);

                    return;
                }

                // build substitute
                GPOS_ASSERT(NULL != pexprInner);
                (*pexprApply)[0]->AddRef();
                CExpression *pexprOuter = (*pexprApply)[0];
                CExpression *pexprPredicate = CPredicateUtils::PexprConjunction(memory_pool, pdrgpexpr);

                CExpression *pexprResult =
                GPOS_NEW(memory_pool) CExpression
                (
                 memory_pool,
                 GPOS_NEW(memory_pool) TJoin(memory_pool), // join operator
                 pexprOuter,
                 pexprInner,
                 pexprPredicate
                 );

                // add alternative to results
                pxfres->Add(pexprResult);
            }

			// helper function to create a join expression from an apply expression and insert alternative into results container
			static
			void CreateJoinAlternative(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexprApply)
            {
#ifdef GPOS_DEBUG
                CExpressionHandle exprhdl(pxfctxt->Pmp());
                exprhdl.Attach(pexprApply);
                GPOS_ASSERT_IMP(CUtils::HasOuterRefs((*pexprApply)[1]), !exprhdl.GetRelationalProperties(1)->PcrsOuter()->ContainsAll(exprhdl.GetRelationalProperties(0)->PcrsOutput())
                                && "Apply's inner child can only use external columns");
#endif // GPOS_DEBUG

                IMemoryPool *memory_pool = pxfctxt->Pmp();
                CExpression *pexprOuter = (*pexprApply)[0];
                CExpression *pexprInner = (*pexprApply)[1];
                CExpression *pexprPred = (*pexprApply)[2];
                pexprOuter->AddRef();
                pexprInner->AddRef();
                pexprPred->AddRef();
                CExpression *pexprResult =
                GPOS_NEW(memory_pool) CExpression
                (
                 memory_pool,
                 GPOS_NEW(memory_pool) TJoin(memory_pool), // join operator
                 pexprOuter,
                 pexprInner,
                 pexprPred
                 );

                // add alternative to results
                pxfres->Add(pexprResult);
            }

		public:

			// ctor for deep pattern
			explicit
			CXformApply2Join<TApply, TJoin>(IMemoryPool *memory_pool, BOOL )
                :
                // pattern
                CXformExploration
                (
                 GPOS_NEW(memory_pool) CExpression
                 (
                  memory_pool,
                  GPOS_NEW(memory_pool) TApply(memory_pool),
                  GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternLeaf(memory_pool)), // left child
                  GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternTree(memory_pool)), // right child
                  GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternTree(memory_pool)) // predicate
                  )
                 )
            {}

			// ctor for shallow pattern
			explicit
			CXformApply2Join<TApply, TJoin>(IMemoryPool *memory_pool)
                :
                // pattern
                CXformExploration
                (
                 GPOS_NEW(memory_pool) CExpression
                 (
                  memory_pool,
                  GPOS_NEW(memory_pool) TApply(memory_pool),
                  GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternLeaf(memory_pool)), // left child
                  GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternLeaf(memory_pool)), // right child
                  GPOS_NEW(memory_pool) CExpression(memory_pool, GPOS_NEW(memory_pool) CPatternLeaf(memory_pool)) // predicate
                  )
                 )
            {}

			// ctor for passed pattern
			CXformApply2Join<TApply, TJoin>
				(
				IMemoryPool *, // memory_pool
				CExpression *pexprPattern
				)
				:
				CXformExploration(pexprPattern)
			{}

			// dtor
			virtual
			~CXformApply2Join<TApply, TJoin>()
			{}

			// is transformation an Apply decorrelation (Apply To Join) xform?
			virtual
			BOOL FApplyDecorrelating() const
			{
				return true;
			}

	}; // class CXformApply2Join

}

#endif // !GPOPT_CXformApply2Join_H

// EOF
