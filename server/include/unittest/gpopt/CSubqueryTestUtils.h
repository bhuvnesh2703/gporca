//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSubqueryTestUtils.h
//
//	@doc:
//		Optimizer test utility functions for tests requiring subquery
//		expressions
//---------------------------------------------------------------------------
#ifndef GPOPT_CSubqueryTestUtils_H
#define GPOPT_CSubqueryTestUtils_H

#include "gpos/base.h"
#include "unittest/gpopt/CTestUtils.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CSubqueryTestUtils
	//
	//	@doc:
	//		Test utility functions for tests requiring subquery expressions
	//
	//---------------------------------------------------------------------------
	class CSubqueryTestUtils
	{

		public:

			//-------------------------------------------------------------------
			// Helpers for generating expressions
			//-------------------------------------------------------------------
						
			// helper to generate a pair of random Get expressions
			static
			void GenerateGetExpressions(IMemoryPool *memory_pool, CExpression **ppexprOuter, CExpression **ppexprInner);

			// generate a random join expression with a subquery predicate
			static
			CExpression *PexprJoinWithAggSubquery(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a select  expression with subquery equality predicate
			static
			CExpression *PexprSelectWithAggSubquery(IMemoryPool *memory_pool, CExpression *pexprOuter, CExpression *pexprInner, BOOL fCorrelated);

			// generate a Select expression with a subquery equality predicate involving constant
			static
			CExpression *PexprSelectWithAggSubqueryConstComparison(IMemoryPool *memory_pool, CExpression *pexprOuter, CExpression *pexprInner, BOOL fCorrelated);

			// generate a project  expression with subquery equality predicate
			static
			CExpression *PexprProjectWithAggSubquery(IMemoryPool *memory_pool, CExpression *pexprOuter, CExpression *pexprInner, BOOL fCorrelated);

			// generate a random select expression with a subquery predicate
			static
			CExpression *PexprSelectWithAggSubquery(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random select expression with a subquery predicate involving constant
			static
			CExpression *PexprSelectWithAggSubqueryConstComparison(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random project expression with a subquery in project element
			static
			CExpression *PexprProjectWithAggSubquery(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random select expression with a subquery over join predicate
			static
			CExpression *PexprSelectWithAggSubqueryOverJoin(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random select expression with ANY subquery predicate
			static
			CExpression *PexprSelectWithAnySubquery(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random select expression with ANY subquery predicate over window operation
			static
			CExpression *PexprSelectWithAnySubqueryOverWindow(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random select expression with Any subquery whose inner expression is a GbAgg
			static
			CExpression *PexprSelectWithAnyAggSubquery(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random project expression with ANY subquery
			static
			CExpression *PexprProjectWithAnySubquery(IMemoryPool *memory_pool,  BOOL fCorrelated);

			// generate a random select expression with ALL subquery predicate
			static
			CExpression *PexprSelectWithAllSubquery(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random select expression with ALL subquery predicate over window operation
			static
			CExpression *PexprSelectWithAllSubqueryOverWindow(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random select expression with Any subquery whose inner expression is a GbAgg
			static
			CExpression *PexprSelectWithAllAggSubquery(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random project expression with ALL subquery
			static
			CExpression *PexprProjectWithAllSubquery(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random select expression with EXISTS subquery predicate
			static
			CExpression *PexprSelectWithExistsSubquery(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random select expression with trimmable EXISTS subquery predicate
			static
			CExpression *PexprSelectWithTrimmableExists(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random select expression with trimmable NOT EXISTS subquery predicate
			static
			CExpression *PexprSelectWithTrimmableNotExists(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random select expression with NOT EXISTS subquery predicate
			static
			CExpression *PexprSelectWithNotExistsSubquery(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random project expression with EXISTS subquery
			static
			CExpression *PexprProjectWithExistsSubquery(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random project expression with NOT EXISTS subquery
			static
			CExpression *PexprProjectWithNotExistsSubquery(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random select expression with nested comparisons involving subqueries
			static
			CExpression *PexprSelectWithNestedCmpSubquery(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate randomized select expression with comparison between two subqueries
			static
			CExpression *PexprSelectWithCmpSubqueries(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random select expression with nested agg subqueries
			static
			CExpression *PexprSelectWithNestedSubquery(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random select expression with nested All subqueries
			static
			CExpression *PexprSelectWithNestedAllSubqueries(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random select expression with nested Any subqueries
			static
			CExpression *PexprSelectWithNestedAnySubqueries(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random select expression with 2-levels correlated subqueries
			static
			CExpression *PexprSelectWith2LevelsCorrSubquery(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random select expression with subquery predicates in an AND tree
			static
			CExpression *PexprSelectWithSubqueryConjuncts(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random select expression with subquery predicates in an OR tree
			static
			CExpression *PexprSelectWithSubqueryDisjuncts(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a random project expression with subqueries
			static
			CExpression *PexprProjectWithSubqueries(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate a randomized Select expression to be used for building subquery examples
			static
			CExpression *PexprSubquery(IMemoryPool *memory_pool, CExpression *pexprOuter, CExpression *pexprInner, BOOL fCorrelated);

			// generate a quantified subquery expression
			static
			CExpression *PexprSubqueryQuantified(IMemoryPool *memory_pool, COperator::EOperatorId eopid, CExpression *pexprOuter, CExpression *pexprInner, BOOL fCorrelated);

			// generate quantified subquery expression over window operations
			static
			CExpression *PexprSelectWithSubqueryQuantifiedOverWindow(IMemoryPool *memory_pool, COperator::EOperatorId eopid, BOOL fCorrelated);

			// generate existential subquery expression
			static
			CExpression *PexprSubqueryExistential(IMemoryPool *memory_pool, COperator::EOperatorId eopid, CExpression *pexprOuter, CExpression *pexprInner, BOOL fCorrelated);

			// generate a ScalarSubquery aggregate expression
			static
			CExpression *PexprSubqueryAgg(IMemoryPool *memory_pool, CExpression *pexprOuter, CExpression *pexprInner, BOOL fCorrelated);

			// generate a random select expression with nested quantified subqueries
			static
			CExpression *PexprSelectWithNestedQuantifiedSubqueries(IMemoryPool *memory_pool, COperator::EOperatorId eopid, BOOL fCorrelated);

			// generate randomized quantified subquery expression
			static
			CExpression *PexprSelectWithSubqueryQuantified(IMemoryPool *memory_pool, COperator::EOperatorId eopid, BOOL fCorrelated);

			// generate randomized quantified subquery expression whose inner expression is a Gb
			static
			CExpression *PexprSelectWithQuantifiedAggSubquery(IMemoryPool *memory_pool, COperator::EOperatorId eopid, BOOL fCorrelated);

			// generate randomized Project expression with quantified subquery
			static
			CExpression *PexprProjectWithSubqueryQuantified(IMemoryPool *memory_pool, CExpression *pexprOuter, CExpression *pexprInner, COperator::EOperatorId eopid, BOOL fCorrelated);

			// generate randomized Select with existential subquery expression
			static
			CExpression *PexprSelectWithSubqueryExistential(IMemoryPool *memory_pool, COperator::EOperatorId eopid, BOOL fCorrelated);

			// generate randomized Select expression with existential subquery predicate that can be trimmed
			static
			CExpression *PexprSelectWithTrimmableExistentialSubquery(IMemoryPool *memory_pool, COperator::EOperatorId eopid, BOOL fCorrelated);

			// generate randomized Project with existential subquery expression
			static
			CExpression *PexprProjectWithSubqueryExistential(IMemoryPool *memory_pool, COperator::EOperatorId eopid, BOOL fCorrelated);

			// generate a Select expression with an AND predicate tree involving subqueries
			static
			CExpression *PexprSelectWithSubqueryBoolOp(IMemoryPool *memory_pool, CExpression *pexprOuter, CExpression *pexprInner, BOOL fCorrelated, CScalarBoolOp::EBoolOperator);

			// generate a Project expression with multiple subqueries
			static
			CExpression *PexprProjectWithSubqueries(IMemoryPool *memory_pool, CExpression *pexprOuter, CExpression *pexprInner, BOOL fCorrelated);

			// generate a Select expression with ANY/ALL subquery predicate over a const table get
			static
			CExpression *PexprSubqueryWithConstTableGet(IMemoryPool *memory_pool, COperator::EOperatorId eopid);

			// generate a Select expression with a disjunction of two ANY subqueries
			static
			CExpression *PexprSubqueryWithDisjunction(IMemoryPool *memory_pool);

			// generate an expression with subquery in null test context
			static
			CExpression *PexprSubqueriesInNullTestContext(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate an expression with subqueries in both value and filter contexts
			static
			CExpression *PexprSubqueriesInDifferentContexts(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate an expression with undecorrelatable quantified subquery
			static
			CExpression *PexprUndecorrelatableSubquery(IMemoryPool *memory_pool, COperator::EOperatorId eopid, BOOL fCorrelated);

			// generate an expression with undecorrelatable ANY subquery
			static
			CExpression *PexprUndecorrelatableAnySubquery(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate an expression with undecorrelatable ALL subquery
			static
			CExpression *PexprUndecorrelatableAllSubquery(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate an expression with undecorrelatable exists subquery
			static
			CExpression *PexprUndecorrelatableExistsSubquery(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate an expression with undecorrelatable NotExists subquery
			static
			CExpression *PexprUndecorrelatableNotExistsSubquery(IMemoryPool *memory_pool, BOOL fCorrelated);

			// generate an expression with undecorrelatable Scalar subquery
			static
			CExpression *PexprUndecorrelatableScalarSubquery(IMemoryPool *memory_pool, BOOL fCorrelated);

	}; // class CSubqueryTestUtils

} // namespace gpopt

#endif // !GPOPT_CSubqueryTestUtils_H

// EOF
