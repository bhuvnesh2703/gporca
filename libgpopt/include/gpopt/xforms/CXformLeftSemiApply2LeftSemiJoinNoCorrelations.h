//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformLeftSemiApply2LeftSemiJoinNoCorrelations.h
//
//	@doc:
//		Turn LS apply into LS join when inner child has no outer references
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftSemiApply2LeftSemiJoinNoCorrelations_H
#define GPOPT_CXformLeftSemiApply2LeftSemiJoinNoCorrelations_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformApply2Join.h"
#include "gpopt/operators/ops.h"


namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformLeftSemiApply2LeftSemiJoinNoCorrelations
	//
	//	@doc:
	//		Transform Apply into Join by decorrelating the inner side
	//
	//---------------------------------------------------------------------------
	class CXformLeftSemiApply2LeftSemiJoinNoCorrelations : public CXformApply2Join<CLogicalLeftSemiApply, CLogicalLeftSemiJoin>
	{

		private:

			// private copy ctor
			CXformLeftSemiApply2LeftSemiJoinNoCorrelations(const CXformLeftSemiApply2LeftSemiJoinNoCorrelations &);

		public:

			// ctor
			explicit
			CXformLeftSemiApply2LeftSemiJoinNoCorrelations
				(
				IMemoryPool *memory_pool
				)
				:
				CXformApply2Join<CLogicalLeftSemiApply, CLogicalLeftSemiJoin>(memory_pool)
			{}

			// dtor
			virtual
			~CXformLeftSemiApply2LeftSemiJoinNoCorrelations()
			{}

			// ctor with a passed pattern
			CXformLeftSemiApply2LeftSemiJoinNoCorrelations
				(
				IMemoryPool *memory_pool,
				CExpression *pexprPattern
				)
				:
				CXformApply2Join<CLogicalLeftSemiApply, CLogicalLeftSemiJoin>(memory_pool, pexprPattern)
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfLeftSemiApply2LeftSemiJoinNoCorrelations;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CXformLeftSemiApply2LeftSemiJoinNoCorrelations";
			}

			// compute xform promise for a given expression handle
			virtual
			EXformPromise Exfp(CExpressionHandle &exprhdl) const;

			// actual transform
			void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const;


	}; // class CXformLeftSemiApply2LeftSemiJoinNoCorrelations

}

#endif // !GPOPT_CXformLeftSemiApply2LeftSemiJoinNoCorrelations_H

// EOF
