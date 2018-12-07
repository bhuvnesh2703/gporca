//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformLeftOuterJoin2RightOuterJoin.h
//
//	@doc:
//		Simplify Left Outer Join with constant false predicate
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftOuterJoin2RightOuterJoin_H
#define GPOPT_CXformLeftOuterJoin2RightOuterJoin_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformLeftOuterJoin2RightOuterJoin
	//
	//	@doc:
	//		Simplify Left Outer Join with constant false predicate
	//
	//---------------------------------------------------------------------------
	class CXformLeftOuterJoin2RightOuterJoin : public CXformExploration
	{

		private:

			// private copy ctor
			CXformLeftOuterJoin2RightOuterJoin(const CXformLeftOuterJoin2RightOuterJoin &);

		public:

			// ctor
			explicit
			CXformLeftOuterJoin2RightOuterJoin(IMemoryPool *mp);

			// dtor
			virtual
			~CXformLeftOuterJoin2RightOuterJoin()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfLeftOuterJoin2RightOuterJoin;
			}

			// return a string for xform name
			virtual
			const CHAR *SzId() const
			{
				return "CXformLeftOuterJoin2RightOuterJoin";
			}

			// Compatibility function for simplifying aggregates
			virtual
			BOOL FCompatible
				(
				CXform::EXformId exfid
				)
			{
				return (CXform::ExfLeftOuterJoin2RightOuterJoin != exfid);
			}

			// compute xform promise for a given expression handle
			virtual
			EXformPromise Exfp (CExpressionHandle &exprhdl) const;

			// actual transform
			virtual
			void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const;

	}; // class CXformLeftOuterJoin2RightOuterJoin

}

#endif // !GPOPT_CXformLeftOuterJoin2RightOuterJoin_H

// EOF
