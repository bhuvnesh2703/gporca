//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformExpandNAryJoinDynamicProgrammingMinCard.h
//
//	@doc:
//		Expand n-ary join into series of binary joins using dynamic
//		programming
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformExpandNAryJoinDynamicProgrammingMinCard_H
#define GPOPT_CXformExpandNAryJoinDynamicProgrammingMinCard_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformExpandNAryJoinDynamicProgrammingMinCard
	//
	//	@doc:
	//		Expand n-ary join into series of binary joins using dynamic
	//		programming
	//
	//---------------------------------------------------------------------------
	class CXformExpandNAryJoinDynamicProgrammingMinCard : public CXformExploration
	{

		private:

			// private copy ctor
			CXformExpandNAryJoinDynamicProgrammingMinCard(const CXformExpandNAryJoinDynamicProgrammingMinCard &);

		public:

			// ctor
			explicit
			CXformExpandNAryJoinDynamicProgrammingMinCard(IMemoryPool *mp);

			// dtor
			virtual
			~CXformExpandNAryJoinDynamicProgrammingMinCard()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfExpandNAryJoinDynamicProgrammingMinCard;
			}

			// return a string for xform name
			virtual
			const CHAR *SzId() const
			{
				return "CXformExpandNAryJoinDynamicProgrammingMinCard";
			}

			// compute xform promise for a given expression handle
			virtual
			EXformPromise Exfp(CExpressionHandle &exprhdl) const;

			// do stats need to be computed before applying xform?
			virtual
			BOOL FNeedsStats() const
			{
				return true;
			}

			// actual transform
			void Transform
					(
					CXformContext *pxfctxt,
					CXformResult *pxfres,
					CExpression *pexpr
					) const;

	}; // class CXformExpandNAryJoinDynamicProgrammingMinCard

}


#endif // !GPOPT_CXformExpandNAryJoinDynamicProgrammingMinCard_H

// EOF
