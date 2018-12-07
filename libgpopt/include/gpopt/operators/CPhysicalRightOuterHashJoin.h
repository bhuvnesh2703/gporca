//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalRightOuterHashJoin.h
//
//	@doc:
//		Left outer hash join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalRightOuterHashJoin_H
#define GPOPT_CPhysicalRightOuterHashJoin_H

#include "gpos/base.h"
#include "gpopt/operators/CPhysicalHashJoin.h"

namespace gpopt
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalRightOuterHashJoin
	//
	//	@doc:
	//		Left outer hash join operator
	//
	//---------------------------------------------------------------------------
	class CPhysicalRightOuterHashJoin : public CPhysicalHashJoin
	{

		private:

			// private copy ctor
			CPhysicalRightOuterHashJoin(const CPhysicalRightOuterHashJoin &);

		public:

			// ctor
			CPhysicalRightOuterHashJoin
				(
				IMemoryPool *mp,
				CExpressionArray *pdrgpexprOuterKeys,
				CExpressionArray *pdrgpexprInnerKeys
				);

			// dtor
			virtual
			~CPhysicalRightOuterHashJoin();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopPhysicalRightOuterHashJoin;
			}

			 // return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CPhysicalRightOuterHashJoin";
			}

			// conversion function
			static
			CPhysicalRightOuterHashJoin *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopPhysicalRightOuterHashJoin == pop->Eopid());

				return dynamic_cast<CPhysicalRightOuterHashJoin*>(pop);
			}


	}; // class CPhysicalRightOuterHashJoin

}

#endif // !GPOPT_CPhysicalRightOuterHashJoin_H

// EOF
