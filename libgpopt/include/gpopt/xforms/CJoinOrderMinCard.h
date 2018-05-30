//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CJoinOrderMinCard.h
//
//	@doc:
//		Cardinality-based join order generation
//---------------------------------------------------------------------------
#ifndef GPOPT_CJoinOrderMinCard_H
#define GPOPT_CJoinOrderMinCard_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/CBitSet.h"
#include "gpos/io/IOstream.h"
#include "gpopt/xforms/CJoinOrder.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CJoinOrderMinCard
	//
	//	@doc:
	//		Helper class for creating join orders based on cardinality of results
	//
	//---------------------------------------------------------------------------
	class CJoinOrderMinCard : public CJoinOrder
	{

		private:

			// result component
			SComponent *m_pcompResult;

			// combine the two given components using applicable edges
			SComponent *PcompCombine(SComponent *pcompOuter, SComponent *pcompInner);

			// mark edges used by result component
			void MarkUsedEdges();

			// derive stats on a given component
			static
			void DeriveStats(IMemoryPool *memory_pool, SComponent *pcomp);

		public:

			// ctor
			CJoinOrderMinCard
				(
				IMemoryPool *memory_pool,
				DrgPexpr *pdrgpexprComponents,
				DrgPexpr *pdrgpexprConjuncts
				);

			// dtor
			virtual
			~CJoinOrderMinCard();

			// main handler
			virtual
			CExpression *PexprExpand();

			// print function
			virtual
			IOstream &OsPrint(IOstream &) const;

	}; // class CJoinOrderMinCard

}

#endif // !GPOPT_CJoinOrderMinCard_H

// EOF
