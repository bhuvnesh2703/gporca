//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CPatternTree.h
//
//	@doc:
//		Pattern that matches entire expression trees
//---------------------------------------------------------------------------
#ifndef GPOPT_CPatternTree_H
#define GPOPT_CPatternTree_H

#include "gpos/base.h"
#include "gpopt/operators/CPattern.h"

namespace gpopt
{
	using namespace gpos;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CPatternTree
	//
	//	@doc:
	//		Pattern that matches entire expression trees, e.g. scalar expressions
	//
	//---------------------------------------------------------------------------
	class CPatternTree : public CPattern
	{

		private:

			// should we extract all the aternatives below subquery
			const BOOL m_exhaust_subquery_childs;

			// private copy ctor
			CPatternTree(const CPatternTree &);

		public:
		
			explicit
			CPatternTree
			(
			 IMemoryPool *mp
			 )
			:
			CPattern(mp),
			m_exhaust_subquery_childs(true)
			{}

			// ctor
			explicit
			CPatternTree
				(
				IMemoryPool *mp,
				BOOL exhaust_subquery_childs
				)
				: 
				CPattern(mp),
				m_exhaust_subquery_childs(exhaust_subquery_childs)
			{}

			// dtor
			virtual 
			~CPatternTree() {}
			
			// check if operator is a pattern leaf
			virtual
			BOOL FLeaf() const
			{
				return false;
			}

			// ident accessors
			virtual 
			EOperatorId Eopid() const
			{
				return EopPatternTree;
			}
			
			// return a string for operator name
			virtual 
			const CHAR *SzId() const
			{
				return "CPatternTree";
			}

			virtual
			BOOL ExhaustSubqueryChilds()
			{
				return m_exhaust_subquery_childs;
			}

			static
			CPatternTree *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(pop->FPattern());

				return dynamic_cast<CPatternTree*>(pop);
			}

	}; // class CPatternTree

}


#endif // !GPOPT_CPatternTree_H

// EOF
