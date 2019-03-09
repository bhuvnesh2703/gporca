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

			const BOOL m_allow_subqueries;
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
			m_allow_subqueries(false)
			{}
		
			// ctor
			explicit
			CPatternTree
				(
				IMemoryPool *mp,
				BOOL allow_subqueries
				)
				: 
				CPattern(mp),
				m_allow_subqueries(allow_subqueries)
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
			BOOL AllowSubqueries()
			{
				return m_allow_subqueries;
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
