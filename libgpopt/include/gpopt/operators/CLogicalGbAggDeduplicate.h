//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CLogicalGbAggDeduplicate.h
//
//	@doc:
//		Group Aggregate operator for deduplicating join outputs. This is used
//		for example when we transform left semijoin to inner join. E.g. the
//		following expression:
//			LSJ
//			|-- A
//			|-- B
//			+-- A.a1 <> B.b1
//		can be transformed to:
//			GbAggDedup (group on keys of A)
//			+-- InnerJoin
//				|-- A
//				|-- B
//				+-- A.a1 <> B.b1
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalGbAggDeduplicate_H
#define GPOS_CLogicalGbAggDeduplicate_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalGbAgg.h"

namespace gpopt
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalGbAggDeduplicate
	//
	//	@doc:
	//		aggregate operator for deduplicating join outputs
	//
	//---------------------------------------------------------------------------
	class CLogicalGbAggDeduplicate : public CLogicalGbAgg
	{
		private:

			// private copy ctor
			CLogicalGbAggDeduplicate(const CLogicalGbAggDeduplicate &);

			// array of keys from the join's child
			DrgPcr *m_pdrgpcrKeys;

		public:

			// ctor
			explicit
			CLogicalGbAggDeduplicate(IMemoryPool *memory_pool);

			// ctor
			CLogicalGbAggDeduplicate
				(
				IMemoryPool *memory_pool,
				DrgPcr *colref_array,
				COperator::EGbAggType egbaggtype,
				DrgPcr *pdrgpcrKeys = NULL
				);

			// ctor
			CLogicalGbAggDeduplicate
				(
				IMemoryPool *memory_pool,
				DrgPcr *colref_array,
				DrgPcr *pdrgpcrMinimal,
				COperator::EGbAggType egbaggtype,
				DrgPcr *pdrgpcrKeys = NULL
				);

			// dtor
			virtual
			~CLogicalGbAggDeduplicate();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalGbAggDeduplicate;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalGbAggDeduplicate";
			}

			// array of keys from the join's child that needs to be deduped
			DrgPcr *PdrgpcrKeys() const
			{
				return m_pdrgpcrKeys;
			}

			// match function
			virtual
			BOOL Matches(COperator *pop) const;

			// hash function
			virtual
			ULONG HashValue() const;

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns(IMemoryPool *memory_pool, UlongColRefHashMap *colref_mapping, BOOL must_exist);

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive key collections
			virtual
			CKeyCollection *PkcDeriveKeys(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;

			// compute required stats columns of the n-th child
			//-------------------------------------------------------------------------------------
			// Required Relational Properties
			//-------------------------------------------------------------------------------------

			// compute required stat columns of the n-th child
			virtual
			CColRefSet *PcrsStat
				(
				IMemoryPool *memory_pool,
				CExpressionHandle &exprhdl,
				CColRefSet *pcrsInput,
				ULONG child_index
				)
				const;

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			virtual
			CXformSet *PxfsCandidates(IMemoryPool *memory_pool) const;

			// derive statistics
			virtual
			IStatistics *PstatsDerive
						(
						IMemoryPool *memory_pool,
						CExpressionHandle &exprhdl,
						StatsArray *stats_ctxt
						)
						const;

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CLogicalGbAggDeduplicate *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalGbAggDeduplicate == pop->Eopid());

				return dynamic_cast<CLogicalGbAggDeduplicate*>(pop);
			}


			// debug print
			virtual
			IOstream &OsPrint(IOstream &os) const;

	}; // class CLogicalGbAggDeduplicate

}


#endif // !GPOS_CLogicalGbAggDeduplicate_H

// EOF
