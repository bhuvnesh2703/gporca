//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalSequence.h
//
//	@doc:
//		Logical sequence operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalSequence_H
#define GPOPT_CLogicalSequence_H

#include "gpos/base.h"
#include "gpopt/operators/CLogical.h"
#include "gpopt/operators/CExpressionHandle.h"

namespace gpopt
{	
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalSequence
	//
	//	@doc:
	//		Logical sequence operator
	//
	//---------------------------------------------------------------------------
	class CLogicalSequence : public CLogical
	{
		private:

			// private copy ctor
			CLogicalSequence(const CLogicalSequence &);

		public:

			// ctor
			explicit
			CLogicalSequence(IMemoryPool *memory_pool);

			// dtor
			virtual
			~CLogicalSequence() 
			{}

			// ident accessors
			virtual 
			EOperatorId Eopid() const
			{
				return EopLogicalSequence;
			}
			
			// return a string for operator name
			virtual 
			const CHAR *SzId() const
			{
				return "CLogicalSequence";
			}
		
			// match function
			virtual
			BOOL FMatch(COperator *pop) const;


			// sensitivity to order of inputs
			BOOL FInputOrderSensitive() const
			{
				return true;
			}

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns
						(
						IMemoryPool *, //memory_pool,
						HMUlCr *, //phmulcr,
						BOOL //fMustExist
						)
			{
				return PopCopyDefault();
			}

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput(IMemoryPool *memory_pool, CExpressionHandle &exprhdl);

			// dervive keys
			virtual 
			CKeyCollection *PkcDeriveKeys(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;

			// derive max card
			virtual
			CMaxCard Maxcard(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;

			// derive partition consumer info
			virtual
			CPartInfo *PpartinfoDerive(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;

			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint
				(
				IMemoryPool *, //memory_pool,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpcDeriveConstraintPassThru(exprhdl, exprhdl.UlArity() - 1);
			}

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
				ULONG ulChildIndex
				)
				const
			{
				const ULONG ulLastChildIndex = exprhdl.UlArity() - 1;
				if (ulChildIndex == ulLastChildIndex)
				{
					// only pass through the required stats column to the last child since
					// the output of the sequence operator is the output of the last child
					return PcrsStatsPassThru(pcrsInput);
				}

				return GPOS_NEW(memory_pool) CColRefSet(memory_pool);
			}

			// derive statistics
			virtual
			IStatistics *PstatsDerive
				(
				IMemoryPool *, //memory_pool,
				CExpressionHandle &exprhdl,
				DrgPstat * //pdrgpstatCtxt
				)
				const
			{
				// pass through stats from last child
				IStatistics *pstats = exprhdl.Pstats(exprhdl.UlArity() - 1);
				pstats->AddRef();

				return pstats;
			}

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			virtual
			CXformSet *PxfsCandidates(IMemoryPool *memory_pool) const;

			// stat promise
			virtual
			EStatPromise Esp(CExpressionHandle &) const
			{
				return CLogical::EspHigh;
			}

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CLogicalSequence *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalSequence == pop->Eopid());
				
				return dynamic_cast<CLogicalSequence*>(pop);
			}

	}; // class CLogicalSequence

}


#endif // !GPOPT_CLogicalSequence_H

// EOF
