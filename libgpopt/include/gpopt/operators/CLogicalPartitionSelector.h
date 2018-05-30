//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CLogicalPartitionSelector.h
//
//	@doc:
//		Logical partition selector operator. This is used for DML
//		on partitioned tables
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalPartitionSelector_H
#define GPOPT_CLogicalPartitionSelector_H

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CLogical.h"


namespace gpopt
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalPartitionSelector
	//
	//	@doc:
	//		Logical partition selector operator
	//
	//---------------------------------------------------------------------------
	class CLogicalPartitionSelector : public CLogical
	{
		private:

			// mdid of partitioned table
			IMDId *m_mdid;

			// filter expressions corresponding to various levels
			DrgPexpr *m_pdrgpexprFilters;

			// oid column - holds the OIDs for leaf parts
			CColRef *m_pcrOid;

			// private copy ctor
			CLogicalPartitionSelector(const CLogicalPartitionSelector &);

		public:

			// ctors
			explicit
			CLogicalPartitionSelector(IMemoryPool *memory_pool);

			CLogicalPartitionSelector
				(
				IMemoryPool *memory_pool,
				IMDId *pmdid,
				DrgPexpr *pdrgpexprFilters,
				CColRef *pcrOid
				);

			// dtor
			virtual
			~CLogicalPartitionSelector();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalPartitionSelector;
			}

			// operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalPartitionSelector";
			}

			// partitioned table mdid
			IMDId *MDId() const
			{
				return m_mdid;
			}

			// oid column
			CColRef *PcrOid() const
			{
				return m_pcrOid;
			}

			// number of partitioning levels
			ULONG UlPartLevels() const
			{
				return m_pdrgpexprFilters->Size();
			}

			// filter expression for a given level
			CExpression *PexprPartFilter(ULONG ulLevel) const
			{
				return (*m_pdrgpexprFilters)[ulLevel];
			}

			// match function
			virtual
			BOOL FMatch(COperator *pop) const;

			// hash function
			virtual
			ULONG HashValue() const;

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const
			{
				// operator has one child
				return false;
			}

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns(IMemoryPool *memory_pool, HMUlCr *phmulcr, BOOL fMustExist);

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput(IMemoryPool *memory_pool, CExpressionHandle &exprhdl);

			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint
				(
				IMemoryPool *, //memory_pool,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpcDeriveConstraintPassThru(exprhdl, 0 /*ulChild*/);
			}

			// derive max card
			virtual
			CMaxCard Maxcard(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;

			// derive partition consumer info
			virtual
			CPartInfo *PpartinfoDerive
				(
				IMemoryPool *, // memory_pool,
				CExpressionHandle &exprhdl
				)
				const
			{
				return PpartinfoPassThruOuter(exprhdl);
			}

			// compute required stats columns of the n-th child
			virtual
			CColRefSet *PcrsStat
				(
				IMemoryPool *,// memory_pool
				CExpressionHandle &,// exprhdl
				CColRefSet *pcrsInput,
				ULONG // ulChildIndex
				)
				const
			{
				return PcrsStatsPassThru(pcrsInput);
			}

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			virtual
			CXformSet *PxfsCandidates(IMemoryPool *memory_pool) const;

			// derive key collections
			virtual
			CKeyCollection *PkcDeriveKeys
				(
				IMemoryPool *, // memory_pool
				CExpressionHandle &exprhdl
				)
				const
			{
				return PkcDeriveKeysPassThru(exprhdl, 0 /* ulChild */);
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
				return PstatsPassThruOuter(exprhdl);
			}

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
			CLogicalPartitionSelector *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalPartitionSelector == pop->Eopid());

				return dynamic_cast<CLogicalPartitionSelector*>(pop);
			}

			// debug print
			virtual
			IOstream &OsPrint(IOstream &) const;

	}; // class CLogicalPartitionSelector

}

#endif // !GPOPT_CLogicalPartitionSelector_H

// EOF
