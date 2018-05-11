//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalTVF.h
//
//	@doc:
//		Physical Table-valued function
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalTVF_H
#define GPOPT_CPhysicalTVF_H

#include "gpos/base.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CPhysical.h"

namespace gpopt
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CPhysicalTVF
	//
	//	@doc:
	//		Physical Table-valued function
	//
	//---------------------------------------------------------------------------
	class CPhysicalTVF : public CPhysical
	{
		private:

			// function mdid
			IMDId *m_func_mdid;

			// return type
			IMDId *m_return_type_mdid;

			// function name
			CWStringConst *m_pstr;

			// MD cache info
			const IMDFunction *m_pmdfunc;

			// array of column descriptors: the schema of the function result
			DrgPcoldesc *m_pdrgpcoldesc;

			// output columns
			CColRefSet *m_pcrsOutput;

			// private copy ctor
			CPhysicalTVF(const CPhysicalTVF &);

		public:

			// ctor
			CPhysicalTVF
				(
				IMemoryPool *memory_pool,
				IMDId *mdid_func,
				IMDId *mdid_return_type,
				CWStringConst *str,
				DrgPcoldesc *pdrgpcoldesc,
				CColRefSet *pcrsOutput
				);

			// dtor
			virtual
			~CPhysicalTVF();

			// ident accessors
			virtual
			EOperatorId Eopid() const
			{
				return EopPhysicalTVF;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CPhysicalTVF";
			}

			// function mdid
			IMDId *FuncMdId() const
			{
				return m_func_mdid;
			}

			// return type
			IMDId *ReturnTypeMdId() const
			{
				return m_return_type_mdid;
			}

			// function name
			const CWStringConst *Pstr() const
			{
				return m_pstr;
			}

			// col descr accessor
			DrgPcoldesc *Pdrgpcoldesc() const
			{
				return m_pdrgpcoldesc;
			}

			// accessors
			CColRefSet *PcrsOutput() const
			{
				return m_pcrsOutput;
			}

			// sensitivity to order of inputs
			BOOL FInputOrderSensitive() const;

			// match function
			virtual
			BOOL Matches(COperator *pop) const;

			//-------------------------------------------------------------------------------------
			// Required Plan Properties
			//-------------------------------------------------------------------------------------

			// compute required output columns of the n-th child
			virtual
			CColRefSet *PcrsRequired
				(
				IMemoryPool *memory_pool,
				CExpressionHandle &exprhdl,
				CColRefSet *pcrsRequired,
				ULONG child_index,
				DrgPdp *pdrgpdpCtxt,
				ULONG ulOptReq
				);

			// compute required ctes of the n-th child
			virtual
			CCTEReq *PcteRequired
				(
				IMemoryPool *memory_pool,
				CExpressionHandle &exprhdl,
				CCTEReq *pcter,
				ULONG child_index,
				DrgPdp *pdrgpdpCtxt,
				ULONG ulOptReq
				)
				const;

			// compute required sort order of the n-th child
			virtual
			COrderSpec *PosRequired
				(
				IMemoryPool *memory_pool,
				CExpressionHandle &exprhdl,
				COrderSpec *posRequired,
				ULONG child_index,
				DrgPdp *pdrgpdpCtxt,
				ULONG ulOptReq
				)
				const;

			// compute required distribution of the n-th child
			virtual
			CDistributionSpec *PdsRequired
				(
				IMemoryPool *memory_pool,
				CExpressionHandle &exprhdl,
				CDistributionSpec *pdsRequired,
				ULONG child_index,
				DrgPdp *pdrgpdpCtxt,
				ULONG ulOptReq
				)
				const;

			// compute required rewindability of the n-th child
			virtual
			CRewindabilitySpec *PrsRequired
				(
				IMemoryPool *memory_pool,
				CExpressionHandle &exprhdl,
				CRewindabilitySpec *prsRequired,
				ULONG child_index,
				DrgPdp *pdrgpdpCtxt,
				ULONG ulOptReq
				)
				const;
			
			// compute required partition propagation of the n-th child
			virtual
			CPartitionPropagationSpec *PppsRequired
				(
				IMemoryPool *, //memory_pool,
				CExpressionHandle &, //exprhdl,
				CPartitionPropagationSpec *, //pppsRequired,
				ULONG , //child_index,
				DrgPdp *, //pdrgpdpCtxt,
				ULONG //ulOptReq
				)
			{
				GPOS_ASSERT(!"CPhysicalTVF has no relational children");
				return NULL;
			}

			// check if required columns are included in output columns
			virtual
			BOOL FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired, ULONG ulOptReq) const;

			//-------------------------------------------------------------------------------------
			// Derived Plan Properties
			//-------------------------------------------------------------------------------------

			// derive sort order
			virtual
			COrderSpec *PosDerive(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;

			// derive distribution
			virtual
			CDistributionSpec *PdsDerive(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;

			// derive rewindability
			virtual
			CRewindabilitySpec *PrsDerive(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;

			// derive cte map
			virtual
			CCTEMap *PcmDerive(IMemoryPool *memory_pool, CExpressionHandle &exprhdl) const;

			// derive partition index map
			virtual
			CPartIndexMap *PpimDerive
				(
				IMemoryPool *memory_pool,
				CExpressionHandle &, // exprhdl
				CDrvdPropCtxt * //pdpctxt
				)
				const
			{
				return GPOS_NEW(memory_pool) CPartIndexMap(memory_pool);
			}
			
			// derive partition filter map
			virtual
			CPartFilterMap *PpfmDerive
				(
				IMemoryPool *memory_pool,
				CExpressionHandle & // exprhdl
				)
				const
			{
				// return empty part filter map
				return GPOS_NEW(memory_pool) CPartFilterMap(memory_pool);
			}

			//-------------------------------------------------------------------------------------
			// Enforced Properties
			//-------------------------------------------------------------------------------------

			// return order property enforcing type for this operator
			virtual
			CEnfdProp::EPropEnforcingType EpetOrder
				(
				CExpressionHandle &exprhdl,
				const CEnfdOrder *peo
				)
				const;


			// return rewindability property enforcing type for this operator
			virtual
			CEnfdProp::EPropEnforcingType EpetRewindability
				(
				CExpressionHandle &exprhdl,
				const CEnfdRewindability *per
				)
				const;
			
			// return partition propagation property enforcing type for this operator
			virtual 
			CEnfdProp::EPropEnforcingType EpetPartitionPropagation
				(
				CExpressionHandle &, // exprhdl,
				const CEnfdPartitionPropagation * // pepp
				) 
				const
			{
				return CEnfdProp::EpetRequired;
			}

			// return true if operator passes through stats obtained from children,
			// this is used when computing stats during costing
			virtual
			BOOL FPassThruStats() const
			{
				return false;
			}

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CPhysicalTVF *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopPhysicalTVF == pop->Eopid());

				return dynamic_cast<CPhysicalTVF*>(pop);
			}

	}; // class CPhysicalTVF

}

#endif // !GPOPT_CPhysicalTVF_H

// EOF
