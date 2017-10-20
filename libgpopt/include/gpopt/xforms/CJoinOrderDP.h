//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CJoinOrderDP.h
//
//	@doc:
//		Dynamic programming-based join order generation
//---------------------------------------------------------------------------
#ifndef GPOPT_CJoinOrderDP_H
#define GPOPT_CJoinOrderDP_H

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpos/io/IOstream.h"
#include "gpopt/xforms/CJoinOrder.h"


namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CJoinOrderDP
	//
	//	@doc:
	//		Helper class for creating join orders using dynamic programming
	//
	//---------------------------------------------------------------------------
	class CJoinOrderDP : public CJoinOrder
	{

		protected:

			class CJoinSet: public CRefCount
		
				{
					private:
					
						CBitSet *m_pbsLeftChild;
						CBitSet *m_pbsRightChild;
					
					public:
					
						CJoinSet
						(
							CBitSet *pbsLeftChild,
							CBitSet *pbsRightChild
						)
						:
						m_pbsLeftChild(pbsLeftChild),
						m_pbsRightChild(pbsRightChild)
						{
							m_pbsLeftChild->AddRef();
							m_pbsLeftChild->AddRef();
						};
					
						~CJoinSet()
						{
							m_pbsLeftChild->Release();
							m_pbsRightChild->Release();
						}
					
						CBitSet *PbsLeftChild() const
						{
							return m_pbsLeftChild;
						}
					
						CBitSet *PbsRightChild() const
						{
							return m_pbsRightChild;
						}
					
						virtual
						BOOL FEqual(const CJoinSet *pjoinsetother) const
						{
							return m_pbsLeftChild->FEqual(pjoinsetother->PbsLeftChild())
								&& m_pbsRightChild->FEqual(pjoinsetother->PbsRightChild());
						}
					
					};
		
			class CJoinExprCost: public CRefCount
			{
			
				private:
				
//					CExpression *m_pjoinExpr;
					CDouble m_pdRightChildCost;
					CDouble m_pdCost;
				
				public:
				
					CJoinExprCost
						(
//						CExpression *pjoinExpr,
						CDouble dRightChildCost,
						CDouble dCost
						)
						:
//						m_pjoinExpr(pjoinExpr),
						m_pdRightChildCost(dRightChildCost),
						m_pdCost(dCost)
						{}
				
					~CJoinExprCost()
					{
//						GPOS_DELETE(m_pdRightChildCost);
					}

					CDouble DRightChildCost()
					{
						return m_pdRightChildCost;
					}
				
//					CExpression *Pexpr()
//					{
//						return m_pjoinExpr;
//					}

					CDouble DCost()
					{
						return m_pdCost;
					}
				};
		
		private:

			//---------------------------------------------------------------------------
			//	@struct:
			//		SComponentPair
			//
			//	@doc:
			//		Struct to capture a pair of components
			//
			//---------------------------------------------------------------------------
			struct SComponentPair : public CRefCount
			{
				// first component
				CBitSet *m_pbsFst;

				// second component
				CBitSet *m_pbsSnd;

				// ctor
				SComponentPair(CBitSet *pbsFst, CBitSet *pbsSnd);

				// dtor
				~SComponentPair();

				// hashing function
				static
				ULONG UlHash(const SComponentPair *pcomppair);

				// equality function
				static
				BOOL FEqual(const SComponentPair *pcomppairFst, const SComponentPair *pcomppairSnd);
			};

			// hashing function
			static
			ULONG UlHashBitSet
				(
				const CBitSet *pbs
				)
			{
				GPOS_ASSERT(NULL != pbs);

				return pbs->UlHash();
			}

			 // equality function
			static
			BOOL FEqualBitSet
				(
				const CBitSet *pbsFst,
				const CBitSet *pbsSnd
				)
			{
				GPOS_ASSERT(NULL != pbsFst);
				GPOS_ASSERT(NULL != pbsSnd);

				return pbsFst->FEqual(pbsSnd);
			}

			static ULONG UlHashJoinSet
				(
				const CJoinSet *pJoinSet
				 );
		
			static
			BOOL FEqualJoinSet
			(
			 const CJoinSet *pJoinSet1,
			 const CJoinSet *pJoinSet2
			 )
			{
				GPOS_ASSERT(NULL != pJoinSet1);
				GPOS_ASSERT(NULL != pJoinSet2);
				
				return pJoinSet1->FEqual(pJoinSet2);
			}
			// hash map from component to best join order
			typedef CHashMap<CBitSet, CExpression, UlHashBitSet, FEqualBitSet,
				CleanupRelease<CBitSet>, CleanupRelease<CExpression> > HMBSExpr;
		
			typedef CHashMap<CJoinSet, CJoinExprCost, UlHashJoinSet, FEqualJoinSet,
				CleanupRelease<CJoinSet>, CleanupRelease<CJoinExprCost> > HMJoinSet;

			// hash map from component pair to connecting edges
			typedef CHashMap<SComponentPair, CExpression, SComponentPair::UlHash, SComponentPair::FEqual,
				CleanupRelease<SComponentPair>, CleanupRelease<CExpression> > HMCompLink;

			// hash map from expression to cost of best join order
			typedef CHashMap<CExpression, CDouble, CExpression::UlHash, CUtils::FEqual,
				CleanupRelease<CExpression>, CleanupDelete<CDouble> > HMExprCost;

			// lookup table for links
			HMCompLink *m_phmcomplink;

			// dynamic programming table
			HMBSExpr *m_phmbsexpr;

			// map of expressions to its cost
			HMExprCost *m_phmexprcost;
		
			HMJoinSet *m_remainingcurrjoinset;

			// array of top-k join expression
			DrgPexpr *m_pdrgpexprTopKOrders;

			// dummy expression to used for non-joinable components
			CExpression *m_pexprDummy;

			// build expression linking given components
			CExpression *PexprBuildPred(CBitSet *pbsFst, CBitSet *pbsSnd);

			// lookup best join order for given set
			CExpression *PexprLookup(CBitSet *pbs);

			// extract predicate joining the two given sets
			CExpression *PexprPred(CBitSet *pbsFst, CBitSet *pbsSnd);

			// join expressions in the given two sets
			CExpression *PexprJoin(CBitSet *pbsFst, CBitSet *pbsSnd);

			// join expressions in the given set
			CExpression *PexprJoin(CBitSet *pbs);

			// find best join order for given component using dynamic programming
			CExpression *PexprBestJoinOrderDP(CBitSet *pbs);

			// find best join order for given component
			CExpression *PexprBestJoinOrder(CBitSet *pbs);

			// generate cross product for the given components
			CExpression *PexprCross(CBitSet *pbs);

			// return maximum connectedness of elements in given set
			CDouble DMaxConnectedness(CBitSet *pbs);

			// return connectedness measure of given component
			CDouble DConnectedness(ULONG ulComp);

			// join a covered subset with uncovered subset
			CExpression *PexprJoinCoveredSubsetWithUncoveredSubset(CBitSet *pbs, CBitSet *pbsCovered, CBitSet *pbsUncovered);

			// return a subset of the given set covered by one or more edges
			CBitSet *PbsCovered(CBitSet *pbsInput);

			// add given join order to best results
			void AddJoinOrder(CExpression *pexprJoin, CDouble dCost);

			// compute cost of given join expression
			CDouble DCost(CExpression *pexpr);

			// derive stats on given expression
			void DeriveStats(CExpression *pexpr);

			// add expression to cost map
			void InsertExpressionCost(CExpression *pexpr, CDouble dCost, BOOL fValidateInsert);

			// generate all subsets of the given array of elements
			static
			void GenerateSubsets(IMemoryPool *pmp, CBitSet *pbsCurrent, ULONG *pulElems, ULONG ulSize, ULONG ulIndex, DrgPbs *pdrgpbsSubsets);

			// driver of subset generation
			static
			DrgPbs *PdrgpbsSubsets(IMemoryPool *pmp, CBitSet *pbs);

		public:

			// ctor
			CJoinOrderDP
				(
				IMemoryPool *pmp,
				DrgPexpr *pdrgpexprComponents,
				DrgPexpr *pdrgpexprConjuncts
				);

			// dtor
			virtual
			~CJoinOrderDP();

			// main handler
			virtual
			CExpression *PexprExpand();

			// best join orders
			DrgPexpr *PdrgpexprTopK() const
			{
				return m_pdrgpexprTopKOrders;
			}

			// print function
			virtual
			IOstream &OsPrint(IOstream &) const;

	}; // class CJoinOrderDP

}

#endif // !GPOPT_CJoinOrderDP_H

// EOF
