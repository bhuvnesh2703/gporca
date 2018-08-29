//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDistributionSpecRandom.h
//
//	@doc:
//		Description of a forced random distribution; 
//		Can be used as required or derived property;
//---------------------------------------------------------------------------
#ifndef GPOPT_CDistributionSpecRandom_H
#define GPOPT_CDistributionSpecRandom_H

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpec.h"

namespace gpopt
{
	using namespace gpos;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CDistributionSpecRandom
	//
	//	@doc:
	//		Class for representing forced random distribution.
	//
	//---------------------------------------------------------------------------
	class CDistributionSpecRandom : public CDistributionSpec
	{
		protected:

			// is the random distribution sensitive to duplicates
			BOOL m_is_duplicate_sensitive;
			
			// does Singleton spec satisfy current distribution?
			// by default, Singleton satisfies hashed/random since all tuples with the same hash value
			// are moved to the same host/segment,
			// this flag adds the ability to mark a distribution request as non-satisfiable by Singleton
			// in case we need to enforce across segments distribution
			BOOL m_fSatisfiedBySingleton;

			// is this specification
			// 1. derived based on the characteristic of table / childs (i.e m_is_enforced_by_motion = false)
			// or
			// 2. result of distribution property enforcement (i.e m_is_enforced_by_motion = true)
			//
			// Case 1: derived based on the characteristic of table / childs
			// consider the query #1: SELECT * FROM t1_random
			// where t1_random is randomly distributed table
			//
			// Distribution spec of randomly distributed table is derived / denoted as
			// CDistributionSpecRandom, thus t1_random derives CDistributionSpecRandom
			// and since it is due to the distribution policy of the relation,
			// m_is_enforced_by_motion is set to false.
			//
			// For query #1, physical plan below:
			//	+--CPhysicalMotionGather(master)
			//	   +--CPhysicalTableScan "t1_random" ===> Derives CDistributionSpecRandom(false) due to distribution policy
			//
			//
			// There are couple of other cases as well where the derived spec
			// is CDistributionSpecRandom:
			// a) Mismatch of distribution policy of root and child partitions.
			// If the root partition is hash distributed and any of its child
			// partition is randomly distributed, the derived distribution spec
			// is CDistributionSpecRandom
			// b) Distribution spec of Union operator when the childs delivers random
			// distribution
			// For all the above cases m_is_enforced_by_motion is set to false.
			//
			//
			// Case 2: result of distribution property enforcement
			// consider the query #2: INSERT INTO t1_random SELECT * FROM t2_hash
			// where t1_random is randomly distributed and t2_hash is hash distributed
			//
			// Distribution spec of hash distributed table is represented by
			// CDistributionSpecHashed.
			//
			// For the above query, relation t1_random derives CDistributionSpecRandom and
			// relation t2_hash derives CDistributionSpecHashed based on its distribution policy.
			// CPhysicalDML(Insert) operator requests its child to deliver
			// CDistributionSpecStrictRandom. However, CPhysicalTableScan (t2_hash) derives
			// CDistributionSpecHashed which does not match the requested spec CDistributionSpecStrictRandom.
			// In order to satisfy the distribution spec requirement of CPhysicalDML(Insert),
			// enforcement framework adds a CPhysicalMotionRandom delivering CDistributionSpecRandom
			// on top of CPhysicalTableScan.
			// Here m_is_enforced_by_motion is set to true as CDistributionSpecRandom
			// derived by CPhysicalMotionRandom is a result of property enforcement.
			//
			// For query #2, physical plan below:
			// +--CPhysicalDML (Insert, "t1_random"), Source Columns: ["a" (0)], Action: ("ColRef_0008" (8))
			//    +--CPhysicalMotionRandom ===> Enforcement framework added motion to deliver CDistributionSpecRandom(true)
			//       +--CPhysicalComputeScalar
			//       |--CPhysicalTableScan "t2_random" ("t2_random")  ===> Derives CDistributionSpecRandom(false) due to distribution
			//       +--CScalarProjectList
			//          +--CScalarProjectElement "ColRef_0008" (8)
			//          +--CScalarConst (1)
			//
			// Note:CDistributionSpecRandom with flag m_is_enforced_by_motion set to true satisfies
			// CDistributionSpecStrictRandom.
			//
			//
			// Why we need to track if CDistributionSpecRandom is enforced by motion or
			// is derived?
			// We need to identify if CPhysicalMotionRandom node must be inserted prior to
			// insert if the subtree does not provide CDistributionSpecRandom enforced by motion.
			//
			// Consider query #2 above, the derived spec CDistributionSpecRandom is due to
			// the enforcement of CPhysicalMotionRandom on top of CPhysicalTableScan(t2_hash), and
			// since CPhysicalMotionRandom will randomly distribute the data, there is no need
			// for additional redistribute before inserting into the randomly distributed table t1_random.
			//
			// Consider another query #3: INSERT INTO t1_random SELECT * FROM t2_random WHERE gp_segment_id = 1
			// where t1_random and t2_random relation are randomly distributed.
			// Although, t2_random derives CDistributionSpecRandom due to its distribution policy, the
			// the filter gp_segment_id = 1 extracts tuples from one segment only.
			// If a CPhysicalMotionRandom is not added prior to CPhysicalDML(Insert), data will be
			// inserted into gp_segment_id = 1 for t1_random. This will lead to data skew on one
			// segment and also violates the random distribution of the data inserted. Using the
			// flag m_is_enforced_by_motion we can determine that a CPhysicalMotionRandom node must
			// be added.
			BOOL m_is_enforced_by_motion;

			// private copy ctor
			CDistributionSpecRandom(const CDistributionSpecRandom &);
			
		public:

			//ctor
			CDistributionSpecRandom();

			// accessor
			virtual 
			EDistributionType Edt() const
			{
				return CDistributionSpec::EdtRandom;
			}
			
			virtual
			const CHAR *SzId() const
			{
				return "RANDOM";
			}

			// is distribution duplicate sensitive
			BOOL IsDuplicateSensitive() const
			{
				return m_is_duplicate_sensitive;
			}
			
			// mark distribution as unsatisfiable by Singleton
			void MarkDuplicateSensitive()
			{
				GPOS_ASSERT(!m_is_duplicate_sensitive);

				m_is_duplicate_sensitive = true;
			}

			// does Singleton spec satisfy current distribution?
			BOOL FSatisfiedBySingleton() const
			{
				return m_fSatisfiedBySingleton;
			}

			// mark distribution as unsatisfiable by Singleton
			void MarkUnsatisfiableBySingleton()
			{
				GPOS_ASSERT(m_fSatisfiedBySingleton);

				m_fSatisfiedBySingleton = false;
			}

			// does this distribution match the given one
			virtual 
			BOOL Matches(const CDistributionSpec *pds) const;
			
			// does current distribution satisfy the given one
			virtual 
			BOOL FSatisfies(const CDistributionSpec *pds) const;
			
			// append enforcers to dynamic array for the given plan properties
			virtual
			void AppendEnforcers(IMemoryPool *mp, CExpressionHandle &exprhdl, CReqdPropPlan *prpp, CExpressionArray *pdrgpexpr, CExpression *pexpr);				

			// return distribution partitioning type
			virtual
			EDistributionPartitioningType Edpt() const
			{
				return EdptPartitioned;
			}

			// print
			virtual
			IOstream &OsPrint(IOstream &os) const;

			// conversion function
			static
			CDistributionSpecRandom *PdsConvert
				(
				CDistributionSpec *pds
				)
			{
				GPOS_ASSERT(NULL != pds);
				GPOS_ASSERT(EdtRandom == pds->Edt());

				return dynamic_cast<CDistributionSpecRandom*>(pds);
			}
			
			// conversion function: const argument
			static
			const CDistributionSpecRandom *PdsConvert
				(
				const CDistributionSpec *pds
				)
			{
				GPOS_ASSERT(NULL != pds);
				GPOS_ASSERT(EdtRandom == pds->Edt());

				return dynamic_cast<const CDistributionSpecRandom*>(pds);
			}
			
	}; // class CDistributionSpecRandom

}

#endif // !GPOPT_CDistributionSpecRandom_H

// EOF
