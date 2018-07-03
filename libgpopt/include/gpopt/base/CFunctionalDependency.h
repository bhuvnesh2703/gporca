//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2012 EMC CORP.
//
//	@filename:
//		CFunctionalDependency.h
//
//	@doc:
//		Functional dependency representation
//---------------------------------------------------------------------------
#ifndef GPOPT_CFunctionalDependency_H
#define GPOPT_CFunctionalDependency_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpopt/base/CColRefSet.h"


namespace gpopt
{
	// fwd declarations
	class CFunctionalDependency;

	// definition of array of functional dependencies
	typedef CDynamicPtrArray<CFunctionalDependency, CleanupRelease> FunctionalDependencyArray;

	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CFunctionalDependency
	//
	//	@doc:
	//		Functional dependency representation
	//
	//---------------------------------------------------------------------------
	class CFunctionalDependency : public CRefCount
	{

		private:

			// the left hand side of the FD
			CColRefSet *m_pcrsKey;

			// the right hand side of the FD
			CColRefSet *m_pcrsDetermined;

			// private copy ctor
			CFunctionalDependency(const CFunctionalDependency &);

		public:

			// ctor
			CFunctionalDependency(CColRefSet *pcrsKey, CColRefSet *pcrsDetermined);

			// dtor
			virtual
			~CFunctionalDependency();

			// key set accessor
			CColRefSet *PcrsKey() const
			{
				return m_pcrsKey;
			}

			// determined set accessor
			CColRefSet *PcrsDetermined() const
			{
				return m_pcrsDetermined;
			}

			// determine if all FD columns are included in the given column set
			BOOL FIncluded(CColRefSet *pcrs) const;

			// hash function
			virtual
			ULONG HashValue() const;

			// equality function
			BOOL Equals(const CFunctionalDependency *pfd) const;

			// do the given arguments form a functional dependency
			BOOL
			FFunctionallyDependent
				(
				CColRefSet *pcrsKey, 
				CColRef *colref
				)
			{
				GPOS_ASSERT(NULL != pcrsKey);
				GPOS_ASSERT(NULL != colref);
				
				return m_pcrsKey->Equals(pcrsKey) && m_pcrsDetermined->FMember(colref);
			}
			
			// print
			virtual
			IOstream &OsPrint(IOstream &os) const;

			// hash function
			static
			ULONG HashValue(const FunctionalDependencyArray *pdrgpfd);

			// equality function
			static
			BOOL Equals(const FunctionalDependencyArray *pdrgpfdFst, const FunctionalDependencyArray *pdrgpfdSnd);

			// create a set of all keys in the passed FD's array
			static
			CColRefSet *PcrsKeys(IMemoryPool *memory_pool, const FunctionalDependencyArray *pdrgpfd);

			// create an array of all keys in the passed FD's array
			static
			ColRefArray *PdrgpcrKeys(IMemoryPool *memory_pool, const FunctionalDependencyArray *pdrgpfd);
			

	}; // class CFunctionalDependency

 	// shorthand for printing
	inline
	IOstream &operator << (IOstream &os, CFunctionalDependency &fd)
	{
		return fd.OsPrint(os);
	}

}

#endif // !GPOPT_CFunctionalDependency_H

// EOF
