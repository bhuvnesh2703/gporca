//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CHashtableAccessByIter.h
//
//	@doc:
//		Iterator's accessor provides protected access to hashtable elements
//		during iteration.
//---------------------------------------------------------------------------
#ifndef GPOS_CHashtableAccessByIter_H
#define GPOS_CHashtableAccessByIter_H


#include "gpos/common/CHashtableAccessorBase.h"
#include "gpos/common/CHashtableIter.h"


namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CHashtableAccessByIter<T, K>
	//
	//	@doc:
	//		Accessor class to provide access to the element pointed to by a
	//		hash table iterator
	//
	//---------------------------------------------------------------------------
	template <class T, class K>
	class CHashtableAccessByIter : public CHashtableAccessorBase<T, K>
	{

		// iterator class is a friend
		friend class CHashtableIter<T, K>;

		private:

			// shorthand for base class
			typedef class CHashtableAccessorBase<T, K> Base;

			// target iterator
			CHashtableIter<T, K> &m_iter;

			// no copy ctor
			CHashtableAccessByIter<T, K>
				(const CHashtableAccessByIter<T, K>&);

			// returns the first valid element starting from the given element
			T *PtFirstValid(T *pt) const
            {
                GPOS_ASSERT(NULL != pt);

                T *ptCurrent = pt;
                while (NULL != ptCurrent &&
                       !Base::Sht().FValid(Base::Sht().Key(ptCurrent)))
                {
                    ptCurrent = Base::PtNext(ptCurrent);
                }

                return ptCurrent;
            }

		public:

			// ctor
			explicit
			CHashtableAccessByIter<T, K>
				(CHashtableIter<T, K> &iter)
            :
            Base(iter.m_ht, iter.m_ulBucketIndex),
            m_iter(iter)
            {
            }

			// returns the element pointed to by iterator
			T *Pt() const
            {
                GPOS_ASSERT(m_iter.m_fInvalidInserted &&
                            "Iterator's advance is not called");

                // advance in the current bucket until finding a valid element;
                // this is needed because the next valid element pointed to by
                // iterator might have been deleted by another client just before
                // using the accessor

                return PtFirstValid(m_iter.m_ptInvalid);
            }

	}; // class CHashtableAccessByIter

}

#endif // !GPOS_CHashtableAccessByIter_H

// EOF
