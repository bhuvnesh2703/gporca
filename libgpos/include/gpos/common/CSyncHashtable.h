//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CSyncHashtable.h
//
//	@doc:
//		Allocation-less static hashtable; 
//		Manages client objects without additional allocations; this is a
//		requirement for system programming tasks to ensure the hashtable
//		works in exception situations, e.g. OOM;
//
//		1)	Hashtable is static and cannot resize during operations;
//		2)	expects target type to have SLink (see CList.h) and Key
//			members with appopriate accessors;
//		3)	clients must provide their own hash function;
//		4)	hashtable synchronizes through spinlocks on each bucket;
//---------------------------------------------------------------------------
#ifndef GPOS_CSyncHashtable_H
#define GPOS_CSyncHashtable_H

#include "gpos/base.h"

#include "gpos/common/CAutoRg.h"
#include "gpos/common/CList.h"
#include "gpos/sync/CAutoSpinlock.h"
#include "gpos/task/CAutoSuspendAbort.h"

namespace gpos
{

	// prototypes
	template <class T, class K, class S>
	class CSyncHashtableAccessorBase;

	template <class T, class K, class S>
	class CSyncHashtableAccessByKey;

	template <class T, class K, class S>
	class CSyncHashtableIter;

	template <class T, class K, class S>
	class CSyncHashtableAccessByIter;

	//---------------------------------------------------------------------------
	//	@class:
	//		CSyncHashtable<T, K, S>
	//
	//	@doc:
	//		Allocation-less static hash table;
	//
	//		Ideally the offset of the key would be a template parameter too in order
	//		to avoid accidental tampering with this value -- not all compiler allow 
	//		the use of the offset macro in the template definition, however.
	//
	//---------------------------------------------------------------------------
	template <class T, class K, class S>
	class CSyncHashtable
	{
		// accessor and iterator classes are friends
		friend class CSyncHashtableAccessorBase<T, K, S>;
		friend class CSyncHashtableAccessByKey<T, K, S>;
		friend class CSyncHashtableAccessByIter<T, K, S>;
		friend class CSyncHashtableIter<T, K, S>;

		private:
	
			// hash bucket is a pair of list, spinlock
			struct SBucket
			{
				private:
			
					// no copy ctor
					SBucket(const SBucket &);

				public:
			
					// ctor
					SBucket() {};
				
					// spinlock to protect bucket
					S m_lock;
				
					// hash chain
					CList<T> m_chain;

#ifdef GPOS_DEBUG
					// bucket number
					ULONG m_bucket_idx;
#endif // GPOS_DEBUG

			};

			// range of buckets
			SBucket *m_buckets;
		
			// number of ht buckets
			ULONG m_nbuckets;

			// number of ht entries
			volatile ULONG_PTR m_size;
		
			// offset of key
			ULONG m_key_offset;

			// invalid key - needed for iteration
			const K *m_invalid_key;

			// pointer to hashing function
			ULONG (*m_hashfn)(const K&);

			// pointer to key equality function
			BOOL (*m_eqfn)(const K&, const K&);
				
			// function to compute bucket index for key
			ULONG GetBucketIndex
				(
				const K &key
				)
				const
			{
				GPOS_ASSERT(IsValid(key) && "Invalid key is inaccessible");

				return m_hashfn(key) % m_nbuckets;
			}

			// function to get bucket by index
			SBucket &GetBucket
				(
				const ULONG index
				)
				const
			{
				GPOS_ASSERT(index < m_nbuckets && "Invalid bucket index");

				return m_buckets[index];
			}

			// extract key out of type
			K &Key
				(
				T *value
				)
				const
			{
				GPOS_ASSERT(ULONG_MAX != m_key_offset && "Key offset not initialized.");
			
				K &k = *(K*)((BYTE*)value + m_key_offset);

				return k;
			}

			// key validity check
			BOOL IsValid
				(
				const K &key
				)
				const
			{
				return !m_eqfn(key, *m_invalid_key);
			}

		public:
	
			// type definition of function used to cleanup element
			typedef void (*DestroyEntryFuncPtr)(T *);

			// ctor
			CSyncHashtable<T, K, S>()
				:
				m_buckets(NULL),
				m_nbuckets(0),
				m_size(0),
				m_key_offset(ULONG_MAX),
				m_invalid_key(NULL)
			{}
		
			// dtor
			// deallocates hashtable internals, does not destroy
			// client objects
			~CSyncHashtable<T, K, S>()
            {
                Cleanup();
            }
		
			// Initialization of hashtable
			void Init
				(
				IMemoryPool *memory_pool,
				ULONG size,
				ULONG link_offset,
				ULONG key_offset,
				const K *invalid_key,
				ULONG (*func_hash)(const K&),
				BOOL (*func_equal)(const K&, const K&)
				)
            {
                GPOS_ASSERT(NULL == m_buckets);
                GPOS_ASSERT(0 == m_nbuckets);
                GPOS_ASSERT(NULL != invalid_key);
                GPOS_ASSERT(NULL != func_hash);
                GPOS_ASSERT(NULL != func_equal);

                m_nbuckets = size;
                m_key_offset = key_offset;
                m_invalid_key = invalid_key;
                m_hashfn = func_hash;
                m_eqfn = func_equal;

                m_buckets = GPOS_NEW_ARRAY(memory_pool, SBucket, m_nbuckets);

                // NOTE: 03/25/2008; since it's the only allocation in the
                //		constructor the protection is not needed strictly speaking;
                //		Using auto range here just for cleanliness;
                CAutoRg<SBucket> argbucket;
                argbucket = m_buckets;

                for(ULONG i = 0; i < m_nbuckets; i ++)
                {
                    m_buckets[i].m_chain.Init(link_offset);
    #ifdef GPOS_DEBUG
                    // add serial number
                    m_buckets[i].m_bucket_idx = i;
    #endif // GPOS_DEBUG
                }

                // unhook from protector
                argbucket.RgtReset();
            }

			// dealloc bucket range and reset members
			void Cleanup()
            {
                GPOS_DELETE_ARRAY(m_buckets);
                m_buckets = NULL;

                m_nbuckets = 0;
            }

			// iterate over all entries and call destroy function on each entry
			void DestroyEntries(DestroyEntryFuncPtr pfunc_destroy)
            {
                // need to suspend cancellation while cleaning up
                CAutoSuspendAbort asa;

                T *value = NULL;
                CSyncHashtableIter<T, K, S> it(*this);

                // since removing an entry will automatically advance iter's
                // position, we need to make sure that advance iter is called
                // only when we do not have an entry to delete
                while (NULL != value || it.Advance())
                {
                    if (NULL != value)
                    {
                        pfunc_destroy(value);
                    }

                    {
                        CSyncHashtableAccessByIter<T, K, S> acc(it);
                        if (NULL != (value = acc.Value()))
                        {
                            acc.Remove(value);
                        }
                    }
                }

    #ifdef GPOS_DEBUG
                CSyncHashtableIter<T, K, S> it_snd(*this);
                GPOS_ASSERT(!it_snd.Advance());
    #endif // GPOS_DEBUG
            }

			// insert function;
			void Insert(T *value)
            {
                K &key = Key(value);

                GPOS_ASSERT(IsValid(key));

                // determine target bucket
                SBucket &bucket = GetBucket(GetBucketIndex(key));

                // acquire auto spinlock
                CAutoSpinlock alock(bucket.m_lock);
                alock.Lock();

                // inserting at bucket's head is required by hashtable iteration
                bucket.m_chain.Prepend(value);

                // increase number of entries
                (void) ExchangeAddUlongPtrWithInt(&m_size, 1);
            }

			// return number of entries
			ULONG_PTR Size() const
			{
				return m_size;
			}

	}; // class CSyncHashtable

}

#endif // !GPOS_CSyncHashtable_H

// EOF

