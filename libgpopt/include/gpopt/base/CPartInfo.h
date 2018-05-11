//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CPartInfo.h
//
//	@doc:
//		Derived partition information at the logical level
//---------------------------------------------------------------------------
#ifndef GPOPT_CPartInfo_H
#define GPOPT_CPartInfo_H

#include "gpos/base.h"
#include "gpopt/base/CColRef.h"
#include "gpopt/base/CPartKeys.h"

// fwd decl
namespace gpmd
{
	class IMDId;
}

namespace gpopt
{
	using namespace gpos;
	using namespace gpmd;

	// fwd decl
	class CPartConstraint;

	//---------------------------------------------------------------------------
	//	@class:
	//		CPartInfo
	//
	//	@doc:
	//		Derived partition information at the logical level
	//
	//---------------------------------------------------------------------------
	class CPartInfo : public CRefCount
	{
		private:

			//---------------------------------------------------------------------------
			//	@class:
			//		CPartInfoEntry
			//
			//	@doc:
			//		A single entry of the CPartInfo
			//
			//---------------------------------------------------------------------------
			class CPartInfoEntry : public CRefCount
			{

				private:

					// scan id
					ULONG m_scan_id;

					// partition table mdid
					IMDId *m_mdid;

					// partition keys
					DrgPpartkeys *m_pdrgppartkeys;

					// part constraint of the relation
					CPartConstraint *m_ppartcnstrRel;

					// private copy ctor
					CPartInfoEntry(const CPartInfoEntry &);

				public:

					// ctor
					CPartInfoEntry(ULONG scan_id, IMDId *mdid, DrgPpartkeys *pdrgppartkeys, CPartConstraint *ppartcnstrRel);

					// dtor
					virtual
					~CPartInfoEntry();

					// scan id
					virtual
					ULONG ScanId() const
					{
						return m_scan_id;
					}

					// relation part constraint
					CPartConstraint *PpartcnstrRel() const
					{
						return m_ppartcnstrRel;
					}

					// create a copy of the current object, and add a set of remapped
					// part keys to this entry, using the existing keys and the given hashmap
					CPartInfoEntry *PpartinfoentryAddRemappedKeys(IMemoryPool *memory_pool, CColRefSet *pcrs, UlongColRefHashMap *colref_mapping);

					// mdid of partition table
					virtual
					IMDId *MDId() const
					{
						return m_mdid;
					}

					// partition keys of partition table
					virtual
					DrgPpartkeys *Pdrgppartkeys() const
					{
						return m_pdrgppartkeys;
					}

					// print function
					IOstream &OsPrint(IOstream &os) const;

					// copy part info entry into given memory pool
					CPartInfoEntry *PpartinfoentryCopy(IMemoryPool *memory_pool);

#ifdef GPOS_DEBUG
					// debug print for interactive debugging sessions only
					void DbgPrint() const;
#endif // GPOS_DEBUG

			}; // CPartInfoEntry

			typedef CDynamicPtrArray<CPartInfoEntry, CleanupRelease> DrgPpartentries;

			// partition table consumers
			DrgPpartentries *m_pdrgppartentries;

			// private ctor
			explicit
			CPartInfo(DrgPpartentries *pdrgppartentries);

			//private copy ctor
			CPartInfo(const CPartInfo &);

		public:

			// ctor
			explicit
			CPartInfo(IMemoryPool *memory_pool);

			// dtor
			virtual
			~CPartInfo();

			// number of part table consumers
			ULONG UlConsumers() const
			{
				return m_pdrgppartentries->Size();
			}

			// add part table consumer
			void AddPartConsumer
				(
				IMemoryPool *memory_pool,
				ULONG scan_id,
				IMDId *mdid,
				DrgDrgPcr *pdrgpdrgpcrPart,
				CPartConstraint *ppartcnstrRel
				);

			// scan id of the entry at the given position
			ULONG ScanId(ULONG ulPos)	const;

			// relation mdid of the entry at the given position
			IMDId *GetRelMdId(ULONG ulPos) const;

			// part keys of the entry at the given position
			DrgPpartkeys *Pdrgppartkeys(ULONG ulPos) const;

			// part constraint of the entry at the given position
			CPartConstraint *Ppartcnstr(ULONG ulPos) const;

			// check if part info contains given scan id
			BOOL FContainsScanId(ULONG scan_id) const;

			// part keys of the entry with the given scan id
			DrgPpartkeys *PdrgppartkeysByScanId(ULONG scan_id) const;

			// return a new part info object with an additional set of remapped keys
			CPartInfo *PpartinfoWithRemappedKeys
				(
				IMemoryPool *memory_pool,
				DrgPcr *pdrgpcrSrc,
				DrgPcr *pdrgpcrDest
				)
				const;

			// print
			IOstream &OsPrint(IOstream &) const;

			// combine two part info objects
			static
			CPartInfo *PpartinfoCombine
				(
				IMemoryPool *memory_pool,
				CPartInfo *ppartinfoFst,
				CPartInfo *ppartinfoSnd
				);

#ifdef GPOS_DEBUG
			// debug print for interactive debugging sessions only
			void DbgPrint() const;
#endif // GPOS_DEBUG

	}; // CPartInfo

	// shorthand for printing
	inline
	IOstream &operator << (IOstream &os, CPartInfo &partinfo)
	{
		return partinfo.OsPrint(os);
	}
}

#endif // !GPOPT_CPartInfo_H

// EOF

