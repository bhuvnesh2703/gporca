//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		IWorker.h
//
//	@doc:
//		Interface class to worker; broken into interface and implementation
//		to avoid cyclic dependencies between worker, list, spinlock etc.
//		The Worker abstraction contains only components needed to schedule,
//		execute, and abort tasks; no task specific configuration such as
//		output streams is contained in Worker;
//---------------------------------------------------------------------------
#ifndef GPOS_IWorker_H
#define GPOS_IWorker_H

#include "gpos/types.h"
#include "gpos/common/CStackObject.h"

#define GPOS_CHECK_ABORT (IWorker::CheckAbort(__FILE__, __LINE__))

#define GPOS_CHECK_STACK_SIZE                         \
	do                                                \
	{                                                 \
		if (NULL != IWorker::Self())                  \
		{                                             \
			(void) IWorker::Self()->CheckStackSize(); \
		}                                             \
	} while (0)


// assert no spinlock
#define GPOS_ASSERT_NO_SPINLOCK              \
	GPOS_ASSERT_IMP(NULL != IWorker::Self(), \
					(!IWorker::Self()->OwnsSpinlocks()) && "Must not hold a spinlock!")


#if (GPOS_SunOS)
#define GPOS_CHECK_ABORT_MAX_INTERVAL_MSEC (ULONG(2000))
#else
#define GPOS_CHECK_ABORT_MAX_INTERVAL_MSEC (ULONG(1500))
#endif

namespace gpos
{
	// prototypes
	class CSpinlockBase;
	class CMutexBase;
	class ITask;
	class CWorkerId;

	//---------------------------------------------------------------------------
	//	@class:
	//		IWorker
	//
	//	@doc:
	//		Interface to abstract scheduling primitive such as threads;
	//
	//---------------------------------------------------------------------------
	class IWorker : public CStackObject
	{
	private:
		// hidden copy ctor
		IWorker(const IWorker &);

		// check for abort request
		virtual void CheckForAbort(const CHAR *, ULONG) = 0;

	public:
		// dummy ctor
		IWorker()
		{
		}

		// dummy dtor
		virtual ~IWorker()
		{
		}

		// accessors
		virtual ULONG GetThreadId() const = 0;
		virtual CWorkerId GetWid() const = 0;
		virtual ULONG_PTR GetStackStart() const = 0;
		virtual ITask *GetTask() = 0;

		// stack check
		virtual BOOL CheckStackSize(ULONG request = 0) const = 0;

#ifdef GPOS_DEBUG
		virtual BOOL CanAcquireSpinlock(const CSpinlockBase *) const = 0;
		virtual BOOL OwnsSpinlocks() const = 0;
		virtual void RegisterSpinlock(CSpinlockBase *) = 0;
		virtual void UnregisterSpinlock(CSpinlockBase *) = 0;

		virtual BOOL OwnsMutexes() const = 0;
		virtual void RegisterMutex(CMutexBase *) = 0;
		virtual void UnregisterMutex(CMutexBase *) = 0;

		static BOOL m_enforce_time_slices;
#endif  // GPOS_DEBUG

		// lookup worker in worker pool manager
		static IWorker *Self();

		// check for aborts
		static void CheckAbort(const CHAR *file, ULONG line_num);

	};  // class IWorker
}  // namespace gpos

#endif  // !GPOS_IWorker_H

// EOF
