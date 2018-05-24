//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		ioutils.cpp
//
//	@doc:
//		Implementation of I/O utilities
//---------------------------------------------------------------------------

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <sched.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>

#include "gpos/base.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/error/CFSimulator.h"
#include "gpos/error/CLogger.h"
#include "gpos/io/ioutils.h"
#include "gpos/string/CStringStatic.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/task/CTaskContext.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		ioutils::Stat
//
//	@doc:
//		Check state of file or directory
//
//---------------------------------------------------------------------------
void
gpos::ioutils::Stat
	(
	const CHAR *file_path,
	SFileStat *file_state
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != file_path);
	GPOS_ASSERT(NULL != file_state);

	// reset file state
	(void) clib::MemSet(file_state, 0, sizeof(*file_state));

	INT res = -1;

	// check to simulate I/O error
	GPOS_CHECK_SIM_IO_ERR(&res, stat(file_path, file_state));

	if (0 != res)
	{
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::FStat
//
//	@doc:
//		Check state of file or directory by file descriptor
//
//---------------------------------------------------------------------------
void
gpos::ioutils::Fstat
	(
	const INT iFd,
	SFileStat *file_state
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != file_state);

	// reset file state
	(void) clib::MemSet(file_state, 0, sizeof(*file_state));

	INT res = -1;

	// check to simulate I/O error
	GPOS_CHECK_SIM_IO_ERR(&res, fstat(iFd, file_state));

	if (0 != res)
	{
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::FPathExist
//
//	@doc:
//		Check if path is mapped to an accessible file or directory
//
//---------------------------------------------------------------------------
BOOL
gpos::ioutils::FPathExist
	(
	const CHAR *file_path
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != file_path);

	SFileStat fs;

	INT res = stat(file_path, &fs);

	return (0 == res);
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::FDir
//
//	@doc:
//		Check if path is directory
//
//---------------------------------------------------------------------------
BOOL
gpos::ioutils::FDir
	(
	const CHAR *file_path
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != file_path);

	SFileStat fs;
	Stat(file_path, &fs);

	return S_ISDIR(fs.st_mode);
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::FFile
//
//	@doc:
//		Check if path is file
//
//---------------------------------------------------------------------------
BOOL
gpos::ioutils::FFile
	(
	const CHAR *file_path
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != file_path);

	SFileStat fs;
	Stat(file_path, &fs);

	return S_ISREG(fs.st_mode);
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::UllFileSize
//
//	@doc:
//		Get file size by file path
//
//---------------------------------------------------------------------------
ULLONG
gpos::ioutils::UllFileSize
	(
	const CHAR *file_path
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != file_path);
	GPOS_ASSERT(FFile(file_path));

	SFileStat fs;
	Stat(file_path, &fs);

	return fs.st_size;
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::UllFileSize
//
//	@doc:
//		Get file size by file descriptor
//
//---------------------------------------------------------------------------
ULLONG
gpos::ioutils::UllFileSize
	(
	const INT iFd
	)
{
	GPOS_ASSERT_NO_SPINLOCK;

	SFileStat fs;
	Fstat(iFd, &fs);

	return fs.st_size;
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::FPerms
//
//	@doc:
//		Check permissions
//
//---------------------------------------------------------------------------
BOOL
gpos::ioutils::FPerms
	(
	const CHAR *file_path,
	ULONG permission_bits
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != file_path);

	SFileStat fs;
	Stat(file_path, &fs);

	return (permission_bits == (fs.st_mode & permission_bits));
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::MkDir
//
//	@doc:
//		Create directory with specific permissions
//
//---------------------------------------------------------------------------
void
gpos::ioutils::MkDir
	(
	const CHAR *file_path,
	ULONG permission_bits
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != file_path);

	INT res = -1;

	// check to simulate I/O error
	GPOS_CHECK_SIM_IO_ERR(&res, mkdir(file_path, (MODE_T) permission_bits));

	if (0 != res)
	{
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::RmDir
//
//	@doc:
//		Delete directory
//
//---------------------------------------------------------------------------
void
gpos::ioutils::RmDir
	(
	const CHAR *file_path
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != file_path);
	GPOS_ASSERT(FDir(file_path));

	INT res = -1;

	// delete existing directory and check to simulate I/O error
	GPOS_CHECK_SIM_IO_ERR(&res, rmdir(file_path));

	if (0 != res)
	{
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::Move
//
//	@doc:
//		Move file from old path to new path;
//		any file currently mapped to new path is deleted
//
//---------------------------------------------------------------------------
void
gpos::ioutils::Move
	(
	const CHAR *old_path,
	const CHAR *szNew
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != old_path);
	GPOS_ASSERT(NULL != szNew);
	GPOS_ASSERT(FFile(old_path));

	// delete any existing file with the new path
	if (FPathExist(szNew))
	{
		Unlink(szNew);
	}

	INT res = -1;

	// rename file and check to simulate I/O error
	GPOS_CHECK_SIM_IO_ERR(&res, rename(old_path, szNew));

	if (0 != res)
	{
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::Unlink
//
//	@doc:
//		Delete file
//
//---------------------------------------------------------------------------
void
gpos::ioutils::Unlink
	(
	const CHAR *file_path
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != file_path);

	// delete existing file
	(void) unlink(file_path);
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::IOpen
//
//	@doc:
//		Open a file;
//		It shall establish the connection between a file
//		and a file descriptor
//
//---------------------------------------------------------------------------
INT
gpos::ioutils::IOpen
	(
	const CHAR *file_path,
	INT mode,
	INT permission_bits
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != file_path);

	INT res = open(file_path, mode, permission_bits);

	GPOS_ASSERT((0 <= res) || (EINVAL != errno));

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::IClose
//
//	@doc:
//		Close a file descriptor
//
//---------------------------------------------------------------------------
INT
gpos::ioutils::IClose
	(
	INT file_descriptor
	)
{
	GPOS_ASSERT_NO_SPINLOCK;

	INT res = close(file_descriptor);

	GPOS_ASSERT(0 == res || EBADF != errno);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::IFStat
//
//	@doc:
//		Get file status
//
//---------------------------------------------------------------------------
INT
gpos::ioutils::IFStat
	(
	INT iFiledes,
	SFileStat *file_state
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != file_state);

	INT res = fstat(iFiledes, file_state);

	GPOS_ASSERT(0 == res || EBADF != errno);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::IWrite
//
//	@doc:
//		Write to a file descriptor
//
//---------------------------------------------------------------------------
INT_PTR
gpos::ioutils::IWrite
	(
	INT iFd,
	const void *pvBuf,
	const ULONG_PTR ulpCount
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != pvBuf);
	GPOS_ASSERT(0 < ulpCount);
	GPOS_ASSERT(ULONG_PTR_MAX / 2 > ulpCount);

	SSIZE_T res = write(iFd, pvBuf, ulpCount);

	GPOS_ASSERT((0 <= res) || EBADF != errno);

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::IRead
//
//	@doc:
//		Read from a file descriptor
//
//---------------------------------------------------------------------------
INT_PTR
gpos::ioutils::IRead
	(
	INT iFd,
	void *pvBuf,
	const ULONG_PTR ulpCount
	)
{
	GPOS_ASSERT_NO_SPINLOCK;
	GPOS_ASSERT(NULL != pvBuf);
	GPOS_ASSERT(0 < ulpCount);
	GPOS_ASSERT(ULONG_PTR_MAX / 2 > ulpCount);

	SSIZE_T res = read(iFd, pvBuf, ulpCount);

	GPOS_ASSERT((0 <= res) || EBADF != errno);

	return res;
}

//---------------------------------------------------------------------------
//	@function:
//		ioutils::SzMkDTemp
//
//	@doc:
//		Create a unique temporary directory
//
//---------------------------------------------------------------------------
void
gpos::ioutils::SzMkDTemp
	(
	CHAR *szTemplate
	)
{
	GPOS_ASSERT(NULL != szTemplate);

#ifdef GPOS_DEBUG
	const SIZE_T ulNumOfCmp = 6;

	SIZE_T ulSize = clib::StrLen(szTemplate);

	GPOS_ASSERT(ulSize > ulNumOfCmp);

	GPOS_ASSERT(0 == clib::MemCmp("XXXXXX", szTemplate + (ulSize - ulNumOfCmp), ulNumOfCmp));
#endif	// GPOS_DEBUG

	CHAR* szRes = NULL;


#ifdef GPOS_SunOS
	// check to simulate I/O error
	GPOS_CHECK_SIM_IO_ERR(&szRes, mktemp(szTemplate));

	ioutils::MkDir(szTemplate, S_IRUSR  | S_IWUSR  | S_IXUSR);
#else
	// check to simulate I/O error
	GPOS_CHECK_SIM_IO_ERR(&szRes, mkdtemp(szTemplate));
#endif // GPOS_SunOS

	if (NULL == szRes)
	{
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiIOError, errno);
	}

	return;
}


#ifdef GPOS_FPSIMULATOR


//---------------------------------------------------------------------------
//	@function:
//		FSimulateIOErrorInternal
//
//	@doc:
//		Inject I/O exception
//
//---------------------------------------------------------------------------
static BOOL
FSimulateIOErrorInternal
	(
	INT iErrno,
	const CHAR *szFile,
	ULONG ulLine
	)
{
	BOOL fRes = false;

	ITask *ptsk = ITask::Self();
	if (NULL != ptsk &&
	    ptsk->Trace(EtraceSimulateIOError) &&
	    CFSimulator::Pfsim()->FNewStack(CException::ExmaSystem, CException::ExmiIOError) &&
	    !GPOS_MATCH_EX(ptsk->ErrCtxt()->Exc(), CException::ExmaSystem, CException::ExmiIOError))
	{
		// disable simulation temporarily to log injection
		CAutoTraceFlag(EtraceSimulateIOError, false);

		CLogger *plogger = dynamic_cast<CLogger*>(ITask::Self()->TaskCtxt()->LogErr());
		if (!plogger->FLogging())
		{
			GPOS_TRACE_FORMAT_ERR("Simulating I/O error at %s:%d", szFile, ulLine);
		}

		errno = iErrno;

		if (ptsk->ErrCtxt()->FPending())
		{
			ptsk->ErrCtxt()->Reset();
		}

		// inject I/O error
		fRes = true;
	}

	return fRes;
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::FSimulateIOError
//
//	@doc:
//		Inject I/O exception for functions
//		whose returned value type is INT
//
//---------------------------------------------------------------------------
BOOL
gpos::ioutils::FSimulateIOError
	(
	INT *piRes,
	INT iErrno,
	const CHAR *szFile,
	ULONG ulLine
	)
{
	GPOS_ASSERT(NULL != piRes);

	*piRes = -1;

	return FSimulateIOErrorInternal(iErrno, szFile, ulLine);
}


//---------------------------------------------------------------------------
//	@function:
//		ioutils::FSimulateIOError
//
//	@doc:
//		Inject I/O exception for functions
//		whose returned value type is CHAR*
//
//---------------------------------------------------------------------------
BOOL
gpos::ioutils::FSimulateIOError
	(
	CHAR **pszRes,
	INT iErrno,
	const CHAR *szFile,
	ULONG ulLine
	)
{
	GPOS_ASSERT(NULL != pszRes);

	*pszRes = NULL;

	return FSimulateIOErrorInternal(iErrno, szFile, ulLine);
}
#endif // GPOS_FPSIMULATOR

// EOF

