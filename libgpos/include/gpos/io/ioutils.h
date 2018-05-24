//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		ioutils.h
//
//	@doc:
//		I/O utilities;
//		generic I/O functions that are not associated with file descriptors
//---------------------------------------------------------------------------

#ifndef GPOS_ioutils_H
#define GPOS_ioutils_H

#include <dlfcn.h>
#include <unistd.h>

#include "gpos/types.h"
#include "gpos/io/iotypes.h"

// macro for I/O error simulation
#ifdef GPOS_FPSIMULATOR
// simulate I/O error with specified address of returned error value,
// and specified errno
#define GPOS_CHECK_SIM_IO_ERR_CODE(pRes, iErrno, IOFunc) \
		do \
		{\
			if (!ioutils::FSimulateIOError(pRes, iErrno, __FILE__, __LINE__)) \
			{ \
				*pRes = IOFunc; \
			} \
		} while(0)
#else
// execute the I/O function
#define GPOS_CHECK_SIM_IO_ERR_CODE(pRes, iErrno, IOFunc) \
		do \
		{\
			GPOS_ASSERT(NULL != pRes); \
                 \
			*pRes = IOFunc; \
		} while(0)
#endif // GPOS_FPSIMULATOR

// simulate I/O error with specified address of returned error value
// and errno will set to 1 automatically
#define GPOS_CHECK_SIM_IO_ERR(pRes, IOFunc)  GPOS_CHECK_SIM_IO_ERR_CODE(pRes, 1, IOFunc)


namespace gpos
{
	namespace ioutils
	{

		// check state of file or directory
		void Stat(const CHAR *file_path, SFileStat *file_state);

		// check state of file or directory by file descriptor
		void Fstat(const INT iFd, SFileStat *file_state);

		// check if path is mapped to an accessible file or directory
		BOOL FPathExist(const CHAR *file_path);

		// get file size by file path
		ULLONG UllFileSize(const CHAR *file_path);

		// get file size by file descriptor
		ULLONG UllFileSize(const INT iFd);

		// check if path is directory
		BOOL FDir(const CHAR *file_path);

		// check if path is file
		BOOL FFile(const CHAR *file_path);

		// check permissions
		BOOL FPerms(const CHAR *file_path, ULONG permission_bits);

		// create directory with specific permissions
		void MkDir(const CHAR *file_path, ULONG permission_bits);

		// delete file
		void RmDir(const CHAR *file_path);

		// move file
		void Move(const CHAR *old_path, const CHAR *szNew);

		// delete file
		void Unlink(const CHAR *file_path);

		// open a file
		INT IOpen(const CHAR *file_path, INT mode, INT permission_bits);

		// close a file descriptor
		INT IClose(INT file_descriptor);

		// get file status
		INT IFStat(INT iFiledes, SFileStat *file_state);

		// write to a file descriptor
		INT_PTR IWrite(INT iFd, const void *pvBuf, const ULONG_PTR ulpCount);

		// read from a file descriptor
		INT_PTR IRead(INT iFd, void *pvBuf, const ULONG_PTR ulpCount);

		// create a unique temporary directory
		void SzMkDTemp(CHAR *szTemplate);

#ifdef GPOS_FPSIMULATOR
		// inject I/O error for functions whose returned value type is INT
		BOOL FSimulateIOError(INT *piRes, INT iErrno, const CHAR *szFile, ULONG ulLine);

#if defined(GPOS_64BIT) || defined(GPOS_Darwin)
		// inject I/O error for functions whose returned value type is INT_PTR
		inline
		BOOL FSimulateIOError(INT_PTR *piRes, INT iErrno, const CHAR *szFile, ULONG ulLine)
		{
			return FSimulateIOError((INT*) piRes, iErrno, szFile, ulLine);
		}
#endif

		// inject I/O error for functions whose returned value type is CHAR*
		BOOL FSimulateIOError(CHAR **ppfRes, INT iErrno, const CHAR *szFile, ULONG ulLine);
#endif // GPOS_FPSIMULATOR

	}	// namespace ioutils
}

#endif // !GPOS_ioutils_H

// EOF

