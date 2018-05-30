//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CLogicalExternalGet.cpp
//
//	@doc:
//		Implementation of external get
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/COptCtxt.h"

#include "gpopt/operators/CLogicalExternalGet.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/metadata/CName.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalExternalGet::CLogicalExternalGet
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalExternalGet::CLogicalExternalGet
	(
	IMemoryPool *memory_pool
	)
	:
	CLogicalGet(memory_pool)
{}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalExternalGet::CLogicalExternalGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalExternalGet::CLogicalExternalGet
	(
	IMemoryPool *memory_pool,
	const CName *pnameAlias,
	CTableDescriptor *ptabdesc
	)
	:
	CLogicalGet(memory_pool, pnameAlias, ptabdesc)
{}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalExternalGet::CLogicalExternalGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalExternalGet::CLogicalExternalGet
	(
	IMemoryPool *memory_pool,
	const CName *pnameAlias,
	CTableDescriptor *ptabdesc,
	DrgPcr *pdrgpcrOutput
	)
	:
	CLogicalGet(memory_pool, pnameAlias, ptabdesc, pdrgpcrOutput)
{}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalExternalGet::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalExternalGet::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}
	CLogicalExternalGet *popGet = CLogicalExternalGet::PopConvert(pop);

	return Ptabdesc() == popGet->Ptabdesc() &&
			PdrgpcrOutput()->Equals(popGet->PdrgpcrOutput());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalExternalGet::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalExternalGet::PopCopyWithRemappedColumns
	(
	IMemoryPool *memory_pool,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcrOutput = NULL;
	if (fMustExist)
	{
		pdrgpcrOutput = CUtils::PdrgpcrRemapAndCreate(memory_pool, PdrgpcrOutput(), phmulcr);
	}
	else
	{
		pdrgpcrOutput = CUtils::PdrgpcrRemap(memory_pool, PdrgpcrOutput(), phmulcr, fMustExist);
	}
	CName *pnameAlias = GPOS_NEW(memory_pool) CName(memory_pool, Name());

	CTableDescriptor *ptabdesc = Ptabdesc();
	ptabdesc->AddRef();

	return GPOS_NEW(memory_pool) CLogicalExternalGet(memory_pool, pnameAlias, ptabdesc, pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalExternalGet::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalExternalGet::PxfsCandidates
	(
	IMemoryPool *memory_pool
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(memory_pool) CXformSet(memory_pool);
	(void) pxfs->ExchangeSet(CXform::ExfExternalGet2ExternalScan);

	return pxfs;
}

// EOF

