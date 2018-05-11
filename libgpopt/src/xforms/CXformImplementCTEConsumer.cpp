//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementCTEConsumer.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/xforms/CXformImplementCTEConsumer.h"

#include "gpopt/operators/ops.h"
#include "gpopt/metadata/CTableDescriptor.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementCTEConsumer::CXformImplementCTEConsumer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementCTEConsumer::CXformImplementCTEConsumer
	(
	IMemoryPool *memory_pool
	)
	:
	CXformImplementation
		(
		 // pattern
		GPOS_NEW(memory_pool) CExpression
				(
				memory_pool,
				GPOS_NEW(memory_pool) CLogicalCTEConsumer(memory_pool)
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementCTEConsumer::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformImplementCTEConsumer::Exfp
	(
	CExpressionHandle & // exprhdl
	)
	const
{
	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementCTEConsumer::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementCTEConsumer::Transform
	(
	CXformContext *pxfctxt,
	CXformResult *pxfres,
	CExpression *pexpr
	)
	const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CLogicalCTEConsumer *popCTEConsumer = CLogicalCTEConsumer::PopConvert(pexpr->Pop());
	IMemoryPool *memory_pool = pxfctxt->Pmp();

	// extract components for alternative
	ULONG id = popCTEConsumer->UlCTEId();

	DrgPcr *colref_array = popCTEConsumer->Pdrgpcr();
	colref_array->AddRef();

	UlongColRefHashMap *colref_mapping = popCTEConsumer->Phmulcr();
	GPOS_ASSERT(NULL != colref_mapping);
	colref_mapping->AddRef();

	// create physical CTE Consumer
	CExpression *pexprAlt =
		GPOS_NEW(memory_pool) CExpression
			(
			memory_pool,
			GPOS_NEW(memory_pool) CPhysicalCTEConsumer(memory_pool, id, colref_array, colref_mapping)
			);

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF
