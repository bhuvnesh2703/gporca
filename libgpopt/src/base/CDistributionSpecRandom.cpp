//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDistributionSpecRandom.cpp
//
//	@doc:
//		Specification of random distribution
//---------------------------------------------------------------------------

#include "naucrates/traceflags/traceflags.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CDistributionSpecRandom.h"
#include "gpopt/base/CDistributionSpecStrictRandom.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CPhysicalMotionRandom.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRandom::CDistributionSpecRandom
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDistributionSpecRandom::CDistributionSpecRandom()
	:
	m_is_duplicate_sensitive(false),
	m_fSatisfiedBySingleton(true)
{
	if (COptCtxt::PoctxtFromTLS()->FDMLQuery())
	{
		// set duplicate sensitive flag to enforce Hash-Distribution of
		// Const Tables in DML queries
		MarkDuplicateSensitive();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRandom::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL 
CDistributionSpecRandom::Matches
	(
	const CDistributionSpec *pds
	) 
	const
{
	if (Edt() != pds->Edt())
	{
		return false;
	}

	const CDistributionSpecRandom *pdsRandom =
			dynamic_cast<const CDistributionSpecRandom*>(pds);

	return pdsRandom->IsDuplicateSensitive() == m_is_duplicate_sensitive;
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRandom::FSatisfies
//
//	@doc:
//		Check if this distribution spec satisfies the given one
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecRandom::FSatisfies
	(
	const CDistributionSpec *pds
	)
	const
{
	if (Matches(pds))
	{
		return true;
	}
	
	if (EdtRandom == pds->Edt() && 
			(IsDuplicateSensitive() || !CDistributionSpecRandom::PdsConvert(pds)->IsDuplicateSensitive()))
	{
		return true;
	}

	return EdtAny == pds->Edt() || EdtNonSingleton == pds->Edt();
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRandom::AppendEnforcers
//
//	@doc:
//		Add required enforcers to dynamic array
//
//---------------------------------------------------------------------------
void
CDistributionSpecRandom::AppendEnforcers
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	CReqdPropPlan *
#ifdef GPOS_DEBUG
	prpp
#endif // GPOS_DEBUG
	,
	CExpressionArray *pdrgpexpr,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(!GPOS_FTRACE(EopttraceDisableMotions));
	GPOS_ASSERT(this == prpp->Ped()->PdsRequired() &&
	            "required plan properties don't match enforced distribution spec");


	if (GPOS_FTRACE(EopttraceDisableMotionRandom))
	{
		// random Motion is disabled
		return;
	}

	// consider the query: INSERT INTO t1_random VALUES (1), (2);
	// where t1_random is randomly distributed.
	//
	// CPhysicalDML(Insert) on t1_random requires its child to deliver
	// strict random spec or random spec enforced by motion, however
	// CPhysicalConstTableGet (ie. VALUES) operator derives universal spec.
	// In order to satisfy the distribution spec requirement of CPhysicalDML(Insert),
	// enforcement framework adds a CPhysicalMotionRandom motion
	// delivering random spec.
	// Since, INSERT is executed directly on the segments,
	// CPhysicalConstTableGet (deriving Universal spec) is executed on all the
	// segments locally, but to ensure that duplicates are not inserted,
	// DXL to Planned Statement translator converts the CPhysicalMotionRandom
	// above CPhysicalConstTableGet to a "Result node" with hash filters, which
	// filters data from all the segments except one.
	// (See #2 below in Physical Plan and GPDB plan)
	// In order to identify if the CPhysicalMotionRandom node added below
	// will be not be translated to a Result Node, i.e it does not have a
	// universal spec child, mark m_is_enforced_by_motion to true.
	//
	// Physical plan:
	// +--CPhysicalDML (Insert, "t1_random"), Source Columns: ["a" (0)], Action: ("ColRef_0001" (1))
	//    +--CPhysicalMotionRandom (#1)
	//       +--CPhysicalComputeScalar
	//          |--CPhysicalMotionRandom (#2)  ==> Motion delivers duplicate hazard
	//          |  +--CPhysicalConstTableGet Columns: ["a" (0)] Values: [(1); (2)] ==> Derives universal spec
	//          +--CScalarProjectList   origin: [Grp:9, GrpExpr:0]
	//             +--CScalarProjectElement "ColRef_0001" (1)
	//                +--CScalarConst (1)
	//
	// Insert  (cost=0.00..0.03 rows=1 width=4)
	//   ->  Redistribute Motion 1:1  (slice1; segments: 1)  (cost=0.00..0.00 rows=1 width=8)
	//      ->  Result  (cost=0.00..0.00 rows=1 width=8)
	//         ->  Result  (cost=0.00..0.00 rows=1 width=1)  (#2)  ==> Motion converted to Result Node
	//            ->  Values Scan on "Values"  (cost=0.00..0.00 rows=2 width=4) ==> Derives universal spec

	CDistributionSpec *expr_dist_spec = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pds();
	CDistributionSpecRandom *random_dist_spec = NULL;

	if (Edt() == EdtStrictRandom)
	{
		// strict random spec is a derived class which calls the
		// AppendEnforcers of random spec, thus instantiate an object
		// of strict random spec
		random_dist_spec = GPOS_NEW(mp) CDistributionSpecStrictRandom();
	}
	else if (expr_dist_spec->Edt() == CDistributionSpec::EdtUniversal)
	{
		// the motion node in the enforcer is added on top of a child
		// deriving universal spec, this motion node will be
		// translated to a result node with hash filter to remove
		// duplicates
		random_dist_spec = GPOS_NEW(mp) CDistributionSpecRandom();
	}
	else
	{
		// the motion added in this enforcer will translate to
		// a redistribute motion
		random_dist_spec = GPOS_NEW(mp) CDistributionSpecStrictRandom();
	}

	// add a hashed distribution enforcer
	pexpr->AddRef();
	CExpression *pexprMotion = GPOS_NEW(mp) CExpression
										(
										mp,
										GPOS_NEW(mp) CPhysicalMotionRandom(mp, random_dist_spec),
										pexpr
										);
	pdrgpexpr->Append(pexprMotion);
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecRandom::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CDistributionSpecRandom::OsPrint
	(
	IOstream &os
	)
	const
{
	return os << this->SzId();
}

// EOF

