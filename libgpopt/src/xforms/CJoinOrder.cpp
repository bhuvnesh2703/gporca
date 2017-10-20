//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CJoinOrder.cpp
//
//	@doc:
//		Implementation of join order logic
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpos/common/clibwrapper.h"
#include "gpos/common/CBitSet.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/xforms/CJoinOrder.h"


using namespace gpopt;

			
//---------------------------------------------------------------------------
//	@function:
//		ICmpEdgesByLength
//
//	@doc:
//		Comparison function for simple join ordering: sort edges by length
//		only to guaranteed that single-table predicates don't end up above 
//		joins;
//
//---------------------------------------------------------------------------
INT ICmpEdgesByLength
	(
	const void *pvOne,
	const void *pvTwo
	)
{
	CJoinOrder::SEdge *pedgeOne = *(CJoinOrder::SEdge**)pvOne;
	CJoinOrder::SEdge *pedgeTwo = *(CJoinOrder::SEdge**)pvTwo;

	
	INT iDiff = (pedgeOne->m_pbs->CElements() - pedgeTwo->m_pbs->CElements());
	if (0 == iDiff)
	{
		return (INT)pedgeOne->m_pbs->UlHash() - (INT)pedgeTwo->m_pbs->UlHash();
	}
		
	return iDiff;
}

	
//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::SComponent::SComponent
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrder::SComponent::SComponent
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
	:
	m_pbs(NULL),
	m_pexpr(pexpr),
	m_fUsed(false)
{	
	m_pbs = GPOS_NEW(pmp) CBitSet(pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::SComponent::SComponent
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrder::SComponent::SComponent
	(
	CExpression *pexpr,
	CBitSet *pbs
	)
	:
	m_pbs(pbs),
	m_pexpr(pexpr),
	m_fUsed(false)
{
	GPOS_ASSERT(NULL != pbs);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::SComponent::~SComponent
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrder::SComponent::~SComponent()
{	
	m_pbs->Release();
	CRefCount::SafeRelease(m_pexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::SComponent::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrder::SComponent::OsPrint
	(
	IOstream &os
	)
const
{
	CBitSet *pbs = m_pbs;
	os 
		<< "Component: ";
	os
		<< (*pbs) << std::endl;
	os
		<< *m_pexpr << std::endl;
		
	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::SEdge::SEdge
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrder::SEdge::SEdge
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
	:
	m_pbs(NULL),
	m_pexpr(pexpr),
	m_fUsed(false)
{	
	m_pbs = GPOS_NEW(pmp) CBitSet(pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::SEdge::~SEdge
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrder::SEdge::~SEdge()
{	
	m_pbs->Release();
	m_pexpr->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::SEdge::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrder::SEdge::OsPrint
	(
	IOstream &os
	)
	const
{
	return os 
		<< "Edge : " 
		<< (*m_pbs) << std::endl 
		<< *m_pexpr << std::endl;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::CJoinOrder
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrder::CJoinOrder
	(
	IMemoryPool *pmp,
	DrgPexpr *pdrgpexpr,
	DrgPexpr *pdrgpexprConj
	)
	:
	m_pmp(pmp),
	m_rgpedge(NULL),
	m_ulEdges(0),
	m_rgpcomp(NULL),
	m_ulComps(0)
{
	typedef SComponent* Pcomp;
	typedef SEdge* Pedge;
	
	// The number of expressions except the scalar cmp operator
	m_ulComps = pdrgpexpr->UlLength();
	m_rgpcomp = GPOS_NEW_ARRAY(pmp, Pcomp, m_ulComps);
	
	CAutoTrace at(pmp);
	
	
	// Iterate over the array of expression and create SComponent Object for them and
	// them to an array, and set the bit as the index of that expression.
	// The below will be the content of the SComponent Array with respect to the input tree in
//Component: {0} Hash:1050632
//+--CLogicalDynamicGet "p1" ("p1"), Part constraint: (uninterpreted)), Columns: ["a" (0), "b" (1), "c" (2), "d" (3), "e" (4), "f" (5), "g" (6), "h" (7), "ctid" (8), "xmin" (9), "cmin" (10), "xmax" (11), "cmax" (12), "tableoid" (13), "gp_segment_id" (14)] Scan Id: 1.1, Part constraint: (uninterpreted)   rows:1   width:62  rebinds:1   origin: [Grp:0, GrpExpr:0]
//
//Component: {1} Hash:1050640
//+--CLogicalDynamicGet "p2" ("p2"), Part constraint: (uninterpreted)), Columns: ["a" (15), "b" (16), "c" (17), "d" (18), "e" (19), "f" (20), "g" (21), "h" (22), "ctid" (23), "xmin" (24), "cmin" (25), "xmax" (26), "cmax" (27), "tableoid" (28), "gp_segment_id" (29)] Scan Id: 2.2, Part constraint: (uninterpreted)   rows:1   width:62  rebinds:1   origin: [Grp:1, GrpExpr:0]
//
//Component: {2} Hash:1050656
//+--CLogicalDynamicGet "p3" ("p3"), Part constraint: (uninterpreted)), Columns: ["a" (30), "b" (31), "c" (32), "d" (33), "e" (34), "f" (35), "g" (36), "h" (37), "ctid" (38), "xmin" (39), "cmin" (40), "xmax" (41), "cmax" (42), "tableoid" (43), "gp_segment_id" (44)] Scan Id: 3.3, Part constraint: (uninterpreted)   rows:1   width:62  rebinds:1   origin: [Grp:2, GrpExpr:0]

	for (ULONG ul = 0; ul < m_ulComps; ul++)
	{
		CExpression *pexprComp = (*pdrgpexpr)[ul];
		pexprComp->AddRef();
		m_rgpcomp[ul] = GPOS_NEW(pmp) SComponent(pmp, pexprComp);
		
		// component always covers itself
		(void) m_rgpcomp[ul]->m_pbs->FExchangeSet(ul);
//		m_rgpcomp[ul]->OsPrint(at.Os());
	}

	// Number of scalar cmp expression in the array
	m_ulEdges = pdrgpexprConj->UlLength();
	m_rgpedge = GPOS_NEW_ARRAY(pmp, Pedge, m_ulEdges);
	
	
	// Iterate over the array of expression and create SEdge Object for them and
	// them to an array, and set the bit as the index of that expression.
//Edge : {} Hash:0
//+--CScalarCmp (=)   origin: [Grp:5, GrpExpr:0]
//   |--CScalarIdent "b" (1)   origin: [Grp:3, GrpExpr:0]
//   +--CScalarIdent "b" (16)   origin: [Grp:4, GrpExpr:0]
//
//Edge : {} Hash:0
//+--CScalarCmp (=)   origin: [Grp:7, GrpExpr:0]
//   |--CScalarIdent "b" (16)   origin: [Grp:4, GrpExpr:0]
//   +--CScalarIdent "b" (31)   origin: [Grp:6, GrpExpr:0]
//
//Edge : {} Hash:0
//+--CScalarCmp (=)   origin: [Grp:8, GrpExpr:0]
//   |--CScalarIdent "b" (1)   origin: [Grp:3, GrpExpr:0]
//   +--CScalarIdent "b" (31)   origin: [Grp:6, GrpExpr:0]

	for (ULONG ul = 0; ul < m_ulEdges; ul++)
	{
		CExpression *pexprEdge = (*pdrgpexprConj)[ul];
		pexprEdge->AddRef();
		m_rgpedge[ul] = GPOS_NEW(pmp) SEdge(pmp, pexprEdge);
//		m_rgpedge[ul]->OsPrint(at.Os());
	}
	
	pdrgpexpr->Release();
	pdrgpexprConj->Release();

	//  ComputeEdgeCover populates the bitset of the SEdge object
	// marking what all Components use the edges
	//Edge : {0, 1} Hash:1050648
	//+--CScalarCmp (=)   origin: [Grp:5, GrpExpr:0]
	//   |--CScalarIdent "b" (1)   origin: [Grp:3, GrpExpr:0]
	//   +--CScalarIdent "b" (16)   origin: [Grp:4, GrpExpr:0]
	//
	//Edge : {1, 2} Hash:1050672
	//+--CScalarCmp (=)   origin: [Grp:7, GrpExpr:0]
	//   |--CScalarIdent "b" (16)   origin: [Grp:4, GrpExpr:0]
	//   +--CScalarIdent "b" (31)   origin: [Grp:6, GrpExpr:0]
	//
	//Edge : {0, 2} Hash:1050664
	//+--CScalarCmp (=)   origin: [Grp:8, GrpExpr:0]
	//   |--CScalarIdent "b" (1)   origin: [Grp:3, GrpExpr:0]
	//   +--CScalarIdent "b" (31)   origin: [Grp:6, GrpExpr:0]
	ComputeEdgeCover();
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::~CJoinOrder
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrder::~CJoinOrder()
{
	for (ULONG ul = 0; ul < m_ulComps; ul++)
	{
		m_rgpcomp[ul]->Release();
	}
	GPOS_DELETE_ARRAY(m_rgpcomp);

	for (ULONG ul = 0; ul < m_ulEdges; ul++)
	{
		m_rgpedge[ul]->Release();
	}
	GPOS_DELETE_ARRAY(m_rgpedge);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::ComputeEdgeCover
//
//	@doc:
//		Compute cover for each edge
//
//---------------------------------------------------------------------------
void
CJoinOrder::ComputeEdgeCover()
{
	for (ULONG ulEdge = 0; ulEdge < m_ulEdges; ulEdge++)
	{
		CExpression *pexpr = m_rgpedge[ulEdge]->m_pexpr;
		CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexpr->PdpDerive())->PcrsUsed();

		for (ULONG ulComp = 0; ulComp < m_ulComps; ulComp++)
		{
			CExpression *pexprComp = m_rgpcomp[ulComp]->m_pexpr;
			CColRefSet *pcrsOutput = CDrvdPropRelational::Pdprel(pexprComp->PdpDerive())->PcrsOutput();

			if (!pcrsUsed->FDisjoint(pcrsOutput))
			{
				(void) m_rgpedge[ulEdge]->m_pbs->FExchangeSet(ulComp);
			}
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::SortEdges
//
//	@doc:
//		Sort edge array by lengths of edges;
//		Function to be overwritten by more sophisticated subclasses
//
//---------------------------------------------------------------------------
void
CJoinOrder::SortEdges()
{
	clib::QSort(m_rgpedge, m_ulEdges, GPOS_SIZEOF(m_rgpedge[0]), ICmpEdgesByLength);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::MergeComponents
//
//	@doc:
//		Build join order by establishing an order among the edges, then
//		apply each edge by merging spanned components
//
//---------------------------------------------------------------------------
void
CJoinOrder::MergeComponents()
{
	SortEdges();
	
	ULONG ulEdge = 0;
	while (ulEdge < m_ulEdges)
	{
		// array for conjuncts
		DrgPexpr *pdrgpexpr = GPOS_NEW(m_pmp) DrgPexpr(m_pmp);
		
		// next cover
		CBitSet *pbsCover = m_rgpedge[ulEdge]->m_pbs;

		CExpression *pexprConj = m_rgpedge[ulEdge]->m_pexpr;
		pexprConj->AddRef();
		pdrgpexpr->Append(pexprConj);
		
		// gobble up subsequent edges with exactly the same coverage
		while (ulEdge < m_ulEdges - 1 && 
			   m_rgpedge[ulEdge + 1]->m_pbs->FEqual(pbsCover))
		{
			pexprConj = m_rgpedge[ulEdge + 1]->m_pexpr;
			pexprConj->AddRef();
			pdrgpexpr->Append(pexprConj);
			ulEdge++;
		}
		
		// create new join(s)
		AddEdge(m_rgpedge[ulEdge], pdrgpexpr);
		ulEdge++;
	}
}		


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::AddEdge
//
//	@doc:
//		Process an edge, add Select or Join, then recompute components 
//		and new cover
//
//---------------------------------------------------------------------------
void
CJoinOrder::AddEdge
	(
	CJoinOrder::SEdge *pedge,
	DrgPexpr *pdrgpexpr
	)
{
	// by default start with first component; if edge doesn't span any 
	// components, it suffices to apply to first component
	SComponent *pcomp = m_rgpcomp[0];

	CBitSetIter bsiterEdge(*pedge->m_pbs);
	if (bsiterEdge.FAdvance())
	{
		pcomp = m_rgpcomp[bsiterEdge.UlBit()];
	}

	// edge fully subsumed by first component's cover
	if (pcomp->m_pbs->FSubset(pedge->m_pbs))
	{
		CExpression *pexprPred = CPredicateUtils::PexprConjunction(m_pmp, pdrgpexpr);
		if (CUtils::FScalarConstTrue(pexprPred))
		{
			pexprPred->Release();
		}
		else
		{
			CExpression *pexprRel = pcomp->m_pexpr;
			pcomp->m_pexpr = CUtils::PexprCollapseSelect(m_pmp, pexprRel, pexprPred);
			pexprRel->Release();
			pexprPred->Release();
		}

		return;
	}
	
	// subtract component cover from edge cover
	CBitSet *pbsCover = GPOS_NEW(m_pmp) CBitSet(m_pmp, *pedge->m_pbs);
	GPOS_ASSERT(0 < pbsCover->CElements());

	pbsCover->Difference(pcomp->m_pbs);
	ULONG ulLength = pbsCover->CElements();
	
	// iterate through remaining edge cover
	BOOL fCovered = false;
	CBitSetIter bsiter(*pbsCover);	
	while (bsiter.FAdvance() && !fCovered)
	{
		ULONG ulComp = bsiter.UlBit();
		GPOS_ASSERT(!pcomp->m_pbs->FBit(ulComp));

		SComponent *pcompOther = m_rgpcomp[ulComp];

		// add predicate after all required components are merged
		DrgPexpr *pdrgpexprFinal = NULL;
		ulLength --;
		if (0 == ulLength)
		{
			// reached end of edge cover, use edge's predicate
			pdrgpexprFinal = pdrgpexpr;
		}
		else
		{
			// determine if combining components can produce edge
			// coverage without going through further iterations
			CBitSet *pbsCombined = GPOS_NEW(m_pmp) CBitSet(m_pmp);
			pbsCombined->Union(pcomp->m_pbs);
			pbsCombined->Union(pcompOther->m_pbs);
			fCovered = pbsCombined->FSubset(pbsCover);
			pbsCombined->Release();
			if (fCovered)
			{
				// edge is covered by combining components, use edges's predicate
				pdrgpexprFinal = pdrgpexpr;
			}
		}

		CombineComponents(pcomp, pcompOther, pdrgpexprFinal);
	}
	
	pbsCover->Release();
}



//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::CombineComponents
//
//	@doc:
//		Merge individual disjoint components
//
//---------------------------------------------------------------------------
void
CJoinOrder::CombineComponents
	(
	SComponent *pcompLeft,
	SComponent *pcompRight,
	DrgPexpr *pdrgpexpr
	)
{
	GPOS_ASSERT_IMP(!pcompLeft->m_pbs->FDisjoint(pcompRight->m_pbs),
					pcompLeft->m_pbs->FEqual(pcompRight->m_pbs));

	// special case when coalescing components at end of algorithm
	if (pcompLeft->m_pbs->FEqual(pcompRight->m_pbs))
	{
		return;
	}
	
	// assemble join & update component
	pcompLeft->m_pexpr->AddRef();
	pcompRight->m_pexpr->AddRef();
	
	CExpression *pexprResult = 
		GPOS_NEW(m_pmp) CExpression(m_pmp, 
							   GPOS_NEW(m_pmp) CLogicalInnerJoin(m_pmp),
							   pcompLeft->m_pexpr,
							   pcompRight->m_pexpr,
							   CPredicateUtils::PexprConjunction(m_pmp, pdrgpexpr));
	
	pcompLeft->m_pexpr->Release();
	pcompLeft->m_pexpr = pexprResult;

	// adjust cover information
	pcompLeft->m_pbs->Union(pcompRight->m_pbs);
		
	// extra reference since we unlink all references to right component below
	pcompRight->AddRef();

	// replace all pointers to right component with pointers to left component
	CBitSetIter bsiter(*pcompRight->m_pbs);
	while (bsiter.FAdvance())
	{
		ULONG ulPos = bsiter.UlBit();
	
		GPOS_ASSERT(m_rgpcomp[ulPos] == pcompRight);
		m_rgpcomp[ulPos]->Release();
		
		pcompLeft->AddRef();
		m_rgpcomp[ulPos] = pcompLeft;
	}
	
	pcompRight->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::PexprExpand
//
//	@doc:
//		Create actual join order
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrder::PexprExpand()
{
	// create joins for all connected components
	MergeComponents();
	
	// stitch resulting components together with cross joins if needed
	for (ULONG ul = 0; ul < m_ulComps - 1; ul++)
	{
		CombineComponents(m_rgpcomp[ul], m_rgpcomp[ul + 1], NULL /*pdrgpexr*/);
	}

	CExpression *pexpr = m_rgpcomp[0]->m_pexpr;
	pexpr->AddRef();	
	
	// return last surviving component
	return pexpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrder::OsPrint
//
//	@doc:
//		Helper function to print a join order class
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrder::OsPrint
	(
	IOstream &os
	)
	const
{
	os	
		<< "Join Order: " << std::endl
		<< "Edges: " << m_ulEdges << std::endl;
		
	for (ULONG ul = 0; ul < m_ulEdges; ul++)
	{
		m_rgpedge[ul]->OsPrint(os);
		os << std::endl;
	}

	os << "Components: " << m_ulComps << std::endl;
	for (ULONG ul = 0; ul < m_ulComps; ul++)
	{
		os << (void*)m_rgpcomp[ul] << " - " << std::endl;
		m_rgpcomp[ul]->OsPrint(os);
	}
	
	return os;
}


// EOF
