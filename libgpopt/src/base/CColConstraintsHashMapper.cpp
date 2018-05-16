//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#include "gpos/common/CAutoRef.h"
#include "gpopt/base/CColConstraintsHashMapper.h"

using namespace gpopt;

DrgPcnstr *
CColConstraintsHashMapper::PdrgPcnstrLookup
	(
		CColRef *pcr
	)
{
	DrgPcnstr *pdrgpcnstrCol = m_phmColConstr->Find(pcr);
	pdrgpcnstrCol->AddRef();
	return pdrgpcnstrCol;
}

// mapping between columns and single column constraints in array of constraints
static
HMColConstr *
PhmcolconstrSingleColConstr
	(
		IMemoryPool *pmp,
		DrgPcnstr *drgPcnstr
	)
{
	CAutoRef<DrgPcnstr> arpdrgpcnstr(drgPcnstr);
	HMColConstr *phmcolconstr = GPOS_NEW(pmp) HMColConstr(pmp);

	const ULONG ulLen = arpdrgpcnstr->Size();

	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CConstraint *pcnstrChild = (*arpdrgpcnstr)[ul];
		CColRefSet *pcrs = pcnstrChild->PcrsUsed();

		if (1 == pcrs->Size())
		{
			CColRef *pcr = pcrs->PcrFirst();
			DrgPcnstr *pcnstrMapped = phmcolconstr->Find(pcr);
			if (NULL == pcnstrMapped)
			{
				pcnstrMapped = GPOS_NEW(pmp) DrgPcnstr(pmp);
				phmcolconstr->Insert(pcr, pcnstrMapped);
			}
			pcnstrChild->AddRef();
			pcnstrMapped->Append(pcnstrChild);
		}
	}

	return phmcolconstr;
}

CColConstraintsHashMapper::CColConstraintsHashMapper
	(
		IMemoryPool *pmp,
		DrgPcnstr *pdrgpcnstr
	) :
	m_phmColConstr(PhmcolconstrSingleColConstr(pmp, pdrgpcnstr))
{
}

CColConstraintsHashMapper::~CColConstraintsHashMapper()
{
	m_phmColConstr->Release();
}
