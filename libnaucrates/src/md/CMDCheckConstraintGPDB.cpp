//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDCheckConstraintGPDB.cpp
//
//	@doc:
//		Implementation of the class representing a GPDB check constraint
//		in a metadata cache relation
//---------------------------------------------------------------------------

#include "naucrates/md/CMDCheckConstraintGPDB.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

#include "gpopt/translate/CTranslatorDXLToExpr.h"

using namespace gpdxl;
using namespace gpmd;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CMDCheckConstraintGPDB::CMDCheckConstraintGPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDCheckConstraintGPDB::CMDCheckConstraintGPDB
	(
	IMemoryPool *memory_pool,
	IMDId *pmdid,
	CMDName *pmdname,
	IMDId *pmdidRel,
	CDXLNode *pdxln
	)
	:
	m_memory_pool(memory_pool),
	m_pmdid(pmdid),
	m_pmdname(pmdname),
	m_pmdidRel(pmdidRel),
	m_pdxln(pdxln)
{
	GPOS_ASSERT(pmdid->IsValid());
	GPOS_ASSERT(pmdidRel->IsValid());
	GPOS_ASSERT(NULL != pmdname);
	GPOS_ASSERT(NULL != pdxln);

	m_pstr = CDXLUtils::SerializeMDObj(m_memory_pool, this, false /*fSerializeHeader*/, false /*indentation*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDCheckConstraintGPDB::~CMDCheckConstraintGPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDCheckConstraintGPDB::~CMDCheckConstraintGPDB()
{
	GPOS_DELETE(m_pmdname);
	GPOS_DELETE(m_pstr);
	m_pmdid->Release();
	m_pmdidRel->Release();
	m_pdxln->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDCheckConstraintGPDB::Pexpr
//
//	@doc:
//		Scalar expression of the check constraint
//
//---------------------------------------------------------------------------
CExpression *
CMDCheckConstraintGPDB::Pexpr
	(
	IMemoryPool *memory_pool,
	CMDAccessor *pmda,
	DrgPcr *pdrgpcr
	)
	const
{
	GPOS_ASSERT(NULL != pdrgpcr);

	const IMDRelation *pmdrel = pmda->Pmdrel(m_pmdidRel);
#ifdef GPOS_DEBUG
	const ULONG ulLen = pdrgpcr->Size();
	GPOS_ASSERT(ulLen > 0);

	const ULONG ulArity = pmdrel->UlNonDroppedCols() - pmdrel->UlSystemColumns();
	GPOS_ASSERT(ulArity == ulLen);
#endif // GPOS_DEBUG

	// translate the DXL representation of the check constraint expression
	CTranslatorDXLToExpr dxltr(memory_pool, pmda);
	return dxltr.PexprTranslateScalar(m_pdxln, pdrgpcr, pmdrel->PdrgpulNonDroppedCols());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDCheckConstraintGPDB::Serialize
//
//	@doc:
//		Serialize check constraint in DXL format
//
//---------------------------------------------------------------------------
void
CMDCheckConstraintGPDB::Serialize
	(
	CXMLSerializer *xml_serializer
	)
	const
{
	xml_serializer->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
						CDXLTokens::PstrToken(EdxltokenCheckConstraint));

	m_pmdid->Serialize(xml_serializer, CDXLTokens::PstrToken(EdxltokenMdid));
	xml_serializer->AddAttribute(CDXLTokens::PstrToken(EdxltokenName), m_pmdname->Pstr());
	m_pmdidRel->Serialize(xml_serializer, CDXLTokens::PstrToken(EdxltokenRelationMdid));

	// serialize the scalar expression
	m_pdxln->SerializeToDXL(xml_serializer);

	xml_serializer->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
						CDXLTokens::PstrToken(EdxltokenCheckConstraint));
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CMDCheckConstraintGPDB::DebugPrint
//
//	@doc:
//		Prints a MD constraint to the provided output
//
//---------------------------------------------------------------------------
void
CMDCheckConstraintGPDB::DebugPrint
	(
	IOstream &os
	)
	const
{
	os << "Constraint Id: ";
	MDId()->OsPrint(os);
	os << std::endl;

	os << "Constraint Name: " << (Mdname()).Pstr()->GetBuffer() << std::endl;

	os << "Relation id: ";
	PmdidRel()->OsPrint(os);
	os << std::endl;
}

#endif // GPOS_DEBUG

// EOF
