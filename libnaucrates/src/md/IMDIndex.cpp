//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		IMDIndex.cpp
//
//	@doc:
//		Implementation of MD index
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/IMDIndex.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		IMDRelation::GetDistrPolicyStr
//
//	@doc:
//		Return relation distribution policy as a string value
//
//---------------------------------------------------------------------------
const CWStringConst *
IMDIndex::GetDXLStr(EmdindexType index_type)
{
	switch (index_type)
	{
		case EmdindBtree:
			return CDXLTokens::GetDXLTokenStr(EdxltokenIndexTypeBtree);
		case EmdindBitmap:
			return CDXLTokens::GetDXLTokenStr(EdxltokenIndexTypeBitmap);
		default:
			GPOS_ASSERT(!"Unrecognized index type");
			return NULL;
	}
}

// EOF
