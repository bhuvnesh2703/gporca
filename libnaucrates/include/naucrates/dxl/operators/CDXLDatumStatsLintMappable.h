//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal Inc.
//
//	@filename:
//		CDXLDatumStatsLintMappable.h
//
//	@doc:
//		Class for representing DXL datum of types having LINT mapping
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLDatumStatsLintMappable_H
#define GPDXL_CDXLDatumStatsLintMappable_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"
#include "naucrates/dxl/operators/CDXLDatumGeneric.h"

namespace gpdxl
{
	using namespace gpos;

	// fwd decl
	class CXMLSerializer;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLDatumStatsLintMappable
	//
	//	@doc:
	//		Class for representing DXL datum of types having LINT mapping
	//
	//---------------------------------------------------------------------------
	class CDXLDatumStatsLintMappable : public CDXLDatumGeneric
	{
		private:

			// for statistics computation, map to LINT
			LINT m_lValue;

			// private copy ctor
			CDXLDatumStatsLintMappable(const CDXLDatumStatsLintMappable &);

		public:
			// ctor
			CDXLDatumStatsLintMappable
				(
				IMemoryPool *memory_pool,
				IMDId *mdid_type,
				INT type_modifier,
				BOOL fByVal,
				BOOL is_null,
				BYTE *pba,
				ULONG length,
				LINT lValue
				);

			// dtor
			virtual
			~CDXLDatumStatsLintMappable(){};

			// serialize the datum as the given element
			virtual
			void Serialize(CXMLSerializer *xml_serializer);

			// datum type
			virtual
			EdxldatumType GetDatumType() const
			{
				return CDXLDatum::EdxldatumStatsLintMappable;
			}

			// conversion function
			static
			CDXLDatumStatsLintMappable *Cast
				(
				CDXLDatum *datum_dxl
				)
			{
				GPOS_ASSERT(NULL != datum_dxl);
				GPOS_ASSERT(CDXLDatum::EdxldatumStatsLintMappable == datum_dxl->GetDatumType());

				return dynamic_cast<CDXLDatumStatsLintMappable*>(datum_dxl);
			}

			// statistics related APIs

			// can datum be mapped to LINT
			virtual
			BOOL FHasStatsLINTMapping() const
			{
				return true;
			}

			// return the LINT mapping needed for statistics computation
			virtual
			LINT LStatsMapping() const
			{
				return m_lValue;
			}

	};
}

#endif // !GPDXL_CDXLDatumStatsLintMappable_H

// EOF
