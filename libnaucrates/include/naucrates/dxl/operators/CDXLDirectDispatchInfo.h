//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLDirectDispatchInfo.h
//
//	@doc:
//		Class for representing the specification of directly dispatchable plans
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLDirectDispatchInfo_H
#define GPDXL_CDXLDirectDispatchInfo_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLDatum.h"

namespace gpdxl
{
	using namespace gpos;

	// fwd decl
	class CXMLSerializer;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLDirectDispatchInfo
	//
	//	@doc:
	//		Class for representing the specification of directly dispatchable plans
	//
	//---------------------------------------------------------------------------
	class CDXLDirectDispatchInfo : public CRefCount
	{
		private:

			// constants for determining segments to dispatch to
			DXLDatumArrays *m_pdrgpdrgpdxldatum;
		
			// private copy ctor
			CDXLDirectDispatchInfo(const CDXLDirectDispatchInfo &);

		public:
			// ctor
			explicit
			CDXLDirectDispatchInfo(DXLDatumArrays *pdrgpdrgpdxldatum);

			// dtor
			virtual
			~CDXLDirectDispatchInfo();

			// accessor to array of datums
			DXLDatumArrays *Pdrgpdrgpdxldatum() const
			{
				return m_pdrgpdrgpdxldatum;
			}
			
			// serialize the datum as the given element
			void Serialize(CXMLSerializer *xml_serializer);

	};
}

#endif // !GPDXL_CDXLDirectDispatchInfo_H

// EOF
