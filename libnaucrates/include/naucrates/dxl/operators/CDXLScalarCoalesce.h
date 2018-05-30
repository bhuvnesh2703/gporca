//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarCoalesce.h
//
//	@doc:
//		Class for representing DXL Coalesce operator
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarCoalesce_H
#define GPDXL_CDXLScalarCoalesce_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarCoalesce
	//
	//	@doc:
	//		Class for representing DXL Coalesce operator
	//
	//---------------------------------------------------------------------------
	class CDXLScalarCoalesce : public CDXLScalar
	{
		private:
			// return type
			IMDId *m_mdid_type;

			// private copy ctor
			CDXLScalarCoalesce(const CDXLScalarCoalesce&);

		public:

			// ctor
			CDXLScalarCoalesce(IMemoryPool *memory_pool, IMDId *mdid_type);

			//dtor
			virtual
			~CDXLScalarCoalesce();

			// name of the operator
			virtual
			const CWStringConst *PstrOpName() const;

			// return type
			virtual
			IMDId *MDIdType() const
			{
				return m_mdid_type;
			}

			// DXL Operator ID
			virtual
			Edxlopid Edxlop() const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *xml_serializer, const CDXLNode *pdxln) const;

			// does the operator return a boolean result
			virtual
			BOOL FBoolean(CMDAccessor *pmda) const;

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *pdxln, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG

			// conversion function
			static
			CDXLScalarCoalesce *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarCoalesce == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarCoalesce*>(pdxlop);
			}
	};
}
#endif // !GPDXL_CDXLScalarCoalesce_H

// EOF
