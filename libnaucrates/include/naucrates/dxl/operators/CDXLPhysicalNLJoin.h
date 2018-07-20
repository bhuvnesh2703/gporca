//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalNLJoin.h
//
//	@doc:
//		Class for representing DXL nested loop join operators.
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLPhysicalNLJoin_H
#define GPDXL_CDXLPhysicalNLJoin_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLPhysicalJoin.h"
#include "naucrates/dxl/operators/CDXLColRef.h"

namespace gpdxl
{
	// indices of nested loop join elements in the children array
	enum Edxlnlj
	{
		EdxlnljIndexProjList = 0,
		EdxlnljIndexFilter,
		EdxlnljIndexJoinFilter,
		EdxlnljIndexLeftChild,
		EdxlnljIndexRightChild,
		EdxlnljIndexSentinel
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLPhysicalNLJoin
	//
	//	@doc:
	//		Class for representing DXL nested loop join operators
	//
	//---------------------------------------------------------------------------
	class CDXLPhysicalNLJoin : public CDXLPhysicalJoin
	{

		private:

			// flag to indicate whether operator is an index nested loops,
			// i.e., inner side is an index scan that uses values from outer side
			BOOL m_fIndexNLJ;

			DrgPdxlcr *m_nl_params;

			void SerializeNestLoopParamsToDXL(CXMLSerializer *pxmlser) const;

			// private copy ctor
			CDXLPhysicalNLJoin(const CDXLPhysicalNLJoin&);

		public:
			// ctor/dtor
			CDXLPhysicalNLJoin(IMemoryPool *pmp, EdxlJoinType edxljt, BOOL fIndexNLJ);
		
			~CDXLPhysicalNLJoin();
			
			// accessors
			Edxlopid Edxlop() const;
			const CWStringConst *PstrOpName() const;
			
			// is operator an index nested loops?
			BOOL FIndexNLJ() const
			{
				return m_fIndexNLJ;
			}

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser, const CDXLNode *pdxln) const;

			void SetNestLoopParams(DrgPdxlcr *nl_params);

			DrgPdxlcr *GetNestLoopParams()
			{
				return m_nl_params;
			}

			// conversion function
			static
			CDXLPhysicalNLJoin *PdxlConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopPhysicalNLJoin == pdxlop->Edxlop());

				return dynamic_cast<CDXLPhysicalNLJoin*>(pdxlop);
			}

#ifdef GPOS_DEBUG
			// checks whether the operator has valid structure, i.e. number and
			// types of child nodes
			void AssertValid(const CDXLNode *, BOOL fValidateChildren) const;
#endif // GPOS_DEBUG
			
	};
}
#endif // !GPDXL_CDXLPhysicalNLJoin_H

// EOF

