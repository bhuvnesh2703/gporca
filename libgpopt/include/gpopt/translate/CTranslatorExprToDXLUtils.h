//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CTranslatorExprToDXLUtils.h
//
//	@doc:
//		Class providing helper methods for translating from Expr to DXL 
//---------------------------------------------------------------------------
#ifndef GPOPT_CTranslatorExprToDXLUtils_H
#define GPOPT_CTranslatorExprToDXLUtils_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLScalarBoolExpr.h"
#include "naucrates/dxl/operators/CDXLColDescr.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/ops.h"

#include "gpopt/translate/CTranslatorExprToDXL.h"

// fwd decl
namespace gpmd
{
	class IMDRelation;
}

namespace gpdxl
{
	class CDXLPhysicalProperties;
	class CDXLScalarProjElem;
}

namespace gpopt
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpdxl;
	using namespace gpnaucrates;

	//---------------------------------------------------------------------------
	//	@class:
	//		CTranslatorExprToDXLUtils
	//
	//	@doc:
	//		Class providing helper methods for translating from Expr to DXL
	//
	//---------------------------------------------------------------------------
	class CTranslatorExprToDXLUtils
	{
		private:
			// construct a scalar comparison of the given type between the 
			// column with the given col id and the scalar expression 
			static
			CDXLNode *PdxlnCmp
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda, 
				ULONG ulPartLevel,
				BOOL fLowerBound,
				CDXLNode *pdxlnScalar, 
				IMDType::ECmpType ecmpt, 
				IMDId *pmdidTypePartKey,
				IMDId *pmdidTypeExpr,
				IMDId *pmdidTypeCastExpr,
				IMDId *pmdidCastFunc
				);
			
			
			// create a column reference
			static
			CColRef *PcrCreate
				(
				IMemoryPool *memory_pool,
				CMDAccessor *pmda,
				CColumnFactory *pcf,
				IMDId *pmdid,
				INT type_modifier,
				const WCHAR *wszName
				);

			// find the partitioning level of the given part key, given the whole
			// array of part keys
			static
			ULONG UlPartKeyLevel(const CColRef *pcr, DrgDrgPcr *pdrgpdrgpcr);

			// construct a test for a partial scan given a part constraint
			static 
			CDXLNode *PdxlnPartialScanTest
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda, 
				CColumnFactory *pcf,
				CConstraint *pcnstr,
				DrgDrgPcr *pdrgpdrgpcrPartKeys,
				BOOL fRangePart
				);
			
			// construct a test for a conjunction or disjunction-based part constraint
			static 
			CDXLNode *PdxlnPartialScanTestConjDisj
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda, 
				CColumnFactory *pcf,
				DrgPcnstr *pdrgpcnstr,
				BOOL fConjunction,
				DrgDrgPcr *pdrgpdrgpcrPartKeys,
				BOOL fRangePart
				);
			
			// construct a test for a conjunction-based part constraint
			static 
			CDXLNode *PdxlnPartialScanTestConjunction
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda, 
				CColumnFactory *pcf,
				CConstraint *pcnstr,
				DrgDrgPcr *pdrgpdrgpcrPartKeys,
				BOOL fRangePart
				);
			
			// construct a test for a disjunction-based part constraint
			static 
			CDXLNode *PdxlnPartialScanTestDisjunction
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda, 
				CColumnFactory *pcf,
				CConstraint *pcnstr,
				DrgDrgPcr *pdrgpdrgpcrPartKeys,
				BOOL fRangePart
				);
			
			// construct a test for a negation-based part constraint
			static 
			CDXLNode *PdxlnPartialScanTestNegation
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda, 
				CColumnFactory *pcf,
				CConstraint *pcnstr,
				DrgDrgPcr *pdrgpdrgpcrPartKeys,
				BOOL fRangePart
				);
			
			// construct a test for an interval-based part constraint
			static 
			CDXLNode *PdxlnPartialScanTestInterval
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda, 
				CConstraint *pcnstr,
				DrgDrgPcr *pdrgpdrgpcrPartKeys,
				BOOL fRangePart
				);

			// construct a test for a range in a part constraint
			static 
			CDXLNode *PdxlnPartialScanTestRange
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda, 
				CRange *prng,
				IMDId *pmdidPartKeyType,
				ULONG ulPartLevel,
				BOOL fRangePart
				);
			
			// construct a test for testing range containment with respect to the
			// start of the range
			static
			CDXLNode *PdxlnRangeStartPredicate
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda, 
				IDatum *pdatum,
				CRange::ERangeInclusion eri,
				IMDId *pmdidPartKeyType,
				ULONG ulPartLevel
				);
			
			
			// construct a test for testing range containment with respect to the
			// end of the range
			static
			CDXLNode *PdxlnRangeEndPredicate
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda, 
				IDatum *pdatum,
				CRange::ERangeInclusion eri,
				IMDId *pmdidPartKeyType,
				ULONG ulPartLevel
				);
			
			// construct a test for testing range containment with respect to the
			// given point in the range using the provided inclusion (<= or >=) 
			// and exclusion comparison operators (< or >)
			static
			CDXLNode *PdxlnRangePointPredicate
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda, 
				IDatum *pdatum,
				CRange::ERangeInclusion eri,
				IMDId *pmdidPartKeyType,
				IMDId *pmdidCmpExcl, 
				IMDId *pmdidCmpIncl,
				ULONG ulPartLevel,
				BOOL fLower
				);
			
			// construct a test for the default partition
			static
			CDXLNode *PdxlnDefaultPartitionTest
				(
				IMemoryPool *memory_pool, 
				ULONG ulPartLevel
				);
			
			// compute a DXL datum from a point constraint
			static
			CDXLDatum *PdxldatumFromPointConstraint
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda,
				const CColRef *pcrDistrCol, 
				CConstraint *pcnstrDistrCol
				);
			
			// compute an array of DXL datum arrays from a disjunction of point constraints
			static
			DXLDatumArrays *PdrgpdrgpdxldatumFromDisjPointConstraint
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda,
				const CColRef *pcrDistrCol, 
				CConstraint *pcnstrDistrCol
				);
			
			// compute the direct dispatch info  from the constraints
			// on the distribution keys
			static
			CDXLDirectDispatchInfo *Pdxlddinfo
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda,
				DrgPexpr *pdrgpexprHashed, 
				CConstraint *pcnstr
				);
			
			// compute the direct dispatch info for a single distribution key from the constraints
			// on the distribution key
			static
			CDXLDirectDispatchInfo *PdxlddinfoSingleDistrKey
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda,
				CExpression *pexprHashed, 
				CConstraint *pcnstr
				);
			
			// check if the given constant value for a particular distribution column can be used
			// to identify which segment to direct dispatch to.
			static
			BOOL FDirectDispatchable(const CColRef *pcrDistrCol, const CDXLDatum *pdxldatum);

		public:
		
			// construct a default properties container
			static
			CDXLPhysicalProperties *Pdxlprop(IMemoryPool *memory_pool);

			// create a scalar const value expression for the given bool value
			static
			CDXLNode *PdxlnBoolConst(IMemoryPool *memory_pool, CMDAccessor *pmda, BOOL fVal);

			// create a scalar const value expression for the given int4 value
			static
			CDXLNode *PdxlnInt4Const(IMemoryPool *memory_pool, CMDAccessor *pmda, INT iVal);

			// construct a filter node for a list partition predicate
			static
			CDXLNode *PdxlnListFilterScCmp
				(
				IMemoryPool *memory_pool,
				CMDAccessor *pmda,
				CDXLNode *pdxlnPartKey,
				CDXLNode *pdxlnScalar,
				IMDId *pmdidTypePartKey,
				IMDId *pmdidTypeOther,
				IMDType::ECmpType ecmpt,
				ULONG ulPartLevel,
				BOOL fHasDefaultPart
				);

			// construct a DXL node for the part key portion of the list partition filter
			static
			CDXLNode *PdxlnListFilterPartKey
				(
				IMemoryPool *memory_pool,
				CMDAccessor *pmda,
				CExpression *pexprPartKey,
				IMDId *pmdidTypePartKey,
				ULONG ulPartLevel
				);

			// construct a filter node for a range predicate
			static
			CDXLNode *PdxlnRangeFilterScCmp
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda, 
				CDXLNode *pdxlnScalar,
				IMDId *pmdidTypePartKey,
				IMDId *pmdidTypeOther,
				IMDId *pmdidTypeCastExpr,
				IMDId *pmdidCastFunc,
				IMDType::ECmpType ecmpt,
				ULONG ulPartLevel
				);
	
			// construct a range filter for an equality comparison
			static
			CDXLNode *PdxlnRangeFilterEqCmp
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda, 
				CDXLNode *pdxlnScalar,
				IMDId *pmdidTypePartKey,
				IMDId *pmdidTypeOther,
				IMDId *pmdidTypeCastExpr,
				IMDId *pmdidCastFunc,
				ULONG ulPartLevel
				);
			
			// construct a predicate for the lower or upper bound of a partition
			static
			CDXLNode *PdxlnRangeFilterPartBound
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda, 
				CDXLNode *pdxlnScalar,
				IMDId *pmdidTypePartKey,
				IMDId *pmdidTypeOther,
				IMDId *pmdidTypeCastExpr,
				IMDId *pmdidCastFunc,
				ULONG ulPartLevel,
				ULONG fLowerBound,
				IMDType::ECmpType ecmpt
				);
			
			// construct predicates to cover the cases of default partition and
			// open-ended partitions if necessary
			static
			CDXLNode *PdxlnRangeFilterDefaultAndOpenEnded
				(
				IMemoryPool *memory_pool, 
				ULONG ulPartLevel,
				BOOL fLTComparison,
				BOOL fGTComparison,
				BOOL fEQComparison,
				BOOL fDefaultPart
				);

			// construct a test for partial scan in the partial partition propagator
			static 
			CDXLNode *PdxlnPartialScanTest
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda, 
				CColumnFactory *pcf,
				const CPartConstraint *ppartcnstr,
				DrgDrgPcr *pdrgpdrgpcrPartKeys,
				CharPtrArray *pdrgszPartTypes
				);
			
			// construct a nested if statement testing the constraints in the 
			// given part index map and propagating to the right part index id
			static
			CDXLNode *PdxlnPropagationExpressionForPartConstraints
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda, 
				CColumnFactory *pcf,
				PartCnstrMap *ppartcnstrmap,
				DrgDrgPcr *pdrgpdrgpcrPartKeys,
				CharPtrArray *pdrgszPartTypes
				);
			
			// check if the DXL Node is a scalar const TRUE
			static
			BOOL FScalarConstTrue(CMDAccessor *pmda, CDXLNode *pdxln);

			// check if the DXL Node is a scalar const false
			static
			BOOL FScalarConstFalse(CMDAccessor *pmda, CDXLNode *pdxln);

			// check whether a project list has the same columns in the given array
			// and in the same order
			static
			BOOL FProjectListMatch(CDXLNode *pdxlnPrL, DrgPcr *pdrgpcr);

			// create a project list by creating references to the columns of the given
			// project list of the child node
			static
			CDXLNode *PdxlnProjListFromChildProjList
				(
				IMemoryPool *memory_pool, 
				CColumnFactory *pcf, 
				HMCrDxln *phmcrdxln, 
				const CDXLNode *pdxlnProjListChild
				);

			// construct the project list of a partition selector
			static
			CDXLNode *PdxlnPrLPartitionSelector
				(
				IMemoryPool *memory_pool,
				CMDAccessor *pmda,
				CColumnFactory *pcf,
				HMCrDxln *phmcrdxln,
				BOOL fUseChildProjList,
				CDXLNode *pdxlnPrLChild,
				CColRef *pcrOid,
				ULONG ulPartLevels,
				BOOL fGeneratePartOid
				);

			// construct the propagation expression for a partition selector
			static
			CDXLNode *PdxlnPropExprPartitionSelector
				(
				IMemoryPool *memory_pool,
				CMDAccessor *pmda,
				CColumnFactory *pcf,
				BOOL fConditional,
				PartCnstrMap *ppartcnstrmap,
				DrgDrgPcr *pdrgpdrgpcrKeys,
				ULONG ulScanId,
				CharPtrArray *pdrgszPartTypes
				);

			// create a DXL project elem node from as a scalar identifier for the
			// child project element node
			static
			CDXLNode *PdxlnProjElem(IMemoryPool *memory_pool, CColumnFactory *pcf, HMCrDxln *phmcrdxln, const CDXLNode *pdxlnProjElemChild);
			
			// create a scalar identifier node for the given column reference
			static
			CDXLNode *PdxlnIdent(IMemoryPool *memory_pool, HMCrDxln *phmcrdxlnSubplans, HMCrDxln *phmcrdxlnIndexLookup, const CColRef *pcr);
			
			// replace subplan entry in the given map with a dxl column reference
			static
			void ReplaceSubplan(IMemoryPool *memory_pool, HMCrDxln *phmcrdxlnSubplans, const CColRef *pcr, CDXLScalarProjElem *pdxlopPrEl);

			// create a project elem from a given col ref
			static
			CDXLNode *PdxlnProjElem(IMemoryPool *memory_pool, HMCrDxln *phmcrdxlnSubplans, const CColRef *pcr);

			// construct an array of NULL datums for a given array of columns
			static
			DrgPdatum *PdrgpdatumNulls(IMemoryPool *memory_pool, DrgPcr *pdrgpcr);

			// map an array of columns to a new array of columns
			static
			DrgPcr *PdrgpcrMapColumns(IMemoryPool *memory_pool, DrgPcr *pdrgpcrInput, HMCrUl *phmcrul, DrgPcr *pdrgpcrMapDest);

			// combine two boolean expressions using the given boolean operator
			static
			CDXLNode *PdxlnCombineBoolean(IMemoryPool *memory_pool, CDXLNode *pdxlnFst, CDXLNode *pdxlnSnd, EdxlBoolExprType boolexptype);

			// construct a partition selector node
			static
			CDXLNode *PdxlnPartitionSelector
				(
				IMemoryPool *memory_pool,
				IMDId *pmdid,
				ULONG ulPartLevels,
				ULONG ulScanId,
				CDXLPhysicalProperties *pdxlprop,
				CDXLNode *pdxlnPrL,
				CDXLNode *pdxlnEqFilters,
				CDXLNode *pdxlnFilters,
				CDXLNode *pdxlnResidual,
				CDXLNode *pdxlnPropagation,
				CDXLNode *pdxlnPrintable,
				CDXLNode *pdxlnChild = NULL
				);

			// create a DXL result node
			static
			CDXLNode *PdxlnResult
				(
				IMemoryPool *memory_pool,
				CDXLPhysicalProperties *pdxlprop,
				CDXLNode *pdxlnPrL,
				CDXLNode *pdxlnFilter,
				CDXLNode *pdxlnOneTimeFilter,
				CDXLNode *pdxlnChild
				);

			// create a DXL ValuesScan node
			static
			CDXLNode *PdxlnValuesScan
				(
				IMemoryPool *memory_pool,
				CDXLPhysicalProperties *pdxlprop,
				CDXLNode *pdxlnPrL,
				DrgPdrgPdatum *pdrgpdrgdatum
				);

			// build hashmap based on a column array, where the key is the column
			// and the value is the index of that column in the array
			static
			HMCrUl *PhmcrulColIndex(IMemoryPool *memory_pool, DrgPcr *pdrgpcr);
			
			// set statistics of the operator
			static
			void SetStats(IMemoryPool *memory_pool, CMDAccessor *pmda, CDXLNode *pdxln, const IStatistics *pstats, BOOL fRoot);

			// set direct dispatch info of the operator
			static
			void SetDirectDispatchInfo
				(
				IMemoryPool *memory_pool, 
				CMDAccessor *pmda, 
				CDXLNode *pdxln, 
				CDrvdPropRelational *pdpRel, 
				DrgPds *pdrgpdsBaseTables
				);
			
			// is the aggregate a local hash aggregate that is safe to stream
			static
			BOOL FLocalHashAggStreamSafe(CExpression *pexprAgg);
			
			// if operator is a scalar cast, extract cast type and function
			static 
			void ExtractCastMdids(COperator *pop, IMDId **ppmdidType, IMDId **ppmdidCastFunc);

			// produce DXL representation of a datum
			static
			CDXLDatum *Pdxldatum
				(
				IMemoryPool *memory_pool,
				CMDAccessor *pmda,
				IDatum *pdatum
				)
			{
				IMDId *pmdid = pdatum->MDId();
				return pmda->Pmdtype(pmdid)->Pdxldatum(memory_pool, pdatum);
			}

			// return a copy the dxl node's physical properties
			static
			CDXLPhysicalProperties *PdxlpropCopy(IMemoryPool *memory_pool, CDXLNode *pdxln);

			// check if given dxl operator exists in the given list
			static
			BOOL FDXLOpExists(const CDXLOperator *pop, const gpdxl::Edxlopid *peopid, ULONG ulOps);

			// check if given dxl node has any operator in the given list
			static
			BOOL FHasDXLOp(const CDXLNode *pdxln, const gpdxl::Edxlopid *peopid, ULONG ulOps);

			// check if the project lists contains subplans with broadcast motion
			static
			BOOL FProjListContainsSubplanWithBroadCast(CDXLNode *pdxlnPrLNew);

			// check if the dxl node imposes a motion hazard
			static
			BOOL FMotionHazard(IMemoryPool *memory_pool, CDXLNode *pdxln, const gpdxl::Edxlopid *peopid, ULONG ulOps, CBitSet *pbsPrjCols);

			// check if the dxl operator does not impose a motion hazard
			static
			BOOL FMotionHazardSafeOp(CDXLNode *pdxln);

			// extract the column ids of the ident from project list
			static
			void ExtractIdentColIds(CDXLNode *pdxln, CBitSet *pbs);
	};
}

#endif // !GPOPT_CTranslatorExprToDXLUtils_H

// EOF
