SELECT 
	gb.GraderBatchID
	,bu.BinsTipped
	,gb.InputKgs
	,gb.WasteOtherKgs+COALESCE(jkg.JuiceKgs,0)+COALESCE(skg.SampleKgs,0) AS RejectKgs
	,gb.StorageTypeID
	,cop.CompanyName AS [Packing site]
	,ExternalRun
FROM ma_Grader_BatchT AS gb
INNER JOIN
	sw_CompanyT AS cop
ON cop.CompanyID = gb.PackingCompanyID
/* Bins tipped */
LEFT JOIN
	(
	SELECT 
		GraderBatchID,
		SUM(BinQty) AS BinsTipped
	FROM ma_Bin_UsageT
	WHERE GraderBatchID IS NOT NULL
	GROUP BY GraderBatchID
	) AS bu
ON bu.GraderBatchID = gb.GraderBatchID
/* Juice Kgs */
LEFT JOIN
	(
	SELECT 
		PresizeOutputFromGraderBatchID AS GraderBatchID
		,SUM(TotalWeight) AS JuiceKgs
	FROM ma_Bin_DeliveryT AS bd
	INNER JOIN
		sw_ProductT AS pr
	ON pr.ProductID = bd.PresizeProductID
	INNER JOIN
		sw_GradeT AS gt
	ON gt.GradeID = pr.GradeID
	WHERE gt.JuiceFlag = 1
	GROUP BY PresizeOutputFromGraderBatchID
	) AS jkg
ON jkg.GraderBatchID = gb.GraderBatchID
/* Sample Kgs */
LEFT JOIN
	(
	SELECT
		GraderBatchID
		,SUM(NoOfUnits*NetFruitWeight) AS SampleKgs
	FROM ma_Pallet_DetailT AS pd
	INNER JOIN
		sw_ProductT AS pr
	ON pr.ProductID = pd.ProductID
	WHERE SampleFlag = 1
	GROUP BY GraderBatchID
	) AS skg
ON skg.GraderBatchID = gb.GraderBatchID
WHERE SeasonID = 2012
AND ClosedDateTime IS NOT NULL
AND PresizeInputFlag = 0

