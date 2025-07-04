SELECT 
	gb.GraderBatchID
	,InputKgs AS InputKgs
	,WasteOtherKgs+COALESCE(JuiceKgs,0)+COALESCE(SampleKgs,0) AS RejectKgs
	,StorageTypeID
FROM ma_Grader_BatchT AS gb
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
		pd.GraderBatchID
		,SUM(pd.NoOfUnits*pr.NetFruitWeight) AS SampleKgs
	FROM ma_Pallet_detailT AS pd
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

