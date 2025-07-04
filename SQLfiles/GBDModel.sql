SELECT 
	gb.GraderBatchID,
	st.SeasonDesc AS Season,
	fa.FarmCode AS RPIN,
	fa.FarmName AS Orchard,
	sb.SubdivisionCode AS [Production site],
	ctp.CompanyName AS [Packing site],
	gb.PackDate,
	gb.HarvestDate AS gbHarvestDate,
	CASE
		WHEN StorageTypeID = 4 THEN 'CA'
		ELSE 'RA'
	END AS [Storage type],
	gb.InputKgs,
	gb.WasteOtherKgs+COALESCE(JuiceKgs,0)+COALESCE(SampleKgs,0) AS RejectKgs,
	gb.ExternalRun
FROM ma_Grader_BatchT AS gb
/* Juice Kgs */
LEFT JOIN
	(
	SELECT 
		PresizeOutputFromGraderBatchID AS GraderBatchID,
		SUM(TotalWeight) AS JuiceKgs
	FROM ma_Bin_DeliveryT AS bd
	INNER JOIN
		sw_ProductT AS pr
	ON pr.ProductID = bd.PresizeProductID
	INNER JOIN
		sw_GradeT AS gt
	ON gt.GradeID = pr.GradeID
	WHERE JuiceFlag = 1
	GROUP BY PresizeOutputFromGraderBatchID
	) AS jkg
ON jkg.GraderBatchID = gb.GraderBatchID
/* SampleKgs */
LEFT JOIN
	(
	SELECT 
		GraderBatchID,
		SUM(NoOfUnits*NetFruitWeight) AS SampleKgs
	FROM ma_Pallet_detailT AS pd
	INNER JOIN
		sw_ProductT AS pr
	ON pr.ProductID = pd.ProductID
	WHERE pr.SampleFlag = 1
	GROUP BY GraderBatchID
	) AS skg
ON skg.GraderBatchID = gb.GraderBatchID
/* Bins Tipped */
LEFT JOIN
	(
	SELECT 
		GraderBatchID,
		SUM(BinQty) AS BinsTipped
	FROM ma_Bin_UsageT
	WHERE GraderBatchID IS NOT NULL
	GROUP BY GraderBatchID
	) AS bins
ON bins.GraderBatchID = gb.GraderBatchID
INNER JOIN
	sw_SeasonT AS st
ON st.SeasonID = gb.SeasonID
INNER JOIN
	sw_FarmT AS fa
ON fa.FarmID = gb.FarmID
INNER JOIN
	sw_SubdivisionT AS sb
ON sb.SubdivisionID = gb.SubdivisionID
INNER JOIN
	sw_CompanyT AS ctp
ON ctp.CompanyID = gb.PackingCompanyID
WHERE PresizeInputFlag = 0
AND ClosedDateTime IS NOT NULL

