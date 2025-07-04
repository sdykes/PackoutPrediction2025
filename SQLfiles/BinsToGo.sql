SELECT 
	bd.BinDeliveryID
	,HarvestDate
	,NoOfBins
	,BinsToGo
	,bd.StorageTypeID
	,co.CompanyName AS [Storage site]
FROM ma_Bin_DeliveryT AS bd
INNER JOIN
	(
	SELECT 
		BinDeliveryID
		,SUM(BinQty) AS BinsToGo
	FROM ma_Bin_UsageT
	WHERE GraderBatchID IS NULL
	GROUP BY BinDeliveryID
	) AS bu
ON bu.BinDeliveryID = bd.BinDeliveryID
INNER JOIN
	sw_CompanyT AS co
ON co.CompanyID = bd.FirstStorageSiteCompanyID
WHERE PresizeFlag = 0
AND SeasonID = 2012


