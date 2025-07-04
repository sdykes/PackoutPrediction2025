SELECT 
	SUM(NoOfBins) AS SeasonsBins
FROM ma_Bin_DeliveryT
WHERE SeasonID = 2012
AND PresizeFlag = 0

