CREATE FUNCTION RTSNG_PRD.RTSNG_F_GETMAXCAPCLSNOSEATs (
    @tripid INT,
    @noka VARCHAR(20),
    @tripdate DATE,
    @wagonclassid INT
)
RETURNS INT
AS
BEGIN
    DECLARE @capacityperclass INT = 0;
    DECLARE @jmlnoseat INT = 0;
    DECLARE @orderorg INT;
    DECLARE @orderdes INT;

    -- Ambil orderorg dan orderdes
    SELECT 
        @orderorg = MIN(C_STOP_ORDERORG),
        @orderdes = MAX(C_STOP_ORDERDES)
    FROM RTSNG_PRD.RTSNG_T_ALLOCATION
    WHERE C_TRIP_ID = @tripid;

    -- Hitung jumlah seat yang tidak dibuka
    SELECT 
        @jmlnoseat = COUNT(*)
    FROM RTSNG_PRD.RTSNG_T_ALLOCBLOCK
    WHERE C_ALLOCBLOCK_SEATOPEN = '0'
      AND C_TRIP_ID = @tripid
      AND C_ALLOCBLOCK_STATUS = '1'
      AND C_WAGONCLASS_ID = @wagonclassid
      AND C_ALLOCBLOCK_ORDERORG = @orderorg
      AND C_ALLOCBLOCK_ORDERDES = @orderdes;

    -- Hitung total kapasitas berdasarkan STAMFORM
    SELECT 
        @capacityperclass = ISNULL(SUM(W.C_WAGON_CAPACITY), 0)
    FROM RTSNG_PRD.RTSNG_T_STAMFORMDET S
    JOIN RTSNG_PRD.RTSNG_T_WAGON W
        ON S.C_WAGON_ID = W.C_WAGON_ID
    WHERE
        S.C_SCHEDULE_NOKA = @noka
        AND S.C_TRIP_DATE = @tripdate
        AND S.C_STAMFORMDET_STATUS != '3'
        AND S.C_WAGONCLASS_ID = @wagonclassid;

    RETURN @capacityperclass - @jmlnoseat;
END;