-- drop FUNCTION RTSNG_PRD.RTSNG_F_GETMAXCAPCLSNOSEAT

-- CREATE FUNCTION RTSNG_PRD.RTSNG_F_GETFIRSTDEPARTURE (
--     @TRIPID INT,
--     @STORDERORG INT,
--     @STORDERORGDES INT
-- )
-- RETURNS TABLE
-- AS
-- RETURN (
--     SELECT TOP 1
-- --  C_TRIP_DATE ||' ' ||C_STOP_DEPARTURE
--   CONCAT(
--     CAST(C_TRIP_DATE AS DATE), ' ',
--     STUFF(RIGHT('0000' + C_STOP_DEPARTURE, 4), 3, 0, ':')
--   ) AS DEPARTURE
--   FROM RTSNG_PRD.RTSNG_T_ALLOCATION
--   WHERE C_TRIP_ID = @TRIPID
--     AND C_STOP_ORDERORG = @STORDERORG
--     AND C_STOP_ORDERDES = @STORDERORGDES
--     AND C_ALLOCATION_STATUS IN ('1','2','0')
--   order by C_TRIP_DATE
-- );

CREATE   FUNCTION RTSNG_PRD.RTSNG_F_GETMAXCAPCLSNOSEAT (
    @tripid INT,
    @noka VARCHAR(50),
    @tripdate DATE,
    @wagonclassid_ INT
)
RETURNS TABLE
AS
RETURN (
    WITH ord AS (
        SELECT
            MIN(C_STOP_ORDERORG) AS orderorg,
            MAX(C_STOP_ORDERDES) AS orderdes
        FROM RTSNG_PRD.RTSNG_T_ALLOCATION 
        WHERE C_TRIP_ID = @tripid
    ),
    jmlseat AS (
        SELECT COUNT(*) AS jmlnoseat
        FROM RTSNG_PRD.RTSNG_T_ALLOCBLOCK AB
        JOIN ord ON AB.C_ALLOCBLOCK_ORDERORG = ord.orderorg
                AND AB.C_ALLOCBLOCK_ORDERDES = ord.orderdes
        WHERE AB.C_ALLOCBLOCK_SEATOPEN = '0'
            AND AB.C_TRIP_ID = @tripid
            AND AB.C_ALLOCBLOCK_STATUS = '1'
            AND AB.C_WAGONCLASS_ID = @wagonclassid_
    ),
    capacity AS (
        SELECT SUM(W.C_WAGON_CAPACITY) AS capacityperclass_
        FROM RTSNG_PRD.RTSNG_T_STAMFORMDET S
        JOIN RTSNG_PRD.RTSNG_T_WAGON W ON S.C_WAGON_ID = W.C_WAGON_ID
        WHERE S.C_SCHEDULE_NOKA = @noka
            AND S.C_TRIP_DATE = @tripdate
            AND S.C_STAMFORMDET_STATUS != '3'
            AND S.C_WAGONCLASS_ID = @wagonclassid_
    )
    SELECT 
        ISNULL(capacity.capacityperclass_, 0) - ISNULL(jmlseat.jmlnoseat, 0) AS MAX_CAPACITY
    FROM capacity
    CROSS JOIN jmlseat
);