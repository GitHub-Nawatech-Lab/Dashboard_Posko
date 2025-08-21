-- Create a function to calculate the maximum capacity 
-- of seats available for a specific trip and wagon class
CREATE FUNCTION RTSNG_F_GETMAXCAPCLSNOSEAT (
    @tripid INT,          -- Trip ID for which the capacity is calculated
    @noka VARCHAR(50),    -- Schedule number associated with the trip
    @tripdate DATE,       -- Date of the trip
    @wagonclassid INT     -- ID of the wagon class for which capacity is checked
)
RETURNS INT              -- The function returns an integer value representing the maximum capacity
AS
BEGIN
    -- Initialize variables to hold capacity and seat counts
    DECLARE @capacityperclass INT = 0;  -- Total capacity for the specified wagon class
    DECLARE @jmlnoseat INT = 0;          -- Count of seats that are not opened
    DECLARE @orderorg INT;                -- Order organization ID
    DECLARE @orderdes INT;                -- Order destination ID
    
    -- Get the minimum order organization and maximum order destination for the trip
    SELECT 
        @orderorg = MIN(C_STOP_ORDERORG),  -- Get the minimum order organization
        @orderdes = MAX(C_STOP_ORDERDES)   -- Get the maximum order destination
    FROM RTSNG_T_ALLOCATION
    WHERE C_TRIP_ID = @tripid;             -- Filter by the provided trip ID
    
    -- Count the number of seats that are not opened for the specified trip and wagon class
    SELECT 
        @jmlnoseat = COUNT(*)               -- Count of closed seats
    FROM RTSNG_T_ALLOCBLOCK 
    WHERE 
        C_ALLOCBLOCK_SEATOPEN = '0'        -- Only consider seats that are not opened
        AND C_TRIP_ID = @tripid             -- Filter by the provided trip ID
        AND C_ALLOCBLOCK_STATUS = '1'       -- Only consider active allocation blocks
        AND C_WAGONCLASS_ID = @wagonclassid -- Filter by the specified wagon class ID
        AND C_ALLOCBLOCK_ORDERORG = @orderorg -- Match the order organization
        AND C_ALLOCBLOCK_ORDERDES = @orderdes; -- Match the order destination
    
    -- Calculate the total capacity for the specified wagon class
    SELECT 
        @capacityperclass = ISNULL(SUM(CAST(W.C_WAGON_CAPACITY AS INT)), 0) -- Sum the capacities, default to 0 if NULL
    FROM RTSNG_T_STAMFORMDET S
    JOIN RTSNG_T_WAGON W ON S.C_WAGON_ID = W.C_WAGON_ID -- Join to get wagon capacity
    WHERE 
        S.C_SCHEDULE_NOKA = @noka              -- Filter by the schedule number
        AND S.C_TRIP_DATE = @tripdate          -- Filter by the trip date
        AND S.C_STAMFORMDET_STATUS != '3'      -- Exclude records with status '3'
        AND S.C_WAGONCLASS_ID = @wagonclassid; -- Filter by the specified wagon class ID
    
    -- Return the maximum capacity available by subtracting the number of closed seats from total capacity
    RETURN @capacityperclass - @jmlnoseat;
END;