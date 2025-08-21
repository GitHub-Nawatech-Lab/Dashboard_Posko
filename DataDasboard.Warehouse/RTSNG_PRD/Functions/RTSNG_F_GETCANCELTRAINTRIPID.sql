CREATE FUNCTION RTSNG_PRD.RTSNG_F_GETCANCELTRAINTRIPID (
    @tripid INT,
    @orderorg INT,
    @orderdes INT
)
RETURNS INT
AS
BEGIN
    DECLARE @v_result INT = 0;
    DECLARE @noka VARCHAR(10);
    DECLARE @tripdate DATE;

    -- Get schedule noka and trip date
    SELECT 
        @noka = C_SCHEDULE_NOKA,
        @tripdate = C_TRIP_DATE
    FROM RTSNG_PRD.RTSNG_T_TRIP
    WHERE C_TRIP_ID = @tripid;

    -- Check cancellation logic
    IF EXISTS (
        SELECT 1
        FROM RTSNG_PRD.RTSNG_T_CANCELTRAIN CT
        WHERE 
            CT.C_CANCELTRAIN_NOKA = @noka
            AND CT.C_CANCELTRAIN_STATUS = '1'
            AND @tripdate BETWEEN CT.C_CANCELTRAIN_STARTDATE AND CT.C_CANCELTRAIN_ENDDATE
            AND (
                (@orderorg BETWEEN CT.C_STASIUN_ORDERORG AND CT.C_STASIUN_ORDERDES)
                OR (@orderdes BETWEEN CT.C_STASIUN_ORDERORG AND CT.C_STASIUN_ORDERDES)
            )
            AND NOT (
                (@orderorg = CT.C_STASIUN_ORDERDES AND CT.C_STASIUN_DESSTART = '1')
                OR (@orderdes = CT.C_STASIUN_ORDERORG AND CT.C_STASIUN_ORGSTOP = '1')
            )
    )
    BEGIN
        SET @v_result = 1;
    END

    RETURN @v_result;
END