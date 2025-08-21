CREATE FUNCTION RTSNG_PRD.RTSNG_F_GETCANCELTRAIN (
    @noka VARCHAR(50),
    @tripdate DATE,
    @orderorg INT,
    @orderdes INT
)
RETURNS INT
AS
BEGIN
    DECLARE @v_result INT = 0;

    IF EXISTS (
        SELECT 1
        FROM RTSNG_PRD.RTSNG_T_CANCELTRAIN
        WHERE 
            C_CANCELTRAIN_NOKA = @noka
            AND C_CANCELTRAIN_STATUS = '1'
            AND @tripdate BETWEEN C_CANCELTRAIN_STARTDATE AND C_CANCELTRAIN_ENDDATE
            AND (
                (@orderorg BETWEEN C_STASIUN_ORDERORG AND C_STASIUN_ORDERDES)
                OR (@orderdes BETWEEN C_STASIUN_ORDERORG AND C_STASIUN_ORDERDES)
            )
            AND NOT (
                (@orderorg = C_STASIUN_ORDERDES AND C_STASIUN_DESSTART = '1')
                OR (@orderdes = C_STASIUN_ORDERORG AND C_STASIUN_ORGSTOP = '1')
            )
    )
    BEGIN
        SET @v_result = 1;
    END

    RETURN @v_result;
END