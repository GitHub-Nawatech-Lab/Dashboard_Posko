CREATE FUNCTION RTSNG_PRD.RTSNG_F_GETSTAMFORMPERCLASSs (
    @noka VARCHAR(20),
    @tripdate DATE,
    @wagonclassid INT
)
RETURNS INT
AS
BEGIN
    DECLARE @countstamformperclass INT = 0;

    SELECT 
        @countstamformperclass = COUNT(S.C_STAMFORMDET_ID)
    FROM RTSNG_PRD.RTSNG_T_STAMFORMDET S
    WHERE S.C_SCHEDULE_NOKA = @noka
      AND S.C_TRIP_DATE = @tripdate
      AND S.C_STAMFORMDET_STATUS IN ('1', '2')
      AND S.C_WAGONCLASS_ID = @wagonclassid;

    RETURN @countstamformperclass;
END;