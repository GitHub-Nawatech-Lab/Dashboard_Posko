CREATE TABLE [RTSNG_PRD].[RTSNG_T_SHIFT] (

	[C_SHIFT_ID] bigint NULL, 
	[C_SHIFT_NAME] varchar(50) NULL, 
	[C_SHIFT_OPENON] datetime2(6) NULL, 
	[C_SHIFT_CLOSEDON] datetime2(6) NULL, 
	[C_SHIFT_STATUS] varchar(1) NULL, 
	[C_SHIFT_DOMAIN] bigint NULL, 
	[C_SHIFT_MODIFIEDBY] bigint NULL, 
	[C_SHIFT_MODIFIEDON] datetime2(6) NULL, 
	[C_USER_ID] bigint NULL, 
	[C_UNIT_ID] bigint NULL, 
	[C_SHIFT_CREATEDBY] bigint NULL, 
	[C_SHIFT_CREATEDON] datetime2(6) NULL, 
	[C_USER_FULLNAME] varchar(100) NULL, 
	[C_UNIT_CODE] varchar(30) NULL, 
	[C_STASIUN_ID] bigint NULL, 
	[C_STASIUN_CODE] varchar(30) NULL, 
	[C_USER_USERNAME] varchar(50) NULL
);