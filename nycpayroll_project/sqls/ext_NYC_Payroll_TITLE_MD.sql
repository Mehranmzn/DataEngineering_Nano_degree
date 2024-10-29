

CREATE EXTERNAL TABLE [dbo].[NYC_Payroll_TITLE_MD] (
	[TitleCode] [varchar](10) NULL,
	[TitleDescription] [varchar](100) NULL
	)
	WITH (
	LOCATION = 'dirstaging/**',
	DATA_SOURCE = [udacitypayrollanalytic_dirpayrollfiles_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
	)
GO

