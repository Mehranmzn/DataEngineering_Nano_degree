

CREATE EXTERNAL TABLE [dbo].[NYC_Payroll_AGENCY_MD] (
	 [AgencyID] [varchar](10) NULL,
    [AgencyName] [varchar](50) NULL
	)
	WITH (
	LOCATION = 'dirstaging/**',
	DATA_SOURCE = [udacitypayrollanalytic_dirpayrollfiles_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
	)
GO

