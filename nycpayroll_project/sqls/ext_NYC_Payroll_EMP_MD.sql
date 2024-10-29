
CREATE EXTERNAL TABLE [dbo].[NYC_Payroll_EMP_MD] (
	[EmployeeID] [varchar](10) NULL,
	[LastName] [varchar](20) NULL,
	[FirstName] [varchar](20) NULL
	)
	WITH (
	LOCATION = 'dirstaging/**',
	DATA_SOURCE = [udacitypayrollanalytic_dirpayrollfiles_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
	)
GO


SELECT TOP 100 * FROM [dbo].[NYC_Payroll_EMP_MD]
GO