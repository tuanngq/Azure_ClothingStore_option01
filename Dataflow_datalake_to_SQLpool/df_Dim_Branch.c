source(output(
		bran_id as integer,
		bran_name as string,
		bran_address as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'parquet') ~> sourceBranch
sourceBranch sink(allowSchemaDrift: true,
	validateSchema: false,
	input(
		bran_id as integer,
		bran_name as string,
		bran_address as string
	),
	deletable:false,
	insertable:true,
	updateable:false,
	upsertable:false,
	format: 'table',
	staged: true,
	allowCopyCommand: true,
	preSQLs:['SET IDENTITY_INSERT [dbo].[Dim_Branch] ON'],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	errorHandlingOption: 'stopOnFirstError') ~> sinkBranch