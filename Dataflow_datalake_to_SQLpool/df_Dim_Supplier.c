source(output(
		sup_id as integer,
		sup_name as string,
		sup_address as string,
		sup_contact_phone as string,
		sup_contact_email as string,
		sup_contact_name as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'parquet') ~> source
source sink(allowSchemaDrift: true,
	validateSchema: false,
	input(
		sup_id as integer,
		sup_name as string,
		sup_contact_phone as string,
		sup_contact_email as string,
		sup_address as string,
		sup_contact_name as string
	),
	deletable:false,
	insertable:true,
	updateable:false,
	upsertable:false,
	format: 'table',
	staged: true,
	allowCopyCommand: true,
	preSQLs:['SET IDENTITY_INSERT [dbo].[Dim_Supplier] ON'],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	errorHandlingOption: 'stopOnFirstError') ~> sink