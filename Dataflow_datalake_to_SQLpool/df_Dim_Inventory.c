source(output(
		inv_id as integer,
		inv_quantity_in_stock as integer,
		inv_status as string,
		bran_id as integer,
		prod_id as integer,
		inv_quantity_defective as integer,
		inv_quantity_valid as integer,
		inv_last_stock_update as date,
		inv_min_stock_threshold as integer,
		inv_max_stock_threshold as integer
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'parquet') ~> source
source select(mapColumn(
		inv_id,
		inv_quantity_in_stock,
		bran_id,
		prod_id,
		inv_quantity_defective,
		inv_quantity_valid
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select1
select1 sink(allowSchemaDrift: true,
	validateSchema: false,
	input(
		inv_id as integer,
		inv_quantity_in_stock as integer,
		bran_id as integer,
		prod_id as integer,
		inv_quantity_defective as integer,
		inv_quantity_valid as integer
	),
	deletable:false,
	insertable:true,
	updateable:false,
	upsertable:false,
	format: 'table',
	staged: true,
	allowCopyCommand: true,
	preSQLs:['SET IDENTITY_INSERT [dbo].[Dim_Inventory] ON'],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	errorHandlingOption: 'stopOnFirstError') ~> sink