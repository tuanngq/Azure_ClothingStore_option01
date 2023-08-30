source(output(
		prod_id as integer,
		prod_name as string,
		prod_description as string,
		prod_sale_price as decimal(18,0),
		prod_discounted_price as decimal(18,0),
		cat_id as integer,
		prod_gender as string,
		prod_material as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'parquet') ~> source
source(output(
		cat_id as integer,
		cat_name as string,
		cat_description as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'parquet') ~> sourceCate
source, sourceCate join(source@cat_id == sourceCate@cat_id,
	joinType:'inner',
	matchType:'exact',
	ignoreSpaces: false,
	broadcast: 'auto')~> join1
join1 select(mapColumn(
		prod_id,
		prod_name,
		prod_sale_price,
		prod_discounted_price,
		prod_cat_name = cat_name,
		prod_gender
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select1
select1 derive(prod_discount_percent = case(prod_discounted_price==prod_sale_price, round(0, 2, 5) * 100 , round((prod_sale_price-prod_discounted_price)/prod_sale_price, 2, 5) * 100),
		prod_discount_level = case(prod_discounted_price==prod_sale_price, '0', case((prod_sale_price-prod_discounted_price)/prod_sale_price>=0.5, 'Cao', case((prod_sale_price-prod_discounted_price)/prod_sale_price>=0.3, 'Trung bình', 'Thấp')))) ~> derivedColumn1
derivedColumn1 sink(allowSchemaDrift: true,
	validateSchema: false,
	input(
		prod_id as integer,
		prod_name as string,
		prod_sale_price as decimal(18,0),
		prod_discounted_price as decimal(18,0),
		prod_cat_name as string,
		prod_gender as string,
		prod_discount_percent as double,
		prod_discount_level as string
	),
	deletable:false,
	insertable:true,
	updateable:false,
	upsertable:false,
	format: 'table',
	staged: true,
	allowCopyCommand: true,
	preSQLs:['SET IDENTITY_INSERT [dbo].[Dim_Product] ON'],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	errorHandlingOption: 'stopOnFirstError') ~> sink