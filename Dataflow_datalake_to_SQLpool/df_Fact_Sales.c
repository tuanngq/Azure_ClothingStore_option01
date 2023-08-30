source(output(
		ord_id as integer,
		ord_date as date,
		ord_total_payment as decimal(18,0),
		ord_shipping_fee as decimal(18,0),
		ord_total_discount as decimal(18,0),
		cus_id as integer,
		emp_id as integer,
		shipt_id as integer,
		ord_channel as string,
		ord_pay_method as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'parquet') ~> sourceOrder
source(output(
		ord_detail_id as integer,
		ord_detail_quantity as integer,
		ord_detail_price as decimal(18,0),
		prod_id as integer,
		ord_id as integer,
		ord_detail_sale_amount as decimal(18,0),
		ord_detail_sale_price as decimal(18,0)
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'parquet') ~> sourceOrderDetail
source(output(
		emp_id as integer,
		emp_name as string,
		emp_date_of_birth as date,
		emp_address as string,
		emp_phone as string,
		emp_email as string,
		emp_salary as decimal(18,0),
		emp_position as string,
		bran_id as integer,
		emp_join_date as date,
		emp_type as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'parquet') ~> sourceEmployee
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
	format: 'parquet') ~> sourceInventory
source(output(
		track_detail_id as integer,
		track_id as integer,
		track_detail_quantity as integer,
		track_detail_cost_per_unit as decimal(18,0),
		tracK_detail_total_cost as decimal(18,0),
		inv_id as integer
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'parquet') ~> sourceTrackDetail
source(output(
		time_id as integer,
		time_date as date,
		time_day as integer,
		time_month as integer,
		time_year as integer,
		time_quarter as integer,
		time_day_of_week as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	isolationLevel: 'READ_UNCOMMITTED',
	format: 'table',
	staged: true) ~> sourceDimTime
sourceOrder, sourceOrderDetail join(sourceOrder@ord_id == sourceOrderDetail@ord_id,
	joinType:'inner',
	matchType:'exact',
	ignoreSpaces: false,
	broadcast: 'auto')~> join1
join1 select(mapColumn(
		ord_id = sourceOrder@ord_id,
		ord_date,
		prod_id,
		cus_id,
		emp_id,
		shipt_id,
		sale_original_price = ord_detail_sale_price,
		sale_discounted_price = ord_detail_price,
		sale_quantity = ord_detail_quantity,
		sale_channel = ord_channel,
		sale_amount = ord_detail_sale_amount,
		sale_pay_method = ord_pay_method
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select1
select1, sourceEmployee join(select1@emp_id == sourceEmployee@emp_id,
	joinType:'inner',
	matchType:'exact',
	ignoreSpaces: false,
	broadcast: 'auto')~> join2
join2 select(mapColumn(
		ord_id,
		ord_date,
		prod_id,
		cus_id,
		emp_id = select1@emp_id,
		bran_id,
		shipt_id,
		sale_original_price,
		sale_discounted_price,
		sale_quantity,
		sale_channel,
		sale_amount,
		sale_pay_method
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select2
sourceInventory, sourceTrackDetail join(sourceInventory@inv_id == sourceTrackDetail@inv_id,
	joinType:'inner',
	matchType:'exact',
	ignoreSpaces: false,
	broadcast: 'auto')~> join3
join3 select(mapColumn(
		prod_id,
		track_detail_cost_per_unit
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select3
select3 keyGenerate(output(id as long),
	startAt: 1L,
	stepValue: 1L) ~> surrogateKey1
surrogateKey1 filter(id<=140) ~> filter1
select2, filter1 join(select2@prod_id == select3@prod_id,
	joinType:'inner',
	matchType:'exact',
	ignoreSpaces: false,
	broadcast: 'auto')~> joinGetProdCost
joinGetProdCost select(mapColumn(
		ord_id,
		ord_date,
		prod_id = select2@prod_id,
		cus_id,
		emp_id,
		bran_id,
		shipt_id,
		sale_original_price,
		sale_discounted_price,
		sale_quantity,
		sale_channel,
		sale_amount,
		sale_unit_cost = track_detail_cost_per_unit,
		sale_pay_method
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select4
select4 derive(sale_profit = (sale_discounted_price-sale_unit_cost)*sale_quantity) ~> derivedColumn1
sourceDimTime select(mapColumn(
		time_id,
		time_date
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select5
derivedColumn1, select5 join(ord_date == time_date,
	joinType:'inner',
	matchType:'exact',
	ignoreSpaces: false,
	broadcast: 'auto')~> join4
join4 select(mapColumn(
		ord_id,
		order_time_id = time_id,
		prod_id,
		cus_id,
		emp_id,
		bran_id,
		shipt_id,
		sale_original_price,
		sale_discounted_price,
		sale_quantity,
		sale_channel,
		sale_amount,
		sale_unit_cost,
		sale_pay_method,
		sale_profit
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select6
select6 sink(allowSchemaDrift: true,
	validateSchema: false,
	input(
		ord_id as integer,
		order_time_id as integer,
		prod_id as integer,
		cus_id as integer,
		emp_id as integer,
		bran_id as integer,
		shipt_id as integer,
		sale_original_price as decimal(18,0),
		sale_discounted_price as decimal(18,0),
		sale_quantity as integer,
		sale_channel as string,
		sale_amount as decimal(18,0),
		sale_unit_cost as decimal(18,0),
		sale_profit as decimal(18,0),
		sale_pay_method as string
	),
	deletable:false,
	insertable:true,
	updateable:false,
	upsertable:false,
	format: 'table',
	staged: true,
	allowCopyCommand: true,
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	errorHandlingOption: 'stopOnFirstError') ~> sink