source(output(
		shipt_id as integer,
		shipt_method as string,
		shipt_provider as string,
		shipt_status as string,
		shipt_estimated_date as date,
		shipt_actual_date as date,
		shipt_cost as decimal(18,0)
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'parquet') ~> sourceShipment1
source(output(
		shipt_id as integer,
		shipt_method as string,
		shipt_provider as string,
		shipt_status as string,
		shipt_estimated_date as date,
		shipt_actual_date as date,
		shipt_cost as decimal(18,0)
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'parquet') ~> sourceShipment2
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
		track_id as integer,
		sup_id as integer,
		track_order_date as date,
		track_expected_date as date,
		track_actual_date as date,
		track_total_price as decimal(18,0),
		track_status as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'parquet') ~> sourceSupTrack1
source(output(
		track_id as integer,
		sup_id as integer,
		track_order_date as date,
		track_expected_date as date,
		track_actual_date as date,
		track_total_price as decimal(18,0),
		track_status as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'parquet') ~> sourceSupTrack2
source(output(
		track_id as integer,
		sup_id as integer,
		track_order_date as date,
		track_expected_date as date,
		track_actual_date as date,
		track_total_price as decimal(18,0),
		track_status as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'parquet') ~> sourceSupTrack3
sourceShipment1 select(mapColumn(
		time_date = shipt_estimated_date
	),
	skipDuplicateMapInputs: false,
	skipDuplicateMapOutputs: true) ~> selectShiptExpected
sourceShipment2 select(mapColumn(
		time_date = shipt_actual_date
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> selectShiptActual
sourceOrder select(mapColumn(
		time_date = ord_date
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> selectOrder
selectShiptExpected, selectShiptActual, selectOrder, selectSupTrackOrder, selectSupTrackExpect, selectSupTrackActual union(byName: true)~> union1
union1 aggregate(groupBy(time_date),
	count = count(time_date)) ~> aggregate1
sourceSupTrack1 select(mapColumn(
		time_date = track_order_date
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> selectSupTrackOrder
sourceSupTrack2 select(mapColumn(
		time_date = track_expected_date
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> selectSupTrackExpect
sourceSupTrack3 select(mapColumn(
		time_date = track_actual_date
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> selectSupTrackActual
filter1 keyGenerate(output(time_id as long),
	startAt: 1L,
	stepValue: 1L) ~> surrogateKey1
surrogateKey1 select(mapColumn(
		time_id,
		time_date
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select1
select1 derive(time_day = dayOfMonth(time_date),
		time_month = month(time_date),
		time_year = year(time_date),
		time_day_of_week = dayOfWeek(time_date)) ~> derivedColumn1
derivedColumn1 sort(asc(time_id, true)) ~> sort1
aggregate1 filter(isNull(time_date) == false()) ~> filter1
sort1 derive(time_quarter = case(time_month<=3, 1, case(time_month<=6, 2, case(time_month<=9, 3, 4)))) ~> derivedColumn2
derivedColumn2 sink(allowSchemaDrift: true,
	validateSchema: false,
	input(
		time_id as integer,
		time_date as date,
		time_day as integer,
		time_month as integer,
		time_year as integer,
		time_quarter as integer,
		time_day_of_week as string
	),
	deletable:false,
	insertable:true,
	updateable:false,
	upsertable:false,
	format: 'table',
	staged: true,
	allowCopyCommand: true,
	preSQLs:['SET IDENTITY_INSERT [dbo].[Dim_Time] ON'],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	errorHandlingOption: 'stopOnFirstError') ~> sink