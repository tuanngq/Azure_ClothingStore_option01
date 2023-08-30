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
	format: 'parquet') ~> source
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
	format: 'parquet') ~> source1
source select(mapColumn(
		shipt_id,
		shipt_method,
		shipt_provider,
		shipt_status,
		shipt_estimated_date,
		shipt_cost
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select1
sourceDimTime select(mapColumn(
		time_id,
		time_date
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select2
select1, select2 join(shipt_estimated_date == time_date,
	joinType:'left',
	matchType:'exact',
	ignoreSpaces: false,
	broadcast: 'auto')~> join1
source1 select(mapColumn(
		shipt_id,
		shipt_actual_date
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select3
select3, select2 join(shipt_actual_date == time_date,
	joinType:'left',
	matchType:'exact',
	ignoreSpaces: false,
	broadcast: 'auto')~> join2
join1 select(mapColumn(
		shipt_id,
		shipt_method,
		shipt_provider,
		shipt_status,
		shipt_cost,
		estimated_time_id = time_id
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select4
join2 select(mapColumn(
		shipt_id,
		actual_time_id = time_id
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select5
select4, select5 join(select4@shipt_id == select5@shipt_id,
	joinType:'inner',
	matchType:'exact',
	ignoreSpaces: false,
	broadcast: 'auto')~> join3
join3 select(mapColumn(
		shipt_id = select4@shipt_id,
		shipt_method,
		shipt_provider,
		shipt_status,
		shipt_cost,
		estimated_time_id,
		actual_time_id
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select6
select6 sink(allowSchemaDrift: true,
	validateSchema: false,
	input(
		shipt_id as integer,
		shipt_method as string,
		shipt_provider as string,
		shipt_status as string,
		estimated_time_id as integer,
		actual_time_id as integer,
		shipt_cost as decimal(18,0)
	),
	deletable:false,
	insertable:true,
	updateable:false,
	upsertable:false,
	format: 'table',
	staged: true,
	allowCopyCommand: true,
	preSQLs:['SET IDENTITY_INSERT [dbo].[Dim_Shipment] ON'],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	errorHandlingOption: 'stopOnFirstError') ~> sink