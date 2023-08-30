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
	format: 'parquet') ~> sourceSupTrack
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
	staged: true) ~> sourceDimTime1
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
	staged: true) ~> sourceDimTime2
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
	staged: true) ~> sourceDimTime3
sourceSupTrack select(mapColumn(
		suporder_id = track_id,
		sup_id,
		track_order_date,
		track_expected_date,
		track_actual_date
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> selectSupTrack
sourceTrackDetail select(mapColumn(
		track_detail_id,
		suporder_id = track_id,
		import_quantity = track_detail_quantity,
		import_cost = track_detail_cost_per_unit,
		import_total = tracK_detail_total_cost,
		inv_id
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> selectTrackDetail
selectSupTrack, selectTrackDetail join(selectSupTrack@suporder_id == selectTrackDetail@suporder_id,
	joinType:'inner',
	matchType:'exact',
	ignoreSpaces: false,
	broadcast: 'auto')~> join1
sourceDimTime1 select(mapColumn(
		time_id,
		time_date
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> selectDimTime1
sourceDimTime2 select(mapColumn(
		time_id,
		time_date
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> selectDimTime2
sourceDimTime3 select(mapColumn(
		time_id,
		time_date
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> selectDimTime3
selectDimTime1, join1 join(time_date == track_order_date,
	joinType:'right',
	matchType:'exact',
	ignoreSpaces: false,
	broadcast: 'auto')~> joinOrderDate
joinOrderDate select(mapColumn(
		order_time_id = time_id,
		suporder_id = selectSupTrack@suporder_id
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> selectOrderDate
selectDimTime2, join1 join(time_date == track_expected_date,
	joinType:'right',
	matchType:'exact',
	ignoreSpaces: false,
	broadcast: 'auto')~> joinExpectedTime
joinExpectedTime select(mapColumn(
		expected_time_id = time_id,
		suporder_id = selectTrackDetail@suporder_id
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> selectExpectedTime
selectDimTime3, join1 join(time_date == track_actual_date,
	joinType:'right',
	matchType:'exact',
	ignoreSpaces: false,
	broadcast: 'auto')~> joinActualTime
joinActualTime select(mapColumn(
		actual_time_id = time_id,
		suporder_id = selectTrackDetail@suporder_id
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> selectActualTime
join1, aggregateOrderDate join(selectSupTrack@suporder_id == aggregateOrderDate@suporder_id,
	joinType:'inner',
	matchType:'exact',
	ignoreSpaces: false,
	broadcast: 'auto')~> joinZorderdate
joinZorderdate, aggregateExpectedTime join(selectSupTrack@suporder_id == aggregateExpectedTime@suporder_id,
	joinType:'inner',
	matchType:'exact',
	ignoreSpaces: false,
	broadcast: 'auto')~> joinZexpectedtime
joinZexpectedtime, aggregateActualTime join(selectSupTrack@suporder_id == aggregateActualTime@suporder_id,
	joinType:'inner',
	matchType:'exact',
	ignoreSpaces: false,
	broadcast: 'auto')~> joinZactualtime
joinZactualtime select(mapColumn(
		suporder_id = selectSupTrack@suporder_id,
		sup_id,
		inv_id,
		order_time_id,
		expected_time_id,
		actual_time_id,
		import_quantity,
		import_cost,
		import_total
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select1
selectOrderDate aggregate(groupBy(suporder_id,
		order_time_id),
	count = count()) ~> aggregateOrderDate
selectExpectedTime aggregate(groupBy(expected_time_id,
		suporder_id),
	count = count()) ~> aggregateExpectedTime
selectActualTime aggregate(groupBy(actual_time_id,
		suporder_id),
	count = count()) ~> aggregateActualTime
select1 sink(allowSchemaDrift: true,
	validateSchema: false,
	input(
		sup_id as integer,
		suporder_id as integer,
		inv_id as integer,
		order_time_id as integer,
		expected_time_id as integer,
		actual_time_id as integer,
		import_cost as decimal(18,0),
		import_quantity as decimal(18,0),
		import_total as decimal(18,0)
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