trace("Luau preamble executed");

type DataSourceRecordFields = {[string]: string};

type DataSourceRecordIndex = {
	record_number: number,
	line_start: number,
	line_end: number,
};

type Transform = (fields: DataSourceRecordFields, index: DataSourceRecordIndex) -> DataSourceRecordFields?;

