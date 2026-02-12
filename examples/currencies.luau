function uppercase_values(fields: DataSourceRecordFields, index: DataSourceRecordIndex): DataSourceRecordFields?
	for key, value in pairs(fields) do
		fields[key] = string.upper(value)

		info("Preprocessed field:", key, fields[key])
	end

	return fields
end