fn uppercase_values(fields, index) {
    let result = #{};

    print("preprocessing...");

    for key in fields.keys() {
        print("preprocessing...");

        result[key] = fields[key].to_upper();

        info("Preprocessed field: " + key + " " + fields[key]);
    }

    return result;
}