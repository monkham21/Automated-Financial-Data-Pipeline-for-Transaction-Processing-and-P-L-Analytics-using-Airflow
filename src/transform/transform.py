def transform_data(data):
    for row in data:
        if row["type"] == "revenue":
            row["amount"] = row["amount"] * 1.0
    return data