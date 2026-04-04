def calculate_metrics(data):
    # Validate data
    if not data:
        return []

    # Find strongest & weakest currency
    strongest = max(data, key=lambda x: x["rate"])
    weakest = min(data, key=lambda x: x["rate"])

    return [
        {
            "date": data[0]["date"],
            "strongest_currency": strongest["currency"],
            "strongest_rate": strongest["rate"],
            "weakest_currency": weakest["currency"],
            "weakest_rate": weakest["rate"],
        }
    ]