import requests

def fetch_transactions():
    url = "https://api.exchangerate-api.com/v4/latest/USD"
    response = requests.get(url)
    data = response.json()

    rates = data["rates"]

    # Convert to financial-style records
    transactions = []

    for currency, rate in list(rates.items())[:10]:
        transactions.append({
            "date": data["date"],
            "currency": currency,
            "rate": float(rate)
        })

    return transactions