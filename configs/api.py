class S3Configs:
    bucket_name = "daily-stock-prices-750477223923"

class APIConfigs:
    base_url = "https://alpha-vantage.p.rapidapi.com/query/"
    window = 1
    symbols = ["MSFT", "AAPL", "NVDA", "GOOGL", "TSLA"]


class RedshiftConfigs:
    configs = {
        "cluster": "redshift-serverless",
        "workgroup_name": "stock-data-analysis",
        "db_name": "dev",
        "host": "football-results-etl.750477223923.eu-north-1.redshift-serverless.amazonaws.com",
        "port": 5439,
        "region": "eu-north-1",
    }
    column_data = {
        "symbol": "VARCHAR",
        "date": "VARCHAR",
        "opening": "REAL",
        "high": "REAL",
        "low": "REAL",
        "closing": "REAL",
        "volume": "REAL",
        "ai_sentiment": "SUPER",
        "metadata": "SUPER",
        "processing_timestamp": "VARCHAR"
    }
