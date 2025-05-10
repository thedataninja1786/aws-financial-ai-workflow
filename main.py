from configs.api import RedshiftConfigs
import aiohttp
import asyncio
from aws import S3
from daily_prices_extractor import PriceExtractor
from chatbot import generate_sentiment
from aws import RedShift

from dotenv import load_dotenv

load_dotenv()

symbols = ["MSFT", "AAPL", "NVDA", "GOOGL", "TSLA"]
redshift_configs = RedshiftConfigs.configs
bucket_name = "daily-stock-prices-750477223923"
column_data = RedshiftConfigs.column_data
column_names = list(column_data.keys())
table_name = "daily_prices"


async def main():
    timeout = aiohttp.ClientTimeout(total=10)
    s3 = S3(bucket_name)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [
            PriceExtractor(symbol).process_data(session, s3, generate_sentiment)
            for symbol in symbols
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # flatten the data from each symbol
    results = [record for symbol in results for record in symbol]

    # Create a Redshift instance
    redshift = RedShift(
        cluster=redshift_configs["cluster"],
        workgroup_name=redshift_configs["workgroup_name"],
        region=redshift_configs["region"],
        db_name=redshift_configs["db_name"],
        host=redshift_configs["host"],
        port=redshift_configs["port"],
    )

    # generate temporary credentials
    redshift.generate_tmp_credentials()

    # create table if not exsits
    redshift.create_table(table_name=table_name, fields=column_data)

    # upsert data to redshift
    redshift.write_data(
        table_name=table_name,
        data_rows=results,
        column_names=column_names,
        write_method="upsert",
        upsert_on=["date","symbol"],
    )


if __name__ == "__main__":
    asyncio.run(main())
