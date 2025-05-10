from configs.api import APIConfigs, RedshiftConfigs
import os
import aiohttp
import asyncio
import json
from typing import Tuple, List, Callable
from aws import S3
from datetime import datetime


class PriceExtractor:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.api_key = os.environ.get("API_KEY").strip()
        self.host = os.environ.get("HOST").strip()
        self.base_url = APIConfigs.base_url
        self.window = APIConfigs.window

    def get_headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": self.host,
        }

    def get_querystring(self) -> dict:
        return {
            "function": "TIME_SERIES_DAILY",
            "symbol": self.symbol,
            "outputsize": "compact",
            "datatype": "json",
        }

    async def _get_request_async(
        self,
        session: aiohttp.client.ClientSession,
        url: str,
        headers: dict,
        params: dict,
        timeout: int,
    ) -> dict:
        async with session.get(
            url, headers=headers, params=params, timeout=timeout
        ) as response:
            response.raise_for_status()
            return await response.json()

    async def process_data(
        self,
        session: aiohttp.client.ClientSession,
        s3: S3,
        generate_sentiment: Callable,
    ) -> List[Tuple[str, str, float, float, float, float, float, str, str]]:
        try:
            await asyncio.sleep(1)
            headers = self.get_headers()
            params = self.get_querystring()
            response_data = await self._get_request_async(
                session=session,
                url=self.base_url,
                headers=headers,
                params=params,
                timeout=10,
            )
            
            # dump raw JSON to S3
            file_name = f"price_data/{self.symbol}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
            s3.upload_string_to_s3(
                json.dumps(response_data, indent=2), object_name=file_name
            )
            

        except Exception as e:
            print(self.process_data.__name__)
            print(f"Request Error: {e}")
            return {}

        metadata = response_data.get("Meta Data", {})
        time_series = response_data.get("Time Series (Daily)")
        if not time_series:
            print("No time series data found.")
            return []

        # get the most recent n dates since API returned dates >> n
        dates = sorted(time_series.keys(), reverse=True)[: self.window]

        res = []
        for date in dates:
            date_data = time_series[date]
            prices = [float(price) for price in date_data.values()]
            sentiment = generate_sentiment(symbol = self.symbol, date = date)

            # convert to a tuple
            record = (
                self.symbol,
                date,
                *prices,
                json.dumps({"symbol_sentiment":sentiment}),
                json.dumps(metadata),
                datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
            )
            res.append(record)
        return res


