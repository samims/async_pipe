"""
Async I/O operations for data processing
Handles asynchronous HTTP requests and responses with aiohttp with proper error handling and retries.

Used :NOTE to `self note` and `concept` related to async programming and aiohttp.
"""

import aiohttp
import asyncio
import logging

from typing import Dict, Any, List

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class AsyncDataFetcher(object):
    """
    AsyncDataFetcher is responsible for fetching data from source APIs asynchronously
    using aiohttp.
    """

    def __init__(
        self,
        max_retries: int = 6,
        concurrency_limit: int = 5,
        timeout_sec: float = 15.5,
        verify_ssl: bool = True,
    ):
        """
        Initialize the AsyncDataFetcher with a maximum number of retries.

        Args:
            max_retries (int): Number of times to retry a failed request.
            concurrency_limit (int): Maximum number of concurrent requests.
            timeout_sec (float): Timeout for each request in seconds.
            verify_ssl (bool): Whether to verify SSL certificates.
        """
        # NOTE: semaphore to limit concurrent requests, it can be used to implement a retry mechanism
        # basically it's a counter to limit the number of concurrent requests
        self.max_retries = max_retries
        self.semaphore = asyncio.Semaphore(concurrency_limit)
        self.timeout_sec = timeout_sec  # Store timeout as instance variable
        self.verify_ssl = verify_ssl

    async def _make_request(
        self, session: aiohttp.ClientSession, url: str
    ) -> Dict[str, Any]:
        """Make a single HTTP request with timeout."""
        async with session.get(
            url, timeout=aiohttp.ClientTimeout(total=self.timeout_sec)
        ) as response:
            response.raise_for_status()
            data = await response.json()
            return {
                "success": True,
                "url": url,
                "data": data,
            }

    async def fetch_single(
        self, session: aiohttp.ClientSession, url: str
    ) -> Dict[str, Any]:
        """
        Fetch data from a single URL with retries.

        Args:
            session (aiohttp.ClientSession): The aiohttp session to use for the request.
            url (str): The URL to fetch data from.

        Returns:
            Dict[str, Any]: Parsed JSON response or appropriate data.
        """
        async with self.semaphore:
            for attempt in range(1, self.max_retries + 1):
                try:
                    logger.debug(f"Attempt {attempt} Fetching URL: {url}")
                    return await self._make_request(session, url)

                except asyncio.TimeoutError:
                    logger.warning(f"Attempt {attempt} Timeout while fetching: {url}")
                    if attempt == self.max_retries:
                        return {"success": False, "url": url, "error": "timeout"}
                except aiohttp.ClientError as e:
                    logger.error(
                        f"Attempt {attempt} ClientError while fetching: {url} - {e}"
                    )
                    if attempt == self.max_retries:
                        return {"success": False, "url": url, "error": str(e)}
                except Exception as e:
                    logger.error(
                        f"Attempt {attempt} Unexpected error while fetching: {url} - {e}"
                    )
                    if attempt == self.max_retries:
                        return {"success": False, "url": url, "error": str(e)}

                backoff_time = min(2 ** (attempt - 1), 10)
                await asyncio.sleep(backoff_time)
            return {}

    async def fetch_all(self, urls: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch data from multiple urls concurrently

        Args:
            urls (List[str]): List of URLs to fetch data from.

        Returns:
            List[Dict[str, Any]]: List of parsed JSON responses or error messages.
        """
        connector = aiohttp.TCPConnector(ssl=False if not self.verify_ssl else None)
        async with aiohttp.ClientSession(connector=connector) as session:
            # gather all fetch tasks
            tasks = [self.fetch_single(session, url) for url in urls]

            # NOTE: as the doc says - 'Return a future aggregating results from the given coroutines/futures.'
            # NOTE: return_exceptions=True -> this won't break the tasks immediately in case of err
            # doc: https://docs.python.org/3/library/asyncio-task.html#asyncio.gather
            result_resp = await asyncio.gather(*tasks, return_exceptions=True)
            return result_resp


# test function simply test the AsyncDataFetcher
async def test_async_fetcher():
    # using JSONHolder for testing, you can choose your own
    urls = [
        "https://jsonplaceholder.typicode.com/posts/1",
        "https://jsonplaceholder.typicode.com/posts/2",
        "https://jsonplaceholder.typicode.com/posts/3",
        "https://jsonplaceholder.typicode.com/posts/4",
        "https://jsonplaceholder.typicode.com/posts/5",
        # "http://localhost:9999",  # This will fail, assuming nothing is running on this port
    ]

    async_data_fetcher = AsyncDataFetcher(max_retries=4, verify_ssl=False)
    result_list = await async_data_fetcher.fetch_all(urls)

    successful_results = [r for r in result_list if "error" not in r]
    print("=========================")
    print("=========================")
    # process successful results
    print(f"Fetched {len(successful_results)}/{len(urls)} successful results")
    return result_list


if __name__ == "__main__":
    # for direct run of this script
    results = asyncio.run(test_async_fetcher())

    for res in results:
        if res["success"]:
            print(f"Successfully fetched data from {res['url']}")
        else:
            print(f"Failed to fetch data from {res['url']} - {res['error']}")
