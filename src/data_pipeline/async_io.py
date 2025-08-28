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
    ):
        """
        Initialize the AsyncDataFetcher with a maximum number of retries.

        Args:
            max_retries (int): Number of times to retry a failed request.
            concurrency_limit (int): Maximum number of concurrent requests.
            timeout_sec (float): Timeout for each request in seconds.
        """
        # NOTE: semaphore to limit concurrent requests, it can be used to implement a retry mechanism
        # basically it's a counter to limit the number of concurrent requests
        self.max_retries = max_retries
        self.semaphore = asyncio.Semaphore(concurrency_limit)
        self.timeout_sec = timeout_sec  # Store timeout as instance variable

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
        # using semaphore attribute to limit concurrent requests
        # NOTE: we don't have to semaphore lock() and release() manually cause
        # the `async with statement takes care of that
        async with self.semaphore:
            for attempt in range(1, self.max_retries + 1):
                try:
                    logger.debug(f"Attempt {attempt} Fetching URL: {url}")

                    # timeout used to prevent hanging requests, it allows how much time the request can take
                    # before it is considered a failure
                    async with session.get(
                        url, timeout=aiohttp.ClientTimeout(total=self.timeout_sec)
                    ) as response:
                        response.raise_for_status()  # raises error for http error
                        data = await response.json()
                        return {
                            "success": True,
                            "url": url,
                            "data": data,
                        }

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
                # fallback exception, don't judge :-D
                except Exception as e:
                    logger.error(f"Attempt {attempt} Error while fetching: {url} - {e}")
                    if attempt == self.max_retries:
                        return {"success": False, "url": url, "error": str(e)}

                backoff_time = min(
                    2 ** (attempt - 1), 10
                )  # Exponential backoff, max 10 seconds

                # simple backoff strategy
                await asyncio.sleep(backoff_time)

    async def fetch_all(self, urls: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch data from multiple urls concurrently

        Args:
            urls (List[str]): List of URLs to fetch data from.

        Returns:
            List[Dict[str, Any]]: List of parsed JSON responses or error messages.
        """
        async with aiohttp.ClientSession() as session:
            # gather all fetch tasks
            tasks = [self.fetch_single(session, url) for url in urls]

            # NOTE: as the doc says - 'Return a future aggregating results from the given coroutines/futures.'
            # NOTE: return_exceptions=True -> this won't break the tasks immediately in case of err
            # doc: https://docs.python.org/3/library/asyncio-task.html#asyncio.gather
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return results


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

    async_data_fetcher = AsyncDataFetcher(max_retries=4)
    results = await async_data_fetcher.fetch_all(urls)

    successful_results = [res for res in results if "error" not in res]
    print("=========================")
    print("=========================")
    # process successful results
    print(f"Fetched {len(successful_results)}/{len(urls)} successful results")
    return results


if __name__ == "__main__":
    # for direct run of this script
    results = asyncio.run(test_async_fetcher())

    for res in results:
        if res["success"]:
            print(f"Successfully fetched data from {res['url']}")
        else:
            print(f"Failed to fetch data from {res['url']} - {res['error']}")
