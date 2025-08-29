import asyncio

import pytest

from aiohttp import ClientError
from aioresponses import aioresponses

from src.data_pipeline.async_io import AsyncDataFetcher

pytestmark = pytest.mark.asyncio


@pytest.fixture
def fetcher():
    return AsyncDataFetcher(max_retries=4, concurrency_limit=2, timeout_sec=2.5)


async def test_fetch_single_success(fetcher):
    url = "https://example.com/api/data"

    with aioresponses() as mock:
        mock.get(url, payload={"data": "Hello, World!"})

        response_list = await fetcher.fetch_all([url])

        assert len(response_list) == 1
        assert response_list[0]["data"] == {"data": "Hello, World!"}
        assert response_list[0]["success"] is True


async def test_failed_fetch_due_to_client_error(fetcher):
    url = "https://example.com/api/fail"
    fetcher.max_retries = 1
    fetcher.timeout_sec = 0.2
    with aioresponses() as mock:
        mock.get(url, exception=ClientError("Client error"))

        result = await fetcher.fetch_all([url])

        assert len(result) == 1
        assert result[0]["success"] is False
        assert "error" in result[0]


async def test_fetch_retry_on_timeout(fetcher):
    url = "https://example.com/api/timeout"

    fetcher.max_retries = 2  # Set retries to 2 for this test
    fetcher.timeout_sec = 0.01

    with aioresponses() as mock:
        # Simulate timeout for the first two attempts, then return a successful response
        mock.get(url, exception=asyncio.TimeoutError(), repeat=True)
        mock.get(url, exception=asyncio.TimeoutError(), repeat=True)
        mock.get(url, payload={"data": "Hello, World!"})

        response_list = await fetcher.fetch_all([url])

        assert len(response_list) == 1
        assert response_list[0]["success"] is False


async def test_fetch_multiple_urls(fetcher):
    urls = [
        "https://example.com/api/data/1",
        "https://example.com/api/data/2",
        "https://example.com/api/data/3",
    ]

    with aioresponses() as mock:
        for url in urls:
            mock.get(url, payload={"data": f"Response for {url}"})

        response_list = await fetcher.fetch_all(urls)

        assert len(response_list) == len(urls)
        for i, url in enumerate(urls):
            assert response_list[i]["data"] == {"data": f"Response for {url}"}
            assert response_list[i]["success"] is True
