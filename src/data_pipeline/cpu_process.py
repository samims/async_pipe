"""
CPU-intensive processing using multiprocessing.

Demonstrates efficient parallel processing of CPU-bound tasks using
the `concurrent.futures` ProcessPoolExecutor for leveraging multiple CPU cores.

`concurrent.futures` provides a clean, high-level API for parallel execution
using threads or processes.
"""


import logging

from concurrent import futures
from multiprocessing import cpu_count

from typing import Dict, List, Callable, Any

logger = logging.getLogger(__name__)


def process_data_chunk(data_chunk: List[Any], process_func: Callable):
    """
    Process a chunk of data with the given processing function.

    Args:
        data_chunk (List[Any]): The chunk of data to process.
        process_func (Callable): The function to apply to each item in the chunk.

    Returns:
        List[Any]: The processed data.
    """

    return [process_func(item) for item in data_chunk]


class CPUIntensiveProcessor(object):
    """
    Process CPU-intensive tasks using multiprocessing.
    """

    def __init__(self, max_workers: int = None):
        """
        Initialize the CPUIntensiveProcessor with the given number of maximum workers.
        """
        self.max_workers = max_workers or cpu_count()
        logger.debug(f"CPU processor initialized with {self.max_workers} workers")


    def parallel_process(
        self, data: List[Any], process_func: Callable, chunk_size: int = 100
    ) -> List[Any]:
        """
        Process data in parallel using multiprocessing.

        Args:
            data (List[Any]): The data to process.
            process_func (Callable): The function to apply to each item in the data.
            chunk_size (int): The size of each chunk to process in parallel.

        Returns:
            List[Any]: The processed data.
        """
        if not data:
            return []

        # Break the full data list into smaller chunks of length `chunk_size`
        # For example, if data has 1000 items and chunk_size is 100,
        # this will produce 10 chunks: data[0:100], data[100:200], ..., data[900:1000]
        chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]
        logger.info(f"Processing by {self.max_workers} workers {len(data)} items in {len(chunks)} chunks...")

        results = []
        # NOTE:
        # We create the executor inside this method (instead of in __init__)
        # so it only exists for the duration of the call.
        # This way, it automatically shuts down after use — no need to manage or reuse it.
        # Keeps things simple and avoids resource leaks for short-lived processing.
        with futures.ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # we are storing chunk too, to process the exception cases,
            # ex: we can rerun as we are storing the argument chunk
            # future_to_chunk: dict[callable_future, data_list]
            future_to_chunk = {
                executor.submit(process_data_chunk, chunk, process_func): chunk
                for chunk in chunks
            }

            for future in futures.as_completed(future_to_chunk):
                try:
                    chunk_result = future.result()
                    results.extend(chunk_result)
                except Exception as e:
                    logger.error(f"Error processing chunk: {e}")
                    # NOTE: Fallback: process chunk in current process aka main process
                    chunk = future_to_chunk[future]

                    # we can do try catch here too, but leaving for simplicity
                    # our focus is concurrent library
                    results.extend(process_data_chunk(chunk, process_func))

        return results


def is_prime(n: int) -> Dict[str, Any]:
    """
    Naive CPU-intensive check to determine if a number is prime.
    Returns a dictionary with the number and the result.
    NOTE: inefficient on purpose
    """
    if n < 2:
        return {"number": n, "is_prime": False}

    for i in range(2, int(n**0.5) + 1):
        if n % i == 0:
            return {"number": n, "is_prime": False}
    return {"number": n, "is_prime": True}


# test the function
def cpu_intensive_prime_check(processor:CPUIntensiveProcessor) -> Dict[str, Any]:
    # Simulate a complex calculation


    # Wrap numbers in dicts to match process_data_chunk interface
    # data = [{"value": n} for n in numbers]
    data = list(range(10**10, 10**10 + 5_0000))



    results = processor.parallel_process(data, is_prime, chunk_size=25)

    return {"original": data, "processed": results}


if __name__ == "__main__":
    from time import time
    # Benchmark the CPU-intensive prime checking function with varying numbers of worker processes.
    # This demonstrates how multiprocessing scales with more CPU cores.
    for workers in [1, 2, 4, cpu_count()]:
        print(f"\nRunning with {workers} worker(s)")
        start = time()

        # Initialize the processor with the desired number of parallel workers
        processors = CPUIntensiveProcessor(max_workers=workers)
        result = cpu_intensive_prime_check(processors)

        print(f"Processed {len(result['processed'])} numbers in {time() - start:.2f} seconds.")
