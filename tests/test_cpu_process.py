from typing import Dict, Any
from unittest.mock import MagicMock

import pytest

from src.data_pipeline.cpu_process import (
    CPUIntensiveProcessor,
    process_data_chunk,
    is_prime,
)


@pytest.fixture
def mock_process_func():
    # This mock function simply returns a dict with the number and a is_prime flag
    return MagicMock(side_effect=lambda x: {"number": x, "is_prime": True})


# --- Tests for checking if a number is prime ---


@pytest.mark.parametrize(
    "number, expected_result",
    [
        (2, {"number": 2, "is_prime": True}),
        (3, {"number": 3, "is_prime": True}),
        (4, {"number": 4, "is_prime": False}),
        (17, {"number": 17, "is_prime": True}),
        (1, {"number": 1, "is_prime": False}),
        (0, {"number": 0, "is_prime": False}),
        (-5, {"number": -5, "is_prime": False}),
        (7919, {"number": 7919, "is_prime": True}),  # large prime number
        (7920, {"number": 7920, "is_prime": False}),  # large composite number
    ],
)
def test_is_prime_identifies_primes_and_composites(
    number: int, expected_result: Dict[str, Any]
):
    """
    Test that is_prime returns correct results for various numbers, including
    edge cases and large numbers.
    """
    assert is_prime(number) == expected_result


# --- Tests for processing a chunk of data ---


def test_process_data_chunk_returns_expected_results(mock_process_func):
    """
    Given a list of numbers, process_data_chunk returns a list of dicts
    with 'number' and 'is_prime' keys correctly populated.
    """
    data = [2, 4, 5]
    expected = [
        {"number": 2, "is_prime": True},
        {"number": 4, "is_prime": True},
        {"number": 5, "is_prime": True},
    ]

    assert process_data_chunk(data, mock_process_func) == expected


def test_process_data_chunk_handles_empty_input_gracefully(mock_process_func):
    """
    Passing an empty list returns an empty list (no errors).
    """
    data = []
    expected = []

    assert process_data_chunk(data, mock_process_func) == expected


# --- Tests for CPUIntensiveProcessor class functionality ---


def test_cpu_intensive_processor_processes_numbers_correctly(mock_process_func):
    """
    CPUIntensiveProcessor processes a small list of numbers correctly,
    identifying primes.
    """
    data = [2, 4, 5]
    processor = CPUIntensiveProcessor(max_workers=2)

    # ** Pass process_data_chunk and is_prime for processing **
    result = processor.parallel_process(data, mock_process_func, chunk_size=2)

    expected = [
        {"number": 2, "is_prime": True},
        {"number": 4, "is_prime": True},
        {"number": 5, "is_prime": True},
    ]
    # Sort results to avoid order-related test failures
    assert sorted(result, key=lambda x: x["number"]) == expected


@pytest.mark.parametrize("worker_count", [1, 2, 4])
def test_cpu_intensive_processor_with_various_worker_counts(
    mock_process_func, worker_count
):
    """
    CPUIntensiveProcessor should return correct results regardless of number
    of workers.
    """
    data = [2, 3, 4, 5, 6, 7]
    processor = CPUIntensiveProcessor(max_workers=worker_count)
    result = processor.parallel_process(data, mock_process_func, chunk_size=2)

    assert len(result) == len(data)
    result_numbers = {item["number"] for item in result}
    assert result_numbers == set(data)


def test_cpu_intensive_processor_handles_empty_list(mock_process_func):
    """
    When given an empty list, CPUIntensiveProcessor returns an empty list.
    """
    processor = CPUIntensiveProcessor(max_workers=2)
    result = processor.parallel_process([], mock_process_func)
    assert result == []


def test_cpu_intensive_processor_can_handle_large_input(mock_process_func):
    """
    CPUIntensiveProcessor can process a large list of numbers without error
    and returns the expected number of results.
    """
    data = list(range(1000))  # Numbers from 0 to 999
    processor = CPUIntensiveProcessor(max_workers=4)
    result = processor.parallel_process(data, mock_process_func, chunk_size=10)

    assert len(result) == len(data)
    assert all("number" in item and "is_prime" in item for item in result)


def test_cpu_intensive_processor_with_real_is_prime():
    """
    *** INTEGRATION TEST ***
    """
    data = [2, 4, 5]
    processor = CPUIntensiveProcessor(max_workers=2)

    result = processor.parallel_process(data, is_prime, chunk_size=2)

    expected = [
        {"number": 2, "is_prime": True},
        {"number": 4, "is_prime": False},
        {"number": 5, "is_prime": True},
    ]
    assert sorted(result, key=lambda x: x["number"]) == expected


def failing_func_for_test(x):
    """Helper function for testing exception handling in multiprocessing."""
    if x == 3:
        raise ValueError("Test exception")
    return {"number": x, "processed": True}


def test_cpu_intensive_processor_handles_exceptions_in_workers():
    """
    Test that exceptions in worker processes are handled gracefully.
    """
    data = [1, 2, 3, 4]
    processor = CPUIntensiveProcessor(max_workers=2)

    # Should not raise exception, but process other items
    result = processor.parallel_process(data, failing_func_for_test, chunk_size=2)

    # Should have results for successful items (1 and 2), failing chunk (3,4) is skipped
    assert len(result) == 2
    result_numbers = {item["number"] for item in result}
    assert result_numbers == {1, 2}
    assert all("processed" in item for item in result)
