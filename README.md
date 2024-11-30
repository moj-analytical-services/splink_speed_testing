# splink_speed_testing


# Splink Speed Testing

A benchmarking suite for testing the performance of [Splink](https://github.com/moj-analytical-services/splink)'s comparison functions across different database backends.

## Overview

This repository contains benchmarking tests to measure and compare the execution speed of Splink's comparison functions (like exact matching, Jaro similarity, and Jaro-Winkler similarity) across different database backends including DuckDB and Apache Spark.

## Setup

1. Install dependencies using Poetry:

```bash
poetry install
```

## Running Tests

Execute the benchmarks using pytest-benchmark:
```bash
poetry run pytest benchmarks/
```

## Features

- Benchmarks common string comparison functions used in record linkage
- Tests against multiple database backends (DuckDB, Spark)
- Generates test datasets of configurable sizes
- Uses pytest-benchmark for reliable performance measurements

## Requirements

- Python 3.11 (Spark 3.5 doesn't official support Python 3.12)

