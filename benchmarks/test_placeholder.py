def placeholder_function():
    return sum(range(10**6))


def setup_placeholder():
    # Any setup logic goes here
    # For example, creating necessary objects or loading data
    print("Setup phase completed")


def test_placeholder_function(benchmark):
    # Use `benchmark.pedantic` for 2 rounds
    benchmark.pedantic(
        placeholder_function,
        setup=setup_placeholder,  # Specify the setup function
        rounds=2,  # Number of rounds
        iterations=1,  # Number of iterations per round
        warmup_rounds=0,  # Number of warmup rounds
    )
