# import splink.internals.comparison_level_library as cll

# print(
#     cll.PairwiseStringDistanceFunctionLevel(
#         "postcode_array", distance_function_name="jaro_winkler", distance_threshold=0.8
#     )
#     .get_comparison_level("duckdb")
#     .sql_condition
# )


duckdb_pairwise_array_string_similarity = """
list_max(
                    list_transform(
                        flatten(
                            list_transform(
                                "{col_name}_l",
                                x -> list_transform(
                                    "{col_name}_r",
                                    y -> [x,y]
                                )
                            )
                        ),
                        pair -> {similarity_function}(pair[1], pair[2])
                    )
                ) >= {threshold}
"""


spark_pairwise_array_string_similarity = """
array_max(
    transform(
        DualArrayExplode({col_name}_l, {col_name}_r),
        x -> {similarity_function}(x._1, x._2)
    )
) >= {threshold}
"""


def calculate_tf_product_array_sql(token_rel_freq_array_name):
    return f"""
    list_reduce(
        list_prepend(
            1.0,
            list_transform(
                {token_rel_freq_array_name}_l,
                x -> CASE
                        WHEN array_contains(
                            list_transform({token_rel_freq_array_name}_r, y -> y.value),
                            x.value
                        )
                        THEN x.rel_freq
                        ELSE 1.0
                    END
            )
        ),
        (p, q) -> p * q
    )
    """


duckdb_tf_product_array = calculate_tf_product_array_sql("token_freq_array")
