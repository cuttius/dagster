import pandas as pd

from dagster import AssetCheckResult, AssetCheckSpec, Output, asset


@asset(check_specs=[AssetCheckSpec("orders_id_has_no_nulls")])
def orders(context):
    orders_df = pd.DataFrame({"order_id": [1, 2], "item_id": [432, 878]})

    # save the output and indicate that it's been saved
    orders_df.to_csv("orders")
    yield Output(value=None)

    # check it
    num_null_order_ids = orders_df["order_id"].isna().sum()
    yield AssetCheckResult(
        success=(num_null_order_ids == 0),
        metadata={"num_null_order_ids": num_null_order_ids},
    )
