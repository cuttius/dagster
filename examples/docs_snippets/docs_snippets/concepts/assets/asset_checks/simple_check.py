from dagster import AssetCheckResult, Definitions, asset, asset_check


@asset
def my_asset():
    ...


@asset_check(asset=my_asset)
def my_check():
    return AssetCheckResult(success=True)


defs = Definitions(assets=[my_asset], asset_checks=[my_check])
