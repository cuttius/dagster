from dagster import AssetSelection, asset, asset_check, define_asset_job


@asset
def my_asset():
    ...


@asset_check(asset=my_asset)
def check_1():
    ...


@asset_check(asset=my_asset)
def check_2():
    ...


# includes my_asset and both checks
my_job = define_asset_job("my_job", selection=AssetSelection.assets(my_asset))


# includes only my_asset
my_asset_only_job = define_asset_job(
    "my_asset_only_job",
    selection=AssetSelection.assets(my_asset) - AssetSelection.all_asset_checks(),
)

# includes check_1 and check_2, but not my_asset
checks_only_job = define_asset_job(
    "checks_only_job", selection=AssetSelection.asset_checks_for_assets(my_asset)
)

# includes only check_1
check_1_job = define_asset_job(
    "check_1_job", selection=AssetSelection.asset_checks(check_1)
)
