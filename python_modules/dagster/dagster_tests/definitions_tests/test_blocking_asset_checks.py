from dagster import (
    AssetCheckResult,
    AssetKey,
    Definitions,
    ExecuteInProcessResult,
    asset,
    asset_check,
)
from dagster._core.definitions.asset_checks import build_blocking_asset_check
from dagster._core.definitions.asset_in import AssetIn


def execute_assets_and_checks(
    assets=None,
    asset_checks=None,
    raise_on_error: bool = True,
    resources=None,
    instance=None,
    tags=None,
) -> ExecuteInProcessResult:
    defs = Definitions(assets=assets, asset_checks=asset_checks, resources=resources)
    job_def = defs.get_implicit_global_asset_job_def()
    return job_def.execute_in_process(raise_on_error=raise_on_error, instance=instance, tags=tags)

@asset
def upstream_asset():
    pass

@asset(deps=[upstream_asset])
def my_asset():
    pass


@asset_check(asset="my_asset")
def pass_check():
    return AssetCheckResult(success=True, check_name="pass_check")


@asset_check(asset="my_asset")
def fail_check_if_tagged(context):
    return AssetCheckResult(
        success=not context.has_tag("fail_check"), check_name="fail_check_if_tagged"
    )


blocking_asset = build_blocking_asset_check(
    asset_def=my_asset, checks=[pass_check, fail_check_if_tagged]
)


@asset(deps=[blocking_asset])
def downstream_asset():
    pass

def test_check_pass():
    result = execute_assets_and_checks(
        assets=[upstream_asset, blocking_asset, downstream_asset], raise_on_error=False
    )
    assert result.success

    check_evals = result.get_asset_check_evaluations()
    assert len(check_evals) == 2
    check_evals_by_name = {check_eval.check_name: check_eval for check_eval in check_evals}
    assert check_evals_by_name["pass_check"].success
    assert check_evals_by_name["pass_check"].asset_key == AssetKey(["my_asset"])
    assert check_evals_by_name["fail_check_if_tagged"].success
    assert check_evals_by_name["fail_check_if_tagged"].asset_key == AssetKey(["my_asset"])

    # downstream asset materializes
    materialization_events = result.get_asset_materialization_events()
    assert len(materialization_events) == 3
    assert materialization_events[0].asset_key == AssetKey(["upstream_asset"])
    assert materialization_events[1].asset_key == AssetKey(["my_asset"])
    assert materialization_events[2].asset_key == AssetKey(["downstream_asset"])


def test_check_fail_and_block():
    result = execute_assets_and_checks(
        assets=[upstream_asset, blocking_asset, downstream_asset], raise_on_error=False, tags={"fail_check": "true"}
    )
    assert not result.success

    check_evals = result.get_asset_check_evaluations()
    assert len(check_evals) == 2
    check_evals_by_name = {check_eval.check_name: check_eval for check_eval in check_evals}
    assert check_evals_by_name["pass_check"].success
    assert check_evals_by_name["pass_check"].asset_key == AssetKey(["my_asset"])
    assert not check_evals_by_name["fail_check_if_tagged"].success
    assert check_evals_by_name["fail_check_if_tagged"].asset_key == AssetKey(["my_asset"])

    # downstream asset should not have been materialized
    materialization_events = result.get_asset_materialization_events()
    assert len(materialization_events) == 2
    assert materialization_events[0].asset_key == AssetKey(["upstream_asset"])
    assert materialization_events[1].asset_key == AssetKey(["my_asset"])



@asset
def my_asset_with_managed_input(upstream_asset):
    pass



@asset_check(asset="my_asset_with_managed_input")
def fail_check_if_tagged_2(context, my_asset_with_managed_input):
    return AssetCheckResult(
        success=not context.has_tag("fail_check"), check_name="fail_check_if_tagged_2"
    )


blocking_asset_with_managed_input = build_blocking_asset_check(
    asset_def=my_asset_with_managed_input, checks=[fail_check_if_tagged_2]
)

@asset(ins={"input_asset": AssetIn(blocking_asset_with_managed_input.key)})
def downstream_asset_2(input_asset):
    pass

def test_check_pass_with_inputs():
    result = execute_assets_and_checks(
        assets=[upstream_asset, blocking_asset_with_managed_input, downstream_asset_2], raise_on_error=False
    )
    assert result.success

    check_evals = result.get_asset_check_evaluations()
    assert len(check_evals) == 1
    check_evals_by_name = {check_eval.check_name: check_eval for check_eval in check_evals}
    assert check_evals_by_name["fail_check_if_tagged_2"].success
    assert check_evals_by_name["fail_check_if_tagged_2"].asset_key == AssetKey(["my_asset_with_managed_input"])

    # downstream asset materializes
    materialization_events = result.get_asset_materialization_events()
    assert len(materialization_events) == 3
    assert materialization_events[0].asset_key == AssetKey(["upstream_asset"])
    assert materialization_events[1].asset_key == AssetKey(["my_asset_with_managed_input"])
    assert materialization_events[2].asset_key == AssetKey(["downstream_asset_2"])


def test_check_fail_and_block_with_inputs():
    result = execute_assets_and_checks(
        assets=[upstream_asset, blocking_asset_with_managed_input, downstream_asset_2], raise_on_error=False, tags={"fail_check": "true"}
    )
    assert not result.success

    check_evals = result.get_asset_check_evaluations()
    assert len(check_evals) == 1
    check_evals_by_name = {check_eval.check_name: check_eval for check_eval in check_evals}
    assert not check_evals_by_name["fail_check_if_tagged_2"].success
    assert check_evals_by_name["fail_check_if_tagged_2"].asset_key == AssetKey(["my_asset_with_managed_input"])

    # downstream asset should not have been materialized
    materialization_events = result.get_asset_materialization_events()
    assert len(materialization_events) == 2
    assert materialization_events[0].asset_key == AssetKey(["upstream_asset"])
    assert materialization_events[1].asset_key == AssetKey(["my_asset_with_managed_input"])


