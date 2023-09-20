import pytest
from dagster import (
    AssetKey,
    AssetMaterialization,
    DagsterInvariantViolationError,
    Output,
    RunRequest,
    SkipReason,
    asset_sensor,
    build_sensor_context,
    job,
    op,
    sensor,
)
from dagster._annotations import get_experimental_params
from dagster._check import CheckError
from dagster._core.definitions.run_request import SensorResult
from dagster._core.test_utils import instance_for_test


@op
def do_something():
    pass


@job
def do_something_job():
    do_something()


def test_sensor_result_one_run_request():
    @sensor(job=do_something_job)
    def test_sensor(_):
        return SensorResult(run_requests=[RunRequest(run_key="foo")])

    with instance_for_test() as instance:
        ctx = build_sensor_context(
            instance=instance,
        )
        sensor_data = test_sensor.evaluate_tick(ctx)
        assert len(sensor_data.run_requests) == 1
        assert sensor_data.run_requests[0].run_key == "foo"
        assert not sensor_data.skip_message
        assert not sensor_data.dagster_run_reactions
        assert not sensor_data.cursor


def test_sensor_result_skip_reason():
    skip_reason = SkipReason("I'm skipping")

    @sensor(job=do_something_job)
    def test_sensor(_):
        return [
            SensorResult(skip_reason=skip_reason),
        ]

    with instance_for_test() as instance:
        ctx = build_sensor_context(
            instance=instance,
        )
        sensor_data = test_sensor.evaluate_tick(ctx)
        assert not sensor_data.run_requests
        assert sensor_data.skip_message == skip_reason.skip_message
        assert not sensor_data.dagster_run_reactions
        assert not sensor_data.cursor


def test_sensor_result_string_skip_reason():
    skip_reason = "I'm skipping"

    @sensor(job=do_something_job)
    def test_sensor(_):
        return [
            SensorResult(skip_reason=skip_reason),
        ]

    with instance_for_test() as instance:
        ctx = build_sensor_context(
            instance=instance,
        )
        sensor_data = test_sensor.evaluate_tick(ctx)
        assert not sensor_data.run_requests
        assert sensor_data.skip_message == skip_reason
        assert not sensor_data.dagster_run_reactions
        assert not sensor_data.cursor


def test_invalid_skip_reason_invocations():
    @sensor(job=do_something_job)
    def multiple_sensor_results(_):
        return [
            SensorResult(skip_reason=SkipReason("I'm skipping")),
            SensorResult(skip_reason=SkipReason("I'm skipping")),
        ]

    @sensor(job=do_something_job)
    def sensor_result_w_other_objects(_):
        return [
            SensorResult(run_requests=[RunRequest(run_key="foo")]),
            RunRequest(run_key="foo"),
        ]

    @sensor(job=do_something_job)
    def invalid_sensor_result(_):
        return [
            SensorResult(
                run_requests=[RunRequest(run_key="foo")], skip_reason=SkipReason("aklsdj")
            ),
        ]

    with instance_for_test() as instance:
        ctx = build_sensor_context(
            instance=instance,
        )

        with pytest.raises(
            CheckError,
            match=(
                "When a SensorResult is returned from a sensor, it must be the only object"
                " returned."
            ),
        ):
            multiple_sensor_results.evaluate_tick(ctx)

        with pytest.raises(
            CheckError,
            match=(
                "When a SensorResult is returned from a sensor, it must be the only object"
                " returned."
            ),
        ):
            sensor_result_w_other_objects.evaluate_tick(ctx)

        with pytest.raises(
            CheckError,
            match="Expected a single skip reason or one or more run requests",
        ):
            invalid_sensor_result.evaluate_tick(ctx)


def test_update_cursor():
    @sensor(job=do_something_job)
    def test_sensor(_):
        return [
            SensorResult([RunRequest("foo")], cursor="foo"),
        ]

    with instance_for_test() as instance:
        ctx = build_sensor_context(
            instance=instance,
        )
        result = test_sensor.evaluate_tick(ctx)
        assert result.cursor == "foo"


def test_update_cursor_and_sensor_result_cursor():
    @sensor(job=do_something_job)
    def test_sensor(context):
        context.update_cursor("bar")
        return [
            SensorResult([RunRequest("foo")], cursor="foo"),
        ]

    with instance_for_test() as instance:
        ctx = build_sensor_context(
            instance=instance,
        )
        with pytest.raises(
            DagsterInvariantViolationError,
            match=r"cannot be set if context.update_cursor()",
        ):
            test_sensor.evaluate_tick(ctx)


def test_sensor_result_asset_sensor():
    @op
    def my_table_materialization():
        yield AssetMaterialization("my_table")
        yield Output(1)

    @job
    def my_table_job():
        my_table_materialization()

    @asset_sensor(asset_key=AssetKey("my_table"), job=do_something_job)
    def my_asset_sensor(context, asset_event):
        return SensorResult([RunRequest("foo")])

    @asset_sensor(asset_key=AssetKey("my_table"), job=do_something_job)
    def asset_sensor_set_cursor(context, asset_event):
        return SensorResult([RunRequest("foo")], cursor="foo")

    with instance_for_test() as instance:
        my_table_job.execute_in_process(instance=instance)
        with build_sensor_context(
            instance=instance,
        ) as ctx:
            result = my_asset_sensor.evaluate_tick(ctx)
            assert len(result.run_requests) == 1
            assert result.run_requests[0].run_key == "foo"

        with build_sensor_context(
            instance=instance,
        ) as ctx:
            with pytest.raises(
                DagsterInvariantViolationError, match="The cursor is managed by the asset sensor"
            ):
                asset_sensor_set_cursor.evaluate_tick(ctx)


def test_yield_and_return():
    @job
    def job1():
        pass

    @sensor(job=job1)
    def sensor_with_yield_run_request_and_return_skip_reason(context):
        if context.cursor == "skip":
            return SkipReason("This is a skip reason")
        else:
            yield RunRequest()

    result_with_skip = sensor_with_yield_run_request_and_return_skip_reason.evaluate_tick(
        build_sensor_context(cursor="skip")
    )
    assert result_with_skip.skip_message == "This is a skip reason"
    assert result_with_skip.run_requests == []

    result_without_skip = sensor_with_yield_run_request_and_return_skip_reason.evaluate_tick(
        build_sensor_context(cursor="go")
    )
    assert result_without_skip.skip_message is None
    assert len(result_without_skip.run_requests) == 1

    @sensor(job=job1)
    def sensor_with_yield_and_return_run_request(context):
        yield RunRequest()
        return RunRequest()

    result_yield_and_return_run_request = sensor_with_yield_and_return_run_request.evaluate_tick(
        build_sensor_context()
    )
    assert len(result_yield_and_return_run_request.run_requests) == 2


def test_asset_events_experimental_param_on_sensor_result() -> None:
    assert "asset_events" in get_experimental_params(SensorResult)
