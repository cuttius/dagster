from typing import AbstractSet, Any, Callable, Iterable

import pytest
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    AutoMaterializePolicy,
    DagsterInstance,
    DataVersion,
    Definitions,
    IOManager,
    JobDefinition,
    SensorResult,
    SourceAsset,
    _check as check,
    asset,
    observable_source_asset,
)
from dagster._core.definitions import materialize
from dagster._core.definitions.asset_spec import (
    SYSTEM_METADATA_KEY_ASSET_EXECUTION_TYPE,
    AssetExecutionType,
    AssetSpec,
)
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from dagster._core.definitions.decorators.sensor_decorator import sensor
from dagster._core.definitions.events import AssetObservation
from dagster._core.definitions.freshness_policy import FreshnessPolicy
from dagster._core.definitions.observable_asset import (
    create_assets_def_from_source_asset,
    create_unexecutable_observable_assets_def,
)
from dagster._core.definitions.sensor_definition import (
    SensorDefinition,
    build_sensor_context,
)
from dagster._core.definitions.source_asset import (
    observe_fn_to_op_compute_fn,
)
from dagster._core.definitions.time_window_partitions import DailyPartitionsDefinition
from dagster._core.event_api import EventRecordsFilter
from dagster._core.events import DagsterEventType


def test_observable_asset_basic_creation() -> None:
    assets_def = create_unexecutable_observable_assets_def(
        specs=[
            AssetSpec(
                key="observable_asset_one",
                # multi-asset does not support description lol
                # description="desc",
                metadata={"user_metadata": "value"},
                group_name="a_group",
            )
        ]
    )
    assert isinstance(assets_def, AssetsDefinition)

    expected_key = AssetKey(["observable_asset_one"])

    assert assets_def.key == expected_key
    # assert assets_def.descriptions_by_key[expected_key] == "desc"
    assert assets_def.metadata_by_key[expected_key]["user_metadata"] == "value"
    assert assets_def.group_names_by_key[expected_key] == "a_group"
    assert assets_def.is_asset_executable(expected_key) is False


def test_invalid_observable_asset_creation() -> None:
    invalid_specs = [
        AssetSpec("invalid_asset1", auto_materialize_policy=AutoMaterializePolicy.eager()),
        AssetSpec("invalid_asset2", code_version="ksjdfljs"),
        AssetSpec("invalid_asset2", freshness_policy=FreshnessPolicy(maximum_lag_minutes=1)),
        AssetSpec("invalid_asset2", skippable=True),
    ]

    for invalid_spec in invalid_specs:
        with pytest.raises(check.CheckError):
            create_unexecutable_observable_assets_def(specs=[invalid_spec])


def test_normal_asset_materializeable() -> None:
    @asset
    def an_asset() -> None: ...

    assert an_asset.is_asset_executable(AssetKey(["an_asset"])) is True


def test_observable_asset_creation_with_deps() -> None:
    asset_two = AssetSpec("observable_asset_two")
    assets_def = create_unexecutable_observable_assets_def(
        specs=[
            AssetSpec(
                "observable_asset_one",
                deps=[asset_two.key],  # todo remove key when asset deps accepts it
            )
        ]
    )
    assert isinstance(assets_def, AssetsDefinition)

    expected_key = AssetKey(["observable_asset_one"])

    assert assets_def.key == expected_key
    assert assets_def.asset_deps[expected_key] == {
        AssetKey(["observable_asset_two"]),
    }


def test_how_source_assets_are_backwards_compatible() -> None:
    class DummyIOManager(IOManager):
        def handle_output(self, context, obj) -> None:
            pass

        def load_input(self, context) -> str:
            return "hardcoded"

    source_asset = SourceAsset(key="source_asset", io_manager_def=DummyIOManager())

    @asset
    def an_asset(source_asset: str) -> str:
        return source_asset + "-computed"

    defs_with_source = Definitions(assets=[source_asset, an_asset])

    instance = DagsterInstance.ephemeral()

    result_one = defs_with_source.get_implicit_global_asset_job_def().execute_in_process(
        instance=instance
    )

    assert result_one.success
    assert result_one.output_for_node("an_asset") == "hardcoded-computed"

    defs_with_shim = Definitions(
        assets=[create_assets_def_from_source_asset(source_asset), an_asset]
    )

    assert isinstance(defs_with_shim.get_assets_def("source_asset"), AssetsDefinition)

    result_two = defs_with_shim.get_implicit_global_asset_job_def().execute_in_process(
        instance=instance,
        # currently we have to explicitly select the asset to exclude the source from execution
        asset_selection=[AssetKey("an_asset")],
    )

    assert result_two.success
    assert result_two.output_for_node("an_asset") == "hardcoded-computed"


def get_job_for_assets(defs: Definitions, *coercibles_or_defs) -> JobDefinition:
    job_def = defs.get_implicit_job_def_for_assets(set_from_coercibles_or_defs(coercibles_or_defs))
    assert job_def, "Expected to find a job def"
    return job_def


def set_from_coercibles_or_defs(coercibles_or_defs: Iterable) -> AbstractSet["AssetKey"]:
    return set(
        [
            AssetKey.from_coercible_or_definition(coercible_or_def)
            for coercible_or_def in coercibles_or_defs
        ]
    )


def test_how_partitioned_source_assets_are_backwards_compatible() -> None:
    class DummyIOManager(IOManager):
        def handle_output(self, context, obj) -> None:
            pass

        def load_input(self, context) -> str:
            return "hardcoded"

    partitions_def = DailyPartitionsDefinition(start_date="2021-01-01")
    source_asset = SourceAsset(
        key="source_asset", io_manager_def=DummyIOManager(), partitions_def=partitions_def
    )

    @asset(partitions_def=partitions_def)
    def an_asset(context: AssetExecutionContext, source_asset: str) -> str:
        return source_asset + "-computed-" + context.partition_key

    assert an_asset.partitions_def is partitions_def
    assert source_asset.partitions_def is partitions_def

    defs_with_source = Definitions(assets=[source_asset, an_asset])

    instance = DagsterInstance.ephemeral()

    job_def_without_shim = get_job_for_assets(defs_with_source, an_asset)

    result_one = job_def_without_shim.execute_in_process(
        instance=instance, partition_key="2021-01-02"
    )

    assert result_one.success
    assert result_one.output_for_node("an_asset") == "hardcoded-computed-2021-01-02"

    shimmed_source_asset = create_assets_def_from_source_asset(source_asset)
    defs_with_shim = Definitions(
        assets=[create_assets_def_from_source_asset(source_asset), an_asset]
    )

    assert isinstance(defs_with_shim.get_assets_def("source_asset"), AssetsDefinition)

    job_def_with_shim = get_job_for_assets(defs_with_shim, an_asset, shimmed_source_asset)

    result_two = job_def_with_shim.execute_in_process(
        instance=instance,
        # currently we have to explicitly select the asset to exclude the source from execution
        asset_selection=[AssetKey("an_asset")],
        partition_key="2021-01-03",
    )

    assert result_two.success
    assert result_two.output_for_node("an_asset") == "hardcoded-computed-2021-01-03"


def test_observable_source_asset_decorator() -> None:
    @observable_source_asset
    def an_observable_source_asset() -> DataVersion:
        return DataVersion("foo")

    defs = Definitions(assets=[create_assets_def_from_source_asset(an_observable_source_asset)])

    result = defs.get_implicit_global_asset_job_def().execute_in_process()

    assert result.success

    all_observations = result.get_asset_observation_events()
    assert len(all_observations) == 1
    observation_event = all_observations[0]
    assert observation_event.asset_observation_data.asset_observation.data_version == "foo"

    all_materializations = result.get_asset_materialization_events()
    assert len(all_materializations) == 0


def get_latest_asset_observation(
    instance: DagsterInstance, asset_key: AssetKey
) -> AssetObservation:
    event_records = instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.ASSET_OBSERVATION,
            asset_key=asset_key,
        ),
        limit=1,
    )

    assert len(event_records) == 1

    event_record = event_records[0]

    return check.not_none(event_record.asset_observation)


def test_demonstrate_explicit_sensor_in_user_space() -> None:
    def compute_data_version() -> str:
        return "data_version"

    observing_only_asset_key = AssetKey("observing_only_asset")

    @asset(
        key=observing_only_asset_key,
        metadata={SYSTEM_METADATA_KEY_ASSET_EXECUTION_TYPE: AssetExecutionType.OBSERVATION.value},
    )
    def observing_only_asset(context: AssetExecutionContext) -> None:
        context.log_event(
            AssetObservation(
                asset_key=observing_only_asset_key, tags={DATA_VERSION_TAG: compute_data_version()}
            )
        )

    asset_execution_instance = DagsterInstance.ephemeral()

    assert materialize(assets=[observing_only_asset], instance=asset_execution_instance).success

    assert (
        get_latest_asset_observation(
            asset_execution_instance, observing_only_asset_key
        ).data_version
        == "data_version"
    )

    @sensor(job_name="observing_only_sensor")
    def observing_only_asset_sensor() -> SensorResult:
        return SensorResult(
            asset_events=[
                AssetObservation(
                    asset_key=observing_only_asset_key,
                    tags={DATA_VERSION_TAG: compute_data_version()},
                )
            ]
        )

    sensor_instance = DagsterInstance.ephemeral()

    sensor_execution_data = observing_only_asset_sensor.evaluate_tick(
        build_sensor_context(instance=sensor_instance)
    )

    assert len(sensor_execution_data.asset_events) == 1

    asset_event = sensor_execution_data.asset_events[0]

    assert isinstance(asset_event, AssetObservation)


def create_observation_with_version(
    asset_key: AssetKey, data_version: DataVersion
) -> AssetObservation:
    return AssetObservation(
        asset_key=asset_key,
        tags={DATA_VERSION_TAG: data_version.value},
    )


def assets_def_from_observe_fn(asset_key: AssetKey, observe_fn: Callable[..., Any]):
    @asset(
        key=asset_key,
        metadata={SYSTEM_METADATA_KEY_ASSET_EXECUTION_TYPE: AssetExecutionType.OBSERVATION.value},
    )
    def _asset(context: AssetExecutionContext, **kwargs) -> None:
        assets_def = context.job_def.asset_layer.assets_def_for_asset(context.asset_key)
        op_compute_fn = observe_fn_to_op_compute_fn(
            observe_fn=observe_fn,
            partitions_def=assets_def.partitions_def,
            asset_key=context.asset_key,
        )
        return op_compute_fn.decorated_fn(context, **kwargs)

    return _asset


def sensor_def_from_observe_fn(
    asset_key: AssetKey, job_name: str, observe_fn: Callable[..., Any]
) -> SensorDefinition:
    @sensor(job_name=job_name)
    def _sensor(context, **kwargs) -> SensorResult:
        return SensorResult(
            asset_events=[
                create_observation_with_version(asset_key, observe_fn(context, **kwargs)),
            ]
        )

    return _sensor


def test_framework_support_for_observable_source_assets_on_assets_def() -> None:
    def compute_data_version(context) -> DataVersion:
        return DataVersion("data_version")

    observing_only_asset_key = AssetKey("observing_only_asset")

    observing_only_asset = assets_def_from_observe_fn(
        asset_key=observing_only_asset_key, observe_fn=compute_data_version
    )

    asset_execution_instance = DagsterInstance.ephemeral()

    assert materialize(assets=[observing_only_asset], instance=asset_execution_instance).success

    assert (
        get_latest_asset_observation(
            asset_execution_instance, observing_only_asset_key
        ).data_version
        == "data_version"
    )

    observing_only_asset_sensor = sensor_def_from_observe_fn(
        asset_key=observing_only_asset_key, job_name="some_job", observe_fn=compute_data_version
    )

    sensor_instance = DagsterInstance.ephemeral()

    sensor_execution_data = observing_only_asset_sensor.evaluate_tick(
        build_sensor_context(instance=sensor_instance)
    )

    assert len(sensor_execution_data.asset_events) == 1

    asset_event = sensor_execution_data.asset_events[0]

    assert isinstance(asset_event, AssetObservation)
