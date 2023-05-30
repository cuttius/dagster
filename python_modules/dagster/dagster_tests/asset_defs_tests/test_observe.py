from typing import Optional

import pytest
from dagster import DataVersionsByPartition, StaticPartitionsDefinition
from dagster._core.definitions.data_version import (
    DataVersion,
    extract_data_version_from_entry,
)
from dagster._core.definitions.decorators.source_asset_decorator import observable_source_asset
from dagster._core.definitions.events import AssetKey
from dagster._core.definitions.observe import observe
from dagster._core.definitions.resource_definition import ResourceDefinition, resource
from dagster._core.errors import (
    DagsterInvalidConfigError,
    DagsterInvalidDefinitionError,
    DagsterInvalidObservationError,
)
from dagster._core.instance import DagsterInstance


def _get_current_data_version(
    key: AssetKey, instance: DagsterInstance, partition_key: Optional[str] = None
) -> Optional[DataVersion]:
    record = instance.get_latest_data_version_record(key)
    assert record is not None
    return extract_data_version_from_entry(record.event_log_entry)


def test_basic_observe():
    @observable_source_asset
    def foo(_context) -> DataVersion:
        return DataVersion("alpha")

    instance = DagsterInstance.ephemeral()

    observe([foo], instance=instance)
    assert _get_current_data_version(AssetKey("foo"), instance) == DataVersion("alpha")


def test_observe_partitions():
    @observable_source_asset(partitions_def=StaticPartitionsDefinition(["apple", "orange", "kiwi"]))
    def foo():
        return DataVersionsByPartition({"apple": "one", "orange": DataVersion("two")})

    result = observe([foo])
    observations = result.asset_observations_for_node("foo")
    assert len(observations) == 2
    observations_by_asset_partition = {
        (observation.asset_key, observation.partition): observation for observation in observations
    }
    assert observations_by_asset_partition.keys() == {(foo.key, "apple"), (foo.key, "orange")}
    assert observations_by_asset_partition[(foo.key, "apple")].tags == {
        "dagster/data_version": "one"
    }
    assert observations_by_asset_partition[(foo.key, "orange")].tags == {
        "dagster/data_version": "two"
    }


def test_observe_partitions_non_partitioned_asset():
    @observable_source_asset
    def foo():
        return DataVersionsByPartition({"apple": "one", "orange": DataVersion("two")})

    with pytest.raises(DagsterInvalidObservationError):
        observe([foo])


def test_observe_data_version_partitioned_asset():
    @observable_source_asset(partitions_def=StaticPartitionsDefinition(["apple", "orange", "kiwi"]))
    def foo():
        return DataVersion("one")

    with pytest.raises(DagsterInvalidObservationError):
        observe([foo])


def test_observe_tags():
    @observable_source_asset
    def foo(_context) -> DataVersion:
        return DataVersion("alpha")

    instance = DagsterInstance.ephemeral()

    result = observe([foo], instance=instance, tags={"key1": "value1"})
    assert result.success
    assert result.dagster_run.tags == {"key1": "value1"}


def test_observe_raise_on_error():
    @observable_source_asset
    def foo(_context) -> DataVersion:
        raise ValueError()

    instance = DagsterInstance.ephemeral()
    assert not observe([foo], raise_on_error=False, instance=instance).success


@pytest.mark.parametrize(
    "is_valid,resource_defs",
    [(True, {"bar": ResourceDefinition.hardcoded_resource("bar")}), (False, {})],
)
def test_observe_resource(is_valid, resource_defs):
    @observable_source_asset(
        required_resource_keys={"bar"},
        resource_defs=resource_defs,
    )
    def foo(context) -> DataVersion:
        return DataVersion(f"{context.resources.bar}-alpha")

    instance = DagsterInstance.ephemeral()

    if is_valid:
        observe([foo], instance=instance)
        assert _get_current_data_version(AssetKey("foo"), instance) == DataVersion("bar-alpha")
    else:
        with pytest.raises(
            DagsterInvalidDefinitionError,
            match="resource with key 'bar' required by op 'foo' was not provided",
        ):
            observe([foo], instance=instance)


@pytest.mark.parametrize(
    "is_valid,config_value",
    [(True, {"resources": {"bar": {"config": {"baz": "baz"}}}}), (False, {"fake": "fake"})],
)
def test_observe_config(is_valid, config_value):
    @resource(config_schema={"baz": str})
    def bar(context):
        return context.resource_config["baz"]

    @observable_source_asset(required_resource_keys={"bar"}, resource_defs={"bar": bar})
    def foo(context) -> DataVersion:
        return DataVersion(f"{context.resources.bar}-alpha")

    instance = DagsterInstance.ephemeral()

    if is_valid:
        observe([foo], instance=instance, run_config=config_value)
        assert _get_current_data_version(AssetKey("foo"), instance) == DataVersion("baz-alpha")
    else:
        with pytest.raises(DagsterInvalidConfigError, match="Error in config for job"):
            observe([foo], instance=instance, run_config=config_value)
