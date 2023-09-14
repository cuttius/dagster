import dagster._check as check
import pytest
from dagster import OpExecutionContext, asset, job, materialize, op
from dagster._core.definitions.asset_out import AssetOut
from dagster._core.definitions.decorators.asset_decorator import (
    graph_asset,
    graph_multi_asset,
    multi_asset,
)
from dagster._core.definitions.events import Output
from dagster._core.definitions.graph_definition import GraphDefinition
from dagster._core.definitions.job_definition import JobDefinition
from dagster._core.definitions.op_definition import OpDefinition
from dagster._core.errors import DagsterInvalidDefinitionError
from dagster._core.execution.context.compute import AssetExecutionContext
from dagster._core.storage.dagster_run import DagsterRun


def test_op_execution_context():
    @op
    def ctx_op(context: OpExecutionContext):
        check.inst(context.run, DagsterRun)
        assert context.job_name == "foo"
        assert context.job_def.name == "foo"
        check.inst(context.job_def, JobDefinition)
        assert context.op_config is None
        check.inst(context.op_def, OpDefinition)

    @job
    def foo():
        ctx_op()

    assert foo.execute_in_process().success


def test_context_provided_to_asset():
    @asset
    def no_annotation(context):
        assert isinstance(context, AssetExecutionContext)

    materialize([no_annotation])

    @asset
    def asset_annotation(context: AssetExecutionContext):
        assert isinstance(context, AssetExecutionContext)

    materialize([asset_annotation])

    @asset
    def op_annotation(context: OpExecutionContext):
        assert isinstance(context, OpExecutionContext)
        # AssetExecutionContext is an instance of OpExecutionContext, so add this additional check
        assert not isinstance(context, AssetExecutionContext)

    materialize([op_annotation])


def test_context_provided_to_op():
    @op
    def no_annotation(context):
        assert isinstance(context, OpExecutionContext)
        # AssetExecutionContext is an instance of OpExecutionContext, so add this additional check
        assert not isinstance(context, AssetExecutionContext)

    @job
    def no_annotation_job():
        no_annotation()

    assert no_annotation_job.execute_in_process().success

    @op
    def asset_annotation(context: AssetExecutionContext):
        assert isinstance(context, AssetExecutionContext)

    @job
    def asset_annotation_job():
        asset_annotation()

    assert asset_annotation_job.execute_in_process().success

    @op
    def op_annotation(context: OpExecutionContext):
        assert isinstance(context, OpExecutionContext)
        # AssetExecutionContext is an instance of OpExecutionContext, so add this additional check
        assert not isinstance(context, AssetExecutionContext)

    @job
    def op_annotation_job():
        op_annotation()

    assert op_annotation_job.execute_in_process().success


def test_context_provided_to_multi_asset():
    @multi_asset(outs={"out1": AssetOut(dagster_type=None), "out2": AssetOut(dagster_type=None)})
    def no_annotation(context):
        assert isinstance(context, AssetExecutionContext)
        return None, None

    materialize([no_annotation])

    @multi_asset(outs={"out1": AssetOut(dagster_type=None), "out2": AssetOut(dagster_type=None)})
    def asset_annotation(context: AssetExecutionContext):
        assert isinstance(context, AssetExecutionContext)
        return None, None

    materialize([asset_annotation])

    @multi_asset(outs={"out1": AssetOut(dagster_type=None), "out2": AssetOut(dagster_type=None)})
    def op_annotation(context: OpExecutionContext):
        assert isinstance(context, OpExecutionContext)
        # AssetExecutionContext is an instance of OpExecutionContext, so add this additional check
        assert not isinstance(context, AssetExecutionContext)
        return None, None

    materialize([op_annotation])


def test_context_provided_to_graph_asset():
    @op
    def no_annotation_op(context):
        assert isinstance(context, OpExecutionContext)
        # AssetExecutionContext is an instance of OpExecutionContext, so add this additional check
        assert not isinstance(
            context, AssetExecutionContext
        )  # fails, context is an AssetExecutionContext in current impl

    @graph_asset
    def no_annotation_asset():
        return no_annotation_op()

    materialize([no_annotation_asset])

    @op
    def asset_annotation_op(context: AssetExecutionContext):
        assert isinstance(context, AssetExecutionContext)

    @graph_asset
    def asset_annotation_asset():
        return asset_annotation_op()

    materialize([asset_annotation_asset])

    @op
    def op_annotation_op(context: OpExecutionContext):
        assert isinstance(context, OpExecutionContext)
        # AssetExecutionContext is an instance of OpExecutionContext, so add this additional check
        assert not isinstance(context, AssetExecutionContext)

    @graph_asset
    def op_annotation_asset():
        return op_annotation_op()

    materialize([op_annotation_asset])


def test_context_provided_to_graph_multi_asset():
    @op
    def no_annotation_op(context):
        assert isinstance(context, OpExecutionContext)
        # AssetExecutionContext is an instance of OpExecutionContext, so add this additional check
        assert not isinstance(
            context, AssetExecutionContext
        )  # fails, context is an AssetExecutionContext in current impl

    @graph_multi_asset(
        outs={"out1": AssetOut(dagster_type=None), "out2": AssetOut(dagster_type=None)}
    )
    def no_annotation_asset():
        return no_annotation_op(), no_annotation_op()

    materialize([no_annotation_asset])

    @op
    def asset_annotation_op(context: AssetExecutionContext):
        assert isinstance(context, AssetExecutionContext)

    @graph_multi_asset(
        outs={"out1": AssetOut(dagster_type=None), "out2": AssetOut(dagster_type=None)}
    )
    def asset_annotation_asset():
        return asset_annotation_op(), asset_annotation_op()

    materialize([asset_annotation_asset])

    @op
    def op_annotation_op(context: OpExecutionContext):
        assert isinstance(context, OpExecutionContext)
        # AssetExecutionContext is an instance of OpExecutionContext, so add this additional check
        assert not isinstance(context, AssetExecutionContext)

    @graph_multi_asset(
        outs={"out1": AssetOut(dagster_type=None), "out2": AssetOut(dagster_type=None)}
    )
    def op_annotation_asset():
        return op_annotation_op(), op_annotation_op()

    materialize([op_annotation_asset])


def test_context_provided_to_plain_python():
    # tests a job created using Definitions classes, not decorators

    def no_annotation(context, *args):
        assert isinstance(context, OpExecutionContext)
        # AssetExecutionContext is an instance of OpExecutionContext, so add this additional check
        assert not isinstance(context, AssetExecutionContext)
        yield Output(1)

    no_annotation_op = OpDefinition(compute_fn=no_annotation, name="no_annotation_op")
    no_annotation_graph = GraphDefinition(name="no_annotation_graph", node_defs=[no_annotation_op])

    no_annotation_graph.to_job(name="no_annotation_job").execute_in_process()

    def asset_annotation(context: AssetExecutionContext, *args):
        assert isinstance(context, AssetExecutionContext)
        yield Output(1)

    asset_annotation_op = OpDefinition(compute_fn=asset_annotation, name="asset_annotation_op")
    asset_annotation_graph = GraphDefinition(
        name="asset_annotation_graph", node_defs=[asset_annotation_op]
    )

    asset_annotation_graph.to_job(name="asset_annotation_job").execute_in_process()

    def op_annotation(context: OpExecutionContext, *args):
        assert isinstance(context, OpExecutionContext)
        # AssetExecutionContext is an instance of OpExecutionContext, so add this additional check
        assert not isinstance(context, AssetExecutionContext)
        yield Output(1)

    op_annotation_op = OpDefinition(compute_fn=op_annotation, name="op_annotation_op")
    op_annotation_graph = GraphDefinition(name="op_annotation_graph", node_defs=[op_annotation_op])

    op_annotation_graph.to_job(name="op_annotation_job").execute_in_process()


def test_error_on_invalid_context_annotation():
    @op
    def the_op(context: int):
        pass

    @job
    def the_job():
        the_op()

    with pytest.raises(
        DagsterInvalidDefinitionError,
        match="must be annotated with AssetExecutionContext, OpExecutionContext, or left blank",
    ):
        assert the_job.execute_in_process()
