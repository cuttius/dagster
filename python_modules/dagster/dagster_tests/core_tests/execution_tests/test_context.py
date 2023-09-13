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


def test_correct_context_provided_no_type_hints():
    # asset
    @asset
    def the_asset(context):
        assert isinstance(context, AssetExecutionContext)

    materialize([the_asset])

    # ops, jobs
    @op
    def the_op(context):
        assert isinstance(context, OpExecutionContext)
        # AssetExecutionContext is an instance of OpExecutionContext, so add this additional check
        assert not isinstance(context, AssetExecutionContext)

    @job
    def the_job():
        the_op()

    assert the_job.execute_in_process().success

    # multi_asset
    @multi_asset(outs={"out1": AssetOut(dagster_type=None), "out2": AssetOut(dagster_type=None)})
    def the_multi_asset(context):
        assert isinstance(context, AssetExecutionContext)
        return None, None

    materialize([the_multi_asset])

    # graph backed asset
    @op
    def the_asset_op(context):
        assert isinstance(context, AssetExecutionContext)

    @graph_asset
    def the_graph_asset():
        return the_asset_op()

    materialize([the_graph_asset])

    # graph backed multi asset
    @graph_multi_asset(
        outs={"out1": AssetOut(dagster_type=None), "out2": AssetOut(dagster_type=None)}
    )
    def the_graph_multi_asset():
        return the_asset_op(), the_asset_op()

    materialize([the_graph_multi_asset])

    # job created using Definitions classes, not decorators
    def plain_python(context, *args):
        assert isinstance(context, OpExecutionContext)
        # AssetExecutionContext is an instance of OpExecutionContext, so add this additional check
        assert not isinstance(context, AssetExecutionContext)
        yield Output(1)

    no_decorator_op = OpDefinition(compute_fn=plain_python, name="no_decorator_op")
    no_decorator_graph = GraphDefinition(name="no_decorator_graph", node_defs=[no_decorator_op])

    no_decorator_graph.to_job(name="no_decorator_job").execute_in_process()


def test_correct_context_provided_with_expected_type_hints():
    # asset
    @asset
    def the_asset(context: AssetExecutionContext):
        assert isinstance(context, AssetExecutionContext)

    materialize([the_asset])

    # ops, jobs
    @op
    def the_op(context: OpExecutionContext):
        assert isinstance(context, OpExecutionContext)
        # AssetExecutionContext is an instance of OpExecutionContext, so add this additional check
        assert not isinstance(context, AssetExecutionContext)

    @job
    def the_job():
        the_op()

    assert the_job.execute_in_process().success

    # multi_asset
    @multi_asset(outs={"out1": AssetOut(dagster_type=None), "out2": AssetOut(dagster_type=None)})
    def the_multi_asset(context: AssetExecutionContext):
        assert isinstance(context, AssetExecutionContext)
        return None, None

    materialize([the_multi_asset])

    # job created using Definitions classes, not decorators
    def plain_python(context: OpExecutionContext, *args):
        assert isinstance(context, OpExecutionContext)
        # AssetExecutionContext is an instance of OpExecutionContext, so add this additional check
        assert not isinstance(context, AssetExecutionContext)
        yield Output(1)

    no_decorator_op = OpDefinition(compute_fn=plain_python, name="no_decorator_op")
    no_decorator_graph = GraphDefinition(name="no_decorator_graph", node_defs=[no_decorator_op])

    no_decorator_graph.to_job(name="no_decorator_job").execute_in_process()


def test_graph_asset_with_op_context():
    # TODO - this test fails right now. How do we want to handle this case?
    # If we want to provide an OpExecutionContext to this op, then we need a way
    # to determine if the asset is a graph-backed asset rather than an @asset or @multi_asset so that we
    # can special case this behavior
    #
    # weird edge case:
    # an op is used in both a job and a graph backed asset. This would mean in the job it would get an
    # OpExecutionContext, but in the graph backed asset it would get an AssetExecutionContext. Once we
    # deprecate the op methods from AssetExecutionContext this will be a problem since a method like
    # describe_op would be accessed as context.describe_op in the job and context.op_execution_context.describe_op
    # in the graph backed asset

    @op
    def the_op(context: OpExecutionContext):
        assert isinstance(context, OpExecutionContext)
        # AssetExecutionContext is an instance of OpExecutionContext, so add this additional check
        assert not isinstance(context, AssetExecutionContext)

    @graph_asset
    def the_graph_asset():
        return the_op()

    materialize([the_graph_asset])

    @graph_multi_asset(
        outs={"out1": AssetOut(dagster_type=None), "out2": AssetOut(dagster_type=None)}
    )
    def the_graph_multi_asset():
        return the_op(), the_op()

    materialize([the_graph_multi_asset])


def test_graph_asset_with_asset_context():
    @op
    def the_op(context: AssetExecutionContext):
        assert isinstance(context, AssetExecutionContext)

    @graph_asset
    def the_graph_asset():
        return the_op()

    materialize([the_graph_asset])

    @graph_multi_asset(
        outs={"out1": AssetOut(dagster_type=None), "out2": AssetOut(dagster_type=None)}
    )
    def the_graph_multi_asset():
        return the_op(), the_op()

    materialize([the_graph_multi_asset])


def test_error_on_context_type_mismatch():
    @op
    def the_op(context: AssetExecutionContext):
        pass

    @job
    def the_job():
        the_op()

    with pytest.raises(
        DagsterInvalidDefinitionError,
        match="When executed in jobs, the op context should be annotated with OpExecutionContext",
    ):
        assert the_job.execute_in_process().success
