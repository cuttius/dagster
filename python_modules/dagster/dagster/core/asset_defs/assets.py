from typing import AbstractSet, Dict, Iterable, Iterator, Mapping, Optional, Sequence, Set, cast

import dagster._check as check
from dagster.core.decorator_utils import get_function_params
from dagster.core.definitions import (
    GraphDefinition,
    NodeDefinition,
    NodeHandle,
    OpDefinition,
    ResourceDefinition,
)
from dagster.core.definitions.events import AssetKey
from dagster.core.definitions.partition import PartitionsDefinition
from dagster.core.definitions.utils import DEFAULT_GROUP_NAME, validate_group_name
from dagster.core.execution.context.compute import OpExecutionContext
from dagster.utils import merge_dicts

from ..definitions.resource_requirement import (
    ResourceAddable,
    ResourceRequirement,
    ensure_requirements_satisfied,
)
from .partition_mapping import PartitionMapping
from .source_asset import SourceAsset


class AssetsDefinition(ResourceAddable):
    def __init__(
        self,
        asset_keys_by_input_name: Mapping[str, AssetKey],
        asset_keys_by_output_name: Mapping[str, AssetKey],
        node_def: NodeDefinition,
        partitions_def: Optional[PartitionsDefinition] = None,
        partition_mappings: Optional[Mapping[AssetKey, PartitionMapping]] = None,
        asset_deps: Optional[Mapping[AssetKey, AbstractSet[AssetKey]]] = None,
        selected_asset_keys: Optional[AbstractSet[AssetKey]] = None,
        can_subset: bool = False,
        resource_defs: Optional[Mapping[str, ResourceDefinition]] = None,
        group_names: Optional[Mapping[AssetKey, str]] = None,
        # if adding new fields, make sure to handle them in the with_prefix_or_group method
    ):
        self._node_def = node_def
        self._asset_keys_by_input_name = check.dict_param(
            asset_keys_by_input_name,
            "asset_keys_by_input_name",
            key_type=str,
            value_type=AssetKey,
        )
        self._asset_keys_by_output_name = check.dict_param(
            asset_keys_by_output_name,
            "asset_keys_by_output_name",
            key_type=str,
            value_type=AssetKey,
        )

        self._partitions_def = partitions_def
        self._partition_mappings = partition_mappings or {}

        # if not specified assume all output assets depend on all input assets
        all_asset_keys = set(asset_keys_by_output_name.values())
        self._asset_deps = asset_deps or {
            out_asset_key: set(asset_keys_by_input_name.values())
            for out_asset_key in all_asset_keys
        }
        check.invariant(
            set(self._asset_deps.keys()) == all_asset_keys,
            "The set of asset keys with dependencies specified in the asset_deps argument must "
            "equal the set of asset keys produced by this AssetsDefinition. \n"
            f"asset_deps keys: {set(self._asset_deps.keys())} \n"
            f"expected keys: {all_asset_keys}",
        )
        self._resource_defs = check.opt_mapping_param(resource_defs, "resource_defs")

        group_names = check.mapping_param(group_names, "group_names") if group_names else {}
        self._group_names = {}
        # assets that don't have a group name get a DEFAULT_GROUP_NAME
        for key in all_asset_keys:
            group_name = group_names.get(key)
            self._group_names[key] = validate_group_name(group_name)

        if selected_asset_keys is not None:
            self._selected_asset_keys = selected_asset_keys
        else:
            self._selected_asset_keys = all_asset_keys
        self._can_subset = can_subset

        self._metadata_by_asset_key = {
            asset_key: node_def.resolve_output_to_origin(output_name, None)[0].metadata
            for output_name, asset_key in asset_keys_by_output_name.items()
        }

    def __call__(self, *args, **kwargs):
        from dagster.core.definitions.decorators.solid_decorator import DecoratedSolidFunction

        if isinstance(self.node_def, GraphDefinition):
            return self._node_def(*args, **kwargs)
        solid_def = self.op
        provided_context: Optional[OpExecutionContext] = None
        if len(args) > 0 and isinstance(args[0], OpExecutionContext):
            provided_context = _build_invocation_context_with_included_resources(
                self.resource_defs, args[0]
            )
            new_args = [provided_context, *args[1:]]
            return solid_def(*new_args, **kwargs)
        elif (
            isinstance(solid_def.compute_fn.decorated_fn, DecoratedSolidFunction)
            and solid_def.compute_fn.has_context_arg()
        ):
            context_param_name = get_function_params(solid_def.compute_fn.decorated_fn)[0].name
            if context_param_name in kwargs:
                provided_context = _build_invocation_context_with_included_resources(
                    self.resource_defs, kwargs[context_param_name]
                )
                new_kwargs = dict(kwargs)
                new_kwargs[context_param_name] = provided_context
                return solid_def(*args, **new_kwargs)

        return solid_def(*args, **kwargs)

    @staticmethod
    def from_graph(
        graph_def: GraphDefinition,
        asset_keys_by_input_name: Optional[Mapping[str, AssetKey]] = None,
        asset_keys_by_output_name: Optional[Mapping[str, AssetKey]] = None,
        internal_asset_deps: Optional[Mapping[str, Set[AssetKey]]] = None,
        partitions_def: Optional[PartitionsDefinition] = None,
    ) -> "AssetsDefinition":
        """
        Constructs an AssetsDefinition from a GraphDefinition.

        Args:
            graph_def (GraphDefinition): The GraphDefinition that is an asset.
            asset_keys_by_input_name (Optional[Mapping[str, AssetKey]]): A mapping of the input
                names of the decorated graph to their corresponding asset keys. If not provided,
                the input asset keys will be created from the graph input names.
            asset_keys_by_output_name (Optional[Mapping[str, AssetKey]]): A mapping of the output
                names of the decorated graph to their corresponding asset keys. If not provided,
                the output asset keys will be created from the graph output names.
            internal_asset_deps (Optional[Mapping[str, Set[AssetKey]]]): By default, it is assumed
                that all assets produced by the graph depend on all assets that are consumed by that
                graph. If this default is not correct, you pass in a map of output names to a
                corrected set of AssetKeys that they depend on. Any AssetKeys in this list must be
                either used as input to the asset or produced within the graph.
            partitions_def (Optional[PartitionsDefinition]): Defines the set of partition keys that
                compose the assets.
        """
        graph_def = check.inst_param(graph_def, "graph_def", GraphDefinition)
        asset_keys_by_input_name = check.opt_dict_param(
            asset_keys_by_input_name, "asset_keys_by_input_name", key_type=str, value_type=AssetKey
        )
        asset_keys_by_output_name = check.opt_dict_param(
            asset_keys_by_output_name,
            "asset_keys_by_output_name",
            key_type=str,
            value_type=AssetKey,
        )
        internal_asset_deps = check.opt_dict_param(
            internal_asset_deps, "internal_asset_deps", key_type=str, value_type=set
        )

        transformed_internal_asset_deps = {}
        if internal_asset_deps:
            for output_name, asset_keys in internal_asset_deps.items():
                check.invariant(
                    output_name in asset_keys_by_output_name,
                    f"output_name {output_name} specified in internal_asset_deps does not exist in the decorated function",
                )
                transformed_internal_asset_deps[asset_keys_by_output_name[output_name]] = asset_keys

        return AssetsDefinition(
            asset_keys_by_input_name=_infer_asset_keys_by_input_names(
                graph_def,
                asset_keys_by_input_name or {},
            ),
            asset_keys_by_output_name=_infer_asset_keys_by_output_names(
                graph_def, asset_keys_by_output_name or {}
            ),
            node_def=graph_def,
            asset_deps=transformed_internal_asset_deps or None,
            partitions_def=check.opt_inst_param(
                partitions_def, "partitions_def", PartitionsDefinition
            ),
        )

    @property
    def can_subset(self) -> bool:
        return self._can_subset

    @property
    def group_names(self) -> Mapping[AssetKey, str]:
        return self._group_names

    @property
    def op(self) -> OpDefinition:
        check.invariant(
            isinstance(self._node_def, OpDefinition),
            "The NodeDefinition for this AssetsDefinition is not of type OpDefinition.",
        )
        return cast(OpDefinition, self._node_def)

    @property
    def node_def(self) -> NodeDefinition:
        return self._node_def

    @property
    def asset_deps(self) -> Mapping[AssetKey, AbstractSet[AssetKey]]:
        return self._asset_deps

    @property
    def asset_key(self) -> AssetKey:
        check.invariant(
            len(self.asset_keys) == 1,
            "Tried to retrieve asset key from an assets definition with multiple asset keys: "
            + ", ".join([str(ak.to_string()) for ak in self._asset_keys_by_output_name.values()]),
        )

        return next(iter(self.asset_keys))

    @property
    def resource_defs(self) -> Dict[str, ResourceDefinition]:
        return dict(self._resource_defs)

    @property
    def asset_keys(self) -> AbstractSet[AssetKey]:
        return self._selected_asset_keys

    @property
    def dependency_asset_keys(self) -> Iterable[AssetKey]:
        # the input asset keys that are directly upstream of a selected asset key
        upstream_keys = set().union(*(self.asset_deps[key] for key in self.asset_keys))
        input_keys = set(self._asset_keys_by_input_name.values())
        return upstream_keys.intersection(input_keys)

    @property
    def node_asset_keys_by_output_name(self) -> Mapping[str, AssetKey]:
        """AssetKey for each output on the underlying NodeDefinition"""
        return self._asset_keys_by_output_name

    @property
    def node_asset_keys_by_input_name(self) -> Mapping[str, AssetKey]:
        """AssetKey for each input on the underlying NodeDefinition"""
        return self._asset_keys_by_input_name

    @property
    def asset_keys_by_output_name(self) -> Mapping[str, AssetKey]:
        return {
            name: key
            for name, key in self.node_asset_keys_by_output_name.items()
            if key in self.asset_keys
        }

    @property
    def asset_keys_by_input_name(self) -> Mapping[str, AssetKey]:
        upstream_keys = set().union(*(self.asset_deps[key] for key in self.asset_keys))
        return {
            name: key
            for name, key in self.node_asset_keys_by_input_name.items()
            if key in upstream_keys
        }

    @property
    def partitions_def(self) -> Optional[PartitionsDefinition]:
        return self._partitions_def

    @property
    def metadata_by_asset_key(self):
        return self._metadata_by_asset_key

    def get_partition_mapping(self, in_asset_key: AssetKey) -> PartitionMapping:
        if self._partitions_def is None:
            check.failed("Asset is not partitioned")

        return self._partition_mappings.get(
            in_asset_key,
            self._partitions_def.get_default_partition_mapping(),
        )

    def with_prefix_or_group(
        self,
        output_asset_key_replacements: Optional[Mapping[AssetKey, AssetKey]] = None,
        input_asset_key_replacements: Optional[Mapping[AssetKey, AssetKey]] = None,
        group_names: Optional[Mapping[AssetKey, str]] = None,
    ) -> "AssetsDefinition":
        from dagster import DagsterInvalidDefinitionError

        output_asset_key_replacements = check.opt_dict_param(
            output_asset_key_replacements,
            "output_asset_key_replacements",
            key_type=AssetKey,
            value_type=AssetKey,
        )
        input_asset_key_replacements = check.opt_dict_param(
            input_asset_key_replacements,
            "input_asset_key_replacements",
            key_type=AssetKey,
            value_type=AssetKey,
        )
        group_names = check.opt_dict_param(
            group_names, "group_names", key_type=AssetKey, value_type=str
        )

        defined_group_names = [
            asset_key.to_user_string()
            for asset_key in group_names
            if asset_key in self.group_names and self.group_names[asset_key] != DEFAULT_GROUP_NAME
        ]
        if defined_group_names:
            raise DagsterInvalidDefinitionError(
                f"Group name already exists on assets {', '.join(defined_group_names)}"
            )

        return self.__class__(
            asset_keys_by_input_name={
                input_name: input_asset_key_replacements.get(key, key)
                for input_name, key in self._asset_keys_by_input_name.items()
            },
            asset_keys_by_output_name={
                output_name: output_asset_key_replacements.get(key, key)
                for output_name, key in self._asset_keys_by_output_name.items()
            },
            node_def=self.node_def,
            partitions_def=self.partitions_def,
            partition_mappings=self._partition_mappings,
            asset_deps={
                # replace both the keys and the values in this mapping
                output_asset_key_replacements.get(key, key): {
                    input_asset_key_replacements.get(
                        upstream_key,
                        output_asset_key_replacements.get(upstream_key, upstream_key),
                    )
                    for upstream_key in value
                }
                for key, value in self.asset_deps.items()
            },
            can_subset=self.can_subset,
            selected_asset_keys={
                output_asset_key_replacements.get(key, key) for key in self._selected_asset_keys
            },
            resource_defs=self.resource_defs,
            group_names={**self.group_names, **group_names},
        )

    def subset_for(self, selected_asset_keys: AbstractSet[AssetKey]) -> "AssetsDefinition":
        """
        Create a subset of this AssetsDefinition that will only materialize the assets in the
        selected set.

        Args:
            selected_asset_keys (AbstractSet[AssetKey]): The total set of asset keys
        """
        check.invariant(
            self.can_subset,
            f"Attempted to subset AssetsDefinition for {self.node_def.name}, but can_subset=False.",
        )
        return AssetsDefinition(
            # keep track of the original mapping
            asset_keys_by_input_name=self._asset_keys_by_input_name,
            asset_keys_by_output_name=self._asset_keys_by_output_name,
            # TODO: subset this properly for graph-backed-assets
            node_def=self.node_def,
            partitions_def=self.partitions_def,
            partition_mappings=self._partition_mappings,
            asset_deps=self._asset_deps,
            can_subset=self.can_subset,
            selected_asset_keys=selected_asset_keys & self.asset_keys,
            resource_defs=self.resource_defs,
        )

    def to_source_assets(self) -> Sequence[SourceAsset]:
        result = []
        for output_name, asset_key in self.asset_keys_by_output_name.items():
            # This could maybe be sped up by batching
            output_def = self.node_def.resolve_output_to_origin(
                output_name, NodeHandle(self.node_def.name, parent=None)
            )[0]
            result.append(
                SourceAsset(
                    key=asset_key,
                    metadata=output_def.metadata,
                    io_manager_key=output_def.io_manager_key,
                    description=output_def.description,
                    resource_defs=self.resource_defs,
                    partitions_def=self.partitions_def,
                )
            )

        return result

    def get_resource_requirements(self) -> Iterator[ResourceRequirement]:
        yield from self.node_def.get_resource_requirements()  # type: ignore[attr-defined]
        for source_key, resource_def in self.resource_defs.items():
            yield from resource_def.get_resource_requirements(outer_context=source_key)

    @property
    def required_resource_keys(self) -> Set[str]:
        return {requirement.key for requirement in self.get_resource_requirements()}

    def with_resources(self, resource_defs: Mapping[str, ResourceDefinition]) -> "AssetsDefinition":
        from dagster.core.execution.resources_init import get_transitive_required_resource_keys

        merged_resource_defs = merge_dicts(resource_defs, self.resource_defs)

        # Ensure top-level resource requirements are met - except for
        # io_manager, since that is a default it can be resolved later.
        ensure_requirements_satisfied(merged_resource_defs, list(self.get_resource_requirements()))

        # Get all transitive resource dependencies from other resources.
        relevant_keys = get_transitive_required_resource_keys(
            self.required_resource_keys, merged_resource_defs
        )
        relevant_resource_defs = {
            key: resource_def
            for key, resource_def in merged_resource_defs.items()
            if key in relevant_keys
        }

        return AssetsDefinition(
            asset_keys_by_input_name=self._asset_keys_by_input_name,
            asset_keys_by_output_name=self._asset_keys_by_output_name,
            node_def=self.node_def,
            partitions_def=self._partitions_def,
            partition_mappings=self._partition_mappings,
            asset_deps=self._asset_deps,
            selected_asset_keys=self._selected_asset_keys,
            can_subset=self._can_subset,
            resource_defs=relevant_resource_defs,
            group_names=self._group_names,
        )


def _infer_asset_keys_by_input_names(
    graph_def: GraphDefinition, asset_keys_by_input_name: Mapping[str, AssetKey]
) -> Mapping[str, AssetKey]:
    all_input_names = {graph_input.definition.name for graph_input in graph_def.input_mappings}

    if asset_keys_by_input_name:
        check.invariant(
            set(asset_keys_by_input_name.keys()) == all_input_names,
            "The set of input names keys specified in the asset_keys_by_input_name argument must "
            "equal the set of asset keys inputted by this GraphDefinition. \n"
            f"asset_keys_by_input_name keys: {set(asset_keys_by_input_name.keys())} \n"
            f"expected keys: {all_input_names}",
        )

    # If asset key is not supplied in asset_keys_by_input_name, create asset key
    # from input name
    inferred_input_names_by_asset_key: Dict[str, AssetKey] = {
        input_name: asset_keys_by_input_name.get(input_name, AssetKey([input_name]))
        for input_name in all_input_names
    }

    return inferred_input_names_by_asset_key


def _infer_asset_keys_by_output_names(
    graph_def: GraphDefinition, asset_keys_by_output_name: Mapping[str, AssetKey]
) -> Mapping[str, AssetKey]:
    output_names = [output_def.name for output_def in graph_def.output_defs]
    if asset_keys_by_output_name:
        check.invariant(
            set(asset_keys_by_output_name.keys()) == set(output_names),
            "The set of output names keys specified in the asset_keys_by_output_name argument must "
            "equal the set of asset keys outputted by this GraphDefinition. \n"
            f"asset_keys_by_input_name keys: {set(asset_keys_by_output_name.keys())} \n"
            f"expected keys: {set(output_names)}",
        )

    inferred_asset_keys_by_output_names: Dict[str, AssetKey] = {
        output_name: asset_key for output_name, asset_key in asset_keys_by_output_name.items()
    }

    if (
        len(output_names) == 1
        and output_names[0] not in asset_keys_by_output_name
        and output_names[0] == "result"
    ):
        # If there is only one output and the name is the default "result", generate asset key
        # from the name of the node
        inferred_asset_keys_by_output_names[output_names[0]] = AssetKey([graph_def.name])

    for output_name in output_names:
        if output_name not in inferred_asset_keys_by_output_names:
            inferred_asset_keys_by_output_names[output_name] = AssetKey([output_name])
    return inferred_asset_keys_by_output_names


def _build_invocation_context_with_included_resources(
    resource_defs: Dict[str, ResourceDefinition], context: OpExecutionContext
) -> OpExecutionContext:
    from dagster.core.execution.context.invocation import (
        UnboundSolidExecutionContext,
        build_op_context,
    )

    override_resources = context.resources._asdict()
    all_resources = merge_dicts(resource_defs, override_resources)

    if isinstance(context, UnboundSolidExecutionContext):
        context = cast(UnboundSolidExecutionContext, context)
        # pylint: disable=protected-access
        return build_op_context(
            resources=all_resources,
            config=context.solid_config,
            resources_config=context._resources_config,
            instance=context._instance,
            partition_key=context._partition_key,
            mapping_key=context._mapping_key,
        )
    else:
        # If user is mocking OpExecutionContext, send it through (we don't know
        # what modifications they might be making, and we don't want to override)
        return context
