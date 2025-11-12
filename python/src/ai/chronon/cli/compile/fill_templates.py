from gen_thrift.api.ttypes import Join, Team

from ai.chronon import utils
from ai.chronon.cli.compile.compile_context import CompileContext


def _fill_template(table, obj, namespace):
    if table:
        table = table.replace("{{ logged_table }}", utils.log_table_name(obj, full_name=True))
        table = table.replace("{{ db }}", namespace)

    return table


def set_templated_values(obj, cls, compile_context: CompileContext):
    team_obj: Team = compile_context.teams_dict[obj.team]
    namespace = team_obj.outputNamespace

    if cls == Join and obj.bootstrapParts:
        for bootstrap in obj.bootstrapParts:
            bootstrap.table = _fill_template(bootstrap.table, obj, namespace)

        if obj.metaData.dependencies:
            obj.metaData.dependencies = [
                _fill_template(dep, obj, namespace) for dep in obj.metaData.dependencies
            ]
