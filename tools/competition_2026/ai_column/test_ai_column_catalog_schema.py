#!/usr/bin/env python3
"""Contract test for seekdb's native AI Column catalog definitions.
Does not create tables, execute SQL, or change any seekdb process.
"""
import ast
from pathlib import Path
import sys

def verify(path):
    tree = ast.parse(Path(path).read_text(encoding="utf-8"))
    selected = []
    captured = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and target.id in (
                    'all_ai_column_definition_def', 'all_ai_column_task_def'):
                selected.append(node)
        elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Name) and call.func.id == 'def_table_schema':
                if len(call.keywords) == 1 and call.keywords[0].arg is None:
                    arg = call.keywords[0].value
                    if isinstance(arg, ast.Name) and arg.id in (
                            'all_ai_column_definition_def', 'all_ai_column_task_def'):
                        selected.append(node)
    code = compile(ast.Module(body=selected, type_ignores=[]), str(path), 'exec')
    sandbox = {'def_table_schema': lambda **kwargs: captured.setdefault(kwargs['table_name'], kwargs)}
    exec(code, {'__builtins__': {'dict': dict}}, sandbox)
    expected_names = {'__all_ai_column_definition', '__all_ai_column_task'}
    assert set(captured) == expected_names, (captured.keys(), expected_names)
    definitions = captured['__all_ai_column_definition']
    tasks = captured['__all_ai_column_task']
    assert definitions['table_id'] == '1135' and tasks['table_id'] == '1136'
    assert definitions['table_type'] == tasks['table_type'] == 'SYSTEM_TABLE'
    def names(rows): return [row[0] for row in rows]
    assert names(definitions['rowkey_columns']) == ['table_id', 'column_id', 'definition_version']
    assert names(tasks['rowkey_columns']) == ['table_id', 'column_id', 'request_id']
    for name in ('schema_version', 'expression_sql', 'dependency_column_ids',
                 'model_name', 'ai_function', 'return_type', 'vector_dimension',
                 'sync_mode', 'max_retries', 'definition_status'):
        assert name in names(definitions['normal_columns']), name
    for name in ('definition_version', 'schema_version', 'row_identity',
                 'row_incarnation', 'dependency_revision', 'group_id', 'group_size',
                 'task_status', 'attempt_count', 'max_retries', 'next_due_us',
                 'lease_owner', 'lease_token', 'lease_deadline_us', 'staged_result',
                 'result_type', 'error_code', 'error_message'):
        assert name in names(tasks['normal_columns']), name
    all_names = names(definitions['rowkey_columns']) + names(definitions['normal_columns'])
    all_tasks = names(tasks['rowkey_columns']) + names(tasks['normal_columns'])
    assert len(set(all_names)) == len(all_names)
    assert len(set(all_tasks)) == len(all_tasks)
    # Check for literal table_id and table_name collisions in the source text.
    # Static table definitions and generated table aliases both matter.
    content = Path(path).read_text(encoding='utf-8')
    assert content.count("table_id = '1135'") == 1
    assert content.count("table_id = '1136'") == 1
    assert content.count("table_name = '__all_ai_column_definition'") == 1
    assert content.count("table_name = '__all_ai_column_task'") == 1
    print('AI_COLUMN_CATALOG_SCHEMA_CONTRACT_PASS tables=2 definition_columns=%s task_columns=%s' %
          (len(all_names), len(all_tasks)))

if __name__ == '__main__':
    p = Path(sys.argv[1]) if len(sys.argv) > 1 else Path('src/share/inner_table/ob_inner_table_schema_def.py')
    verify(p)
