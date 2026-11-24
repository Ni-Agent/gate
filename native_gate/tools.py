"""Profile-scoped transport; agents own task policy and coordination."""
import json
from client.gate_client import GateError


def _scrub(value, token):
    if isinstance(value, str):
        return value.replace(token, '<redacted>')
    if isinstance(value, list):
        return [_scrub(item, token) for item in value]
    if isinstance(value, dict):
        return {_scrub(key, token): _scrub(item, token) for key, item in value.items()}
    return value


def register_tools(ctx, client):
    def call(args, **kwargs):
        c = client()
        try:
            operation, params = args['operation'], args['arguments']
            if operation == 'upload_binary':
                result = c.upload_binary(**params)
            elif operation == 'download_binary':
                result = c.download_binary_file(**params)
            else:
                result = c.rpc(operation, **params)
            return json.dumps({'status': 200, 'data': result}, ensure_ascii=False)
        except GateError as error:
            return json.dumps({'status': error.status, 'data': _scrub(error.payload, c.token)}, ensure_ascii=False)

    ctx.register_tool(name='gate_call', toolset='gate_native', handler=call, check_fn=lambda: True,
        schema={'name': 'gate_call', 'description':
            'Gate API as this profile. Arguments below: ? means optional; IDs are gate:(tx|bn|tk|tg):lower-hyphen-slug, max 112 characters. '
            'read_resource(id); read_revision(id,revision); create_text(id,body,tags?); '
            'update_text(id,body,expected_revision,expected_hash); seal_revision(id,expected_revision,expected_hash); '
            'delete_resource(id,expected_revision,expected_hash,revision?) tombstones content only. '
            'create_binary(id,mime,tags?); update_binary(id,mime,expected_revision,expected_hash) prepare HTTP instructions only, NOT byte uploads. '
            'create_tag(id,key,value,description_id?); update_tag(id,expected_token,key?,value?,description_id?) where null description clears it. '
            'tag_resource(resource_id,tag_id,revision?,expected_revision?,expected_hash?); '
            'untag_resource(resource_id,tag_id,revision?,expected_revision?,expected_hash?); current content needs revision/hash CAS. '
            'create_task(operation_id,assignee,body,id?,status?,priority?,tags?,verification?); '
            'update_task(id,expected_token,assignee?,body?,status?,priority?,add_session?). '
            'Task/tag edits use the read token; task status: triage,ready,in_progress,blocked,done; verification: [{id,revision?}]. '
            'list_revisions_for_resource(id,after?,limit?,include_deleted?); list_tags_for_resource(id,revision?,as_of?,after?,limit?); '
            'list_resources_for_tag(tag_id,all_tags?,types?,after?,limit?,include_deleted?); '
            'list_tags(id?,key?,value?,tagged_with?,match?,after?,limit?); '
            'list_tasks(identity?,assignee?,creator?,modified_by?,status?,after?,limit?); '
            'list_notification_history(identity?,task_id?,from?,to?,after?,limit?); list_activity(from,to,after?,limit?); '
            'list_sessions_for_task(task_id,after?,limit?); list_verification_candidates(seed_tags). '
            'Paging uses returned after cursors and integer limit 1..200; from/to/as_of are RFC3339 times. '
            'tags/all_tags/tagged_with/seed_tags use tag IDs (tagged_with is one ID); types is an array; revision/expected_revision/priority are integers. '
            'upload_binary(resource_id,source_path,mime,tags?,expected_revision?,expected_hash?) streams a local file; '
            'download_binary(resource_id,path,revision?) hash-verifies before replacing the local file. '
            'A 200 carries the committed result. Fetch guidance when needed; coordinate work and handle conflicts yourself. '
            'No automatic mutation retry or extra readback.',
            'parameters': {'type': 'object', 'properties': {
                'operation': {'type': 'string', 'pattern': '^[a-z_]+$'},
                'arguments': {'type': 'object', 'additionalProperties': True}},
                'required': ['operation', 'arguments'], 'additionalProperties': False}})
