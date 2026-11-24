"""Read-only host policy prerequisite; the candidate never changes profile policy."""

def require_queue_policy():
    from hermes_cli.config import load_config_readonly
    from gateway.platforms._shared import get_scoped_secret
    display=(load_config_readonly() or {}).get('display',{})
    mode=get_scoped_secret('HERMES_GATEWAY_BUSY_INPUT_MODE') or display.get('busy_input_mode','')
    legacy=get_scoped_secret('HERMES_GATEWAY_BUSY_TEXT_MODE') or display.get('busy_text_mode','')
    if str(mode).strip().lower()!='queue' or str(legacy).strip().lower() not in ('','queue'):
        raise ValueError('Gate native candidate requires host display.busy_input_mode=queue and no non-queue legacy override; restart the isolated host after deliberate policy changes')
