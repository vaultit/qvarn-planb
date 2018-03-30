def merge(source, update):
    if isinstance(source, dict):
        return {**source, **{k: merge(source.get(k), v) for k, v in (update or {}).items()}}
    elif isinstance(source, list):
        return source + (update or [])
    else:
        return source if update is None else update
