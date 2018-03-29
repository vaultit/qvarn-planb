def validated(resource_type, schema, data):
    return {k: v for k, v in data.items() if k not in ('id', 'revision')}
