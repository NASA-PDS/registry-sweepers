# claude-coded without alteration, but tests out

def flatten_mappings(mapping, sep=".", include_multifields=False):
    """
    Flatten an OpenSearch/Elasticsearch mappings response into a dict mapping
    each individual property's full dot-separated path onto its mapping 'type'.

    Accepts any of these input shapes:
      - full response:   {"<index>": {"mappings": {"properties": {...}}}}
      - mappings body:   {"mappings": {"properties": {...}}}
      - properties body: {"properties": {...}}  or  {...properties...} directly
    """
    props = _locate_properties(mapping)
    out = {}
    _walk(props, "", sep, include_multifields, out)
    return out


def _locate_properties(node):
    if not isinstance(node, dict):
        return {}
    if isinstance(node.get("properties"), dict):
        return node["properties"]
    if isinstance(node.get("mappings"), dict):
        return _locate_properties(node["mappings"])
    # Full response: one or more {index_name: {mappings/properties: ...}}
    merged = {}
    for value in node.values():
        if isinstance(value, dict):
            merged.update(_locate_properties(value))
    return merged


def _walk(props, prefix, sep, include_multifields, out):
    for name, body in props.items():
        if not isinstance(body, dict):
            continue
        path = f"{prefix}{sep}{name}" if prefix else name

        if isinstance(body.get("properties"), dict):
            # object / nested container -> descend, do not record the container
            _walk(body["properties"], path, sep, include_multifields, out)
        else:
            out[path] = body.get("type", "object")

        if include_multifields and isinstance(body.get("fields"), dict):
            for fname, fbody in body["fields"].items():
                if isinstance(fbody, dict):
                    out[f"{path}{sep}{fname}"] = fbody.get("type")
