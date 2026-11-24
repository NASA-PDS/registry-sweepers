#!/usr/bin/env python3
"""
Convert a legacy flat-schema registry mapping into its structured-metadata equivalent.

Legacy (flat) registry indices name nested PDS4 attributes with slash-joined keys, e.g.
    "ops:Harvest_Info/ops:harvest_date_time": {"type": "date"}
Structured-metadata indices (<node>-registry-structured*) store them as object fields, so the
code addresses them with dotted paths (ops:Harvest_Info.ops:harvest_date_time):
    "ops:Harvest_Info": {"properties": {"ops:harvest_date_time": {"type": "date"}}}

Input may be a GET <index>/_mapping response ({"<index>": {"mappings": {...}}}) or a bare
{"mappings": {...}} body. Output is a {"mappings": {...}} body. Mapping-level options other
than "properties" (dynamic, dynamic_templates, ...) are copied unchanged.

Usage:
    python convert_flat_to_structured.py IN.json OUT.json
"""
import json
import sys
from typing import Dict


def convert_properties(flat: Dict[str, dict]) -> Dict[str, dict]:
    nested: Dict[str, dict] = {}
    for key, field_mapping in flat.items():
        *parents, leaf = key.split("/")
        level = nested
        for i, parent in enumerate(parents):
            node = level.setdefault(parent, {"properties": {}})
            if "properties" not in node:
                raise ValueError(f"'{'/'.join(parents[: i + 1])}' is both a leaf field and a parent of '{key}'")
            level = node["properties"]
        if leaf in level:
            raise ValueError(f"'{key}' collides with an existing field after conversion")
        level[leaf] = field_mapping
    return nested


def convert(body: dict) -> dict:
    if "mappings" not in body and len(body) == 1:  # unwrap GET _mapping response
        (body,) = body.values()
    mappings = dict(body["mappings"])
    mappings["properties"] = convert_properties(mappings.get("properties", {}))
    return {"mappings": mappings}


def main(argv) -> None:
    if len(argv) != 3:
        sys.exit(__doc__)
    with open(argv[1]) as f:
        converted = convert(json.load(f))
    with open(argv[2], "w") as f:
        json.dump(converted, f, indent=2)
        f.write("\n")


if __name__ == "__main__":
    main(sys.argv)
