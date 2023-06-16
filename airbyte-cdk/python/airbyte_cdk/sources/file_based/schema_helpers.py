#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from copy import deepcopy
from typing import Any, Dict, Literal, Mapping

from airbyte_cdk.sources.file_based.exceptions import SchemaInferenceError

type_widths = {str: 0}

JsonSchemaType = Literal["string"]
supported_types = {"string"}


def merge_schemas(schema1: Dict[str, JsonSchemaType], schema2: Dict[str, JsonSchemaType]) -> Dict[str, JsonSchemaType]:
    """
    Returns a new dictionary that contains schema1 and schema2.

    Schemas are merged as follows
    - If a key is in one schema but not the other, add it to the base schema with its existing type.
    - If a key is in both schemas but with different types, use the wider type.
    - If the type is a list in one schema but a different type of element in the other schema, raise an exception.
    - If the type is an object in both schemas but the objects are different raise an exception.
    - If the type is an object in one schema but not in the other schema, raise an exception.

    In other words, we support merging
    - any atomic type with any other atomic type (choose the wider of the two)
    - list with list (union)
    and nothing else.
    """
    for k, t in list(schema1.items()) + list(schema2.items()):
        assert _is_valid_type(t), f"Unsupported type in schema at {k}: {t}"

    merged_schema = deepcopy(schema1)
    for k2, t2 in schema2.items():
        t1 = merged_schema.get(k2)
        if t1 is None:
            merged_schema[k2] = t2
        elif t1 == t2:
            continue
        else:
            merged_schema[k2] = _choose_wider_type(k2, t1, t2)

    return merged_schema


def _is_valid_type(t: str) -> bool:
    return t in supported_types


def _choose_wider_type(key: str, t1: JsonSchemaType, t2: JsonSchemaType) -> JsonSchemaType:
    # TODO: update with additional types.
    if t1 is None and t2 is None:
        raise SchemaInferenceError(f"Null value found in schema at {key}.")
    elif t1 is None or t2 is None:
        return t1 or t2
    else:
        raise SchemaInferenceError(f"Unrecognized type while merging schema field '{key}': {t1}, {t2}")


def conforms_to_schema(record: Mapping[str, Any], schema: Mapping[str, str]) -> bool:
    """
    Return true iff the record conforms to the supplied schema.

    The record conforms to the supplied schema iff:
    - All columns in the record are in the schema.
    - For every column in the record, that column's type is equal to or narrower than the same column's
      type in the schema.
    """
    ...


def type_mapping_to_jsonschema(type_mapping: Mapping[str, Any]) -> Mapping[str, str]:
    """
    Return the user input schema (type mapping), transformed to JSON Schema format.
    """
    return {k: {"type": v} for k, v in type_mapping.items()}
