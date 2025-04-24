# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Serialized DAG and BaseOperator."""

from __future__ import annotations

from typing import Any

from airflow.configuration import conf
from airflow.sdk.execution_time.secrets_masker import redact
from airflow.settings import json


def serialize_template_field(template_field: Any, name: str) -> str | dict | list | int | float:
    """
    Converts a templated field into a JSON-serializable form for Airflow serialization.
    
    If the field is not directly JSON serializable, attempts to use a `serialize()` method or falls back to its string representation, applying truncation and redaction for long strings. Tuples are recursively converted to lists for compatibility. Returns a serializable structure or a truncated/redacted string suitable for storage or transmission.
    
    Args:
        template_field: The value to serialize, which may be of any type.
        name: The field name, used for redaction purposes.
    
    Returns:
        A JSON-serializable representation of the field, or a truncated/redacted string if necessary.
    """

    def is_jsonable(x: Any) -> bool:
        """
        Checks whether the given object can be serialized to JSON.
        
        Args:
            x: The object to test for JSON serializability.
        
        Returns:
            True if the object can be serialized to JSON, False otherwise.
        """
        try:
            json.dumps(x)
            return True
        except (TypeError, OverflowError):
            return False
        return True

    def translate_tuples_to_lists(obj: Any):
        """
        Recursively converts all tuples within a nested structure to lists.
        
        This function traverses lists and dictionaries, ensuring that any tuple found at any depth
        is replaced by a list, making the structure fully JSON-serializable.
        """
        if isinstance(obj, tuple):
            return [translate_tuples_to_lists(item) for item in obj]
        if isinstance(obj, list):
            return [translate_tuples_to_lists(item) for item in obj]
        if isinstance(obj, dict):
            return {key: translate_tuples_to_lists(value) for key, value in obj.items()}
        return obj

    # Constant for truncation message offset
    TRUNCATION_OFFSET = 79
    max_length = conf.getint("core", "max_templated_field_length")

    def truncate_if_needed(value: str) -> str:
        """
        Truncates a string if it exceeds the configured maximum length.
        
        If the string is too long, it is redacted and truncated, and a message is prepended indicating truncation.
        """
        if len(value) > max_length:
            rendered = redact(value, name)
            return (
                "Truncated. You can change this behaviour in [core]max_templated_field_length. "
                f"{rendered[: max_length - TRUNCATION_OFFSET]!r}... "
            )
        return value

    if not is_jsonable(template_field):
        try:
            serialized = template_field.serialize()
        except AttributeError:
            serialized = str(template_field)
        return truncate_if_needed(serialized) if isinstance(serialized, str) else serialized
    if not template_field and not isinstance(template_field, tuple):
        # Avoid unnecessary serialization steps for empty fields unless they are tuples
        # and need to be converted to lists
        return template_field
    template_field = translate_tuples_to_lists(template_field)
    serialized = str(template_field)
    return truncate_if_needed(serialized) if isinstance(serialized, str) else template_field
