from __future__ import annotations

from dataclasses import dataclass
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Union


@dataclass
class Update:
    """Class representing an ES/OpenSearch database update to a single document"""

    id: str
    content: Optional[Dict] = None
    inline_script_content: Optional[str] = None
    inline_script_new_items: Optional[Iterable] = None

    # used when it is necessary to instantiate these updates for flow-control purposes
    # for example, the provenance sweeper needs to trigger bulk write buffer flushes as it iterates, even if only a
    # small fraction of records has to be updated
    skip_write: bool = False

    # These are used for version conflict detection in ES/OpenSearch
    # see: https://www.elastic.co/guide/en/elasticsearch/reference/7.17/optimistic-concurrency-control.html
    primary_term: Union[int, None] = None
    seq_no: Union[int, None] = None

    def has_versioning_information(self) -> bool:
        has_primary_term = self.primary_term is not None
        has_sequence_number = self.seq_no is not None
        has_either = any((has_primary_term, has_sequence_number))
        has_both = all((has_primary_term, has_sequence_number))
        if has_either and not has_both:
            raise ValueError("if either of primary_term, seq_no is provided, both must be provided")

        return has_both

    def __post_init__(self):
        if not (self.content or self.inline_script_content):
            raise ValueError("either content or inline_script_content must be provided")

        if self.content and self.inline_script_content:
            raise ValueError("only one of content or inline_script_content may be provided in a single update")

        if self.inline_script_content and self.inline_script_new_items is None:
            raise ValueError(
                "inline_script_content is provided, inline_script_new_items must be provided (but may be empty)")
