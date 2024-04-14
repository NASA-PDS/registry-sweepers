from __future__ import annotations

from dataclasses import dataclass
from typing import Dict
from typing import Union


@dataclass
class Update:
    """Class representing an ES/OpenSearch database update to a single document"""

    id: str
    content: Dict

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
