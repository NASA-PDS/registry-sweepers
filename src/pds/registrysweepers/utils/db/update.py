from __future__ import annotations

from dataclasses import dataclass
from typing import Dict
from typing import Union


@dataclass
class Update:
    """Class representing an ES/OpenSearch database update to a single document"""

    id: str
    content: Dict

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
