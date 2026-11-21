"""
A guard which makes an OpenSearch client read-only, used by dry runs.

Only the calls named in the allow-lists below are passed through to the wrapped client.  Anything else (bulk, index,
update, delete, indices.put_mapping, indices.create, ...) raises ReadOnlyClientViolation before a request is made, so a
dry run which reaches a write path fails loudly instead of changing the registry.
"""
from typing import Any
from typing import FrozenSet

# calls which only read
READ_METHODS: FrozenSet[str] = frozenset({"search", "scroll", "clear_scroll", "count", "mget", "get", "exists", "info", "ping"})
INDICES_READ_METHODS: FrozenSet[str] = frozenset({"exists", "exists_alias", "get", "get_mapping", "get_alias"})


class ReadOnlyClientViolation(RuntimeError):
    """Raised when code running against a ReadOnlyOpenSearchClient attempts a call which is not known to be read-only"""


class _ReadOnlyNamespace:
    def __init__(self, target: Any, allowed: FrozenSet[str], label: str):
        self._target = target
        self._allowed = allowed
        self._label = label

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            # private/dunder lookups (copy, pickle, hasattr probes) are not requests
            raise AttributeError(name)

        if name not in self._allowed:
            raise ReadOnlyClientViolation(
                f"{self._label}.{name} is not permitted on a read-only client (dry run) - it may write to the registry"
            )
        return getattr(self._target, name)


class ReadOnlyOpenSearchClient(_ReadOnlyNamespace):
    """Wraps an OpenSearch client, passing through only the read calls in READ_METHODS / INDICES_READ_METHODS"""

    def __init__(self, client: Any):
        super().__init__(client, READ_METHODS, "client")
        self.indices = _ReadOnlyNamespace(client.indices, INDICES_READ_METHODS, "client.indices")
