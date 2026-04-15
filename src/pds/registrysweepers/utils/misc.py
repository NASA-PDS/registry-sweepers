import itertools
import logging
import os
import random
from collections import defaultdict
from collections.abc import Sequence
from datetime import datetime
from typing import Any
from typing import Callable
from typing import Collection
from typing import Dict
from typing import Iterable
from typing import List
from typing import TypeVar
from typing import Union

from pds.registrysweepers.utils.productidentifiers.pdsproductidentifier import PdsProductIdentifier

log = logging.getLogger(__name__)


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


def coerce_list_type(db_value: Union[T, List[T]]) -> List[T]:
    """
    Coerce a non-array-typed legacy db record into a list containing itself as the only element, or return the
    original argument if it is already an array (list).  This is sometimes necessary to support legacy db records which
    did not wrap singleton properties in an enclosing array.
    """

    if isinstance(db_value, list):
        return db_value
    else:
        return [
            db_value,
        ]


def coerce_non_list_type(db_value: Union[T, List[T]], support_null: bool = False) -> Union[T, None]:
    """
    Given a value, return it if it is a non-list, else ensure that it only contains a single element and return that
    element.  This is sometimes necessary to gracefully handle data whose type is expected to be non-arraylike but which
    may be unexpectedly wrapped as a singleton array.  If support_null is True, empty arrays will be resolved to None
    """

    if isinstance(db_value, list):
        if support_null and len(db_value) == 0:
            return None

        if len(db_value) != 1:
            raise ValueError(f"Cannot coerce db_value as it is not a single-element array: {db_value}")

        return db_value[0]
    else:
        return db_value


def get_human_readable_elapsed_since(begin: datetime) -> str:
    elapsed_seconds = (datetime.now() - begin).total_seconds()
    h = int(elapsed_seconds / 3600)
    m = int(elapsed_seconds % 3600 / 60)
    s = int(elapsed_seconds % 60)
    return (f"{h}h" if h else "") + (f"{m}m" if m else "") + f"{s}s"


def get_random_hex_id(id_len: int = 6) -> str:
    val = random.randint(0, 16**id_len)
    return hex(val)[2:]


def auto_raise_for_status(f: Callable) -> Callable:
    """
    Given a function requests.{verb}, return a version of it which will automatically raise_for_status().  This is
    used with retry.retry_call() in cirumstances where it is not desirable to extract the retryable block to its own
    function, for example in a loop which shares a lot of state with its enclosing scope.
    """

    def wrapped_f(*args, **kwargs):
        resp = f(*args, **kwargs)
        resp.raise_for_status()
        return resp

    return wrapped_f


def get_sweeper_version_metadata_key(sweeper_name: str) -> str:
    return f"ops:Provenance/ops:registry_sweepers_{sweeper_name}_version"


def iterate_pages_of_size(page_size: int, iterable: Iterable[T]) -> Iterable[List[T]]:
    """Provides a simple interface for lazily iterating over pages of an arbitrary iterable"""
    # TODO: Evaluate whether this is equivalent to more_itertools.batched()
    # TODO: Evaluate whether this is equivalent to utils.misc.chunked()

    if page_size < 1:
        raise ValueError(f"Cannot iterate over pages of size <1 (got {page_size})")

    return iterate_pages_given(lambda page: len(page) < page_size, iterable)


def iterate_pages_given(build_page_while: Callable[[List[T]], bool], iterable: Iterable[T]) -> Iterable[List[T]]:
    """
    Given a condition f(active_page: List[T]) under which to continue accumulating elements into the active page, yield
    pages of elements from the input iterable.  This exists to support behaviour like "continue building page while
    sufficient memory is available".
    """
    # TODO: Evaluate whether this is equivalent to itertools.takewhile()

    page: List[T] = []
    for el in iterable:
        if not build_page_while(page):
            yield page
            page = []

        page.append(el)

    if len(page) > 0:
        yield page


def parse_boolean_env_var(key: str) -> bool:
    raw_value = os.environ.get(key)

    valid_truthy_values = ["true", "True", "TRUE", "1"]
    valid_falsy_values = ["false", "False", "FALSE", "0", ""]
    if raw_value in valid_falsy_values or raw_value is None:
        return False
    elif raw_value in valid_truthy_values:
        return True
    else:
        raise ValueError(
            f'Could not parse valid boolean from env var "{key}" - expected {valid_truthy_values} for True or {valid_falsy_values} for False'
        )


def bin_elements(elements: Iterable[V], key_f: Callable[[V], K]) -> Dict[K, List[V]]:
    """
    Given a collection of elements and a function to derive the hashable bin key of an element, return a dict of
    elements binned by key.

    @param elements: an iterable collection of elements
    @param key: a key function mapping an element onto its bin value
    @return: a dict of bin keys onto the elements belonging to that bin
    """

    # TODO: Evaluate whether this is equivalent to itertools.groupby()

    result: Dict[K, List[V]] = {}
    for e in elements:
        k = key_f(e)
        if k not in result:
            result[k] = []

        result[k].append(e)

    return result


def is_dev_mode():
    return str(os.environ.get("DEV_MODE")).lower() not in {"none", "", "0", "false"}


def chunked(iterable, n):
    """Lazily yield successive n-sized chunks from an iterable."""
    # TODO: Evaluate whether this is equivalent to more_itertools.batched()
    iterator = iter(iterable)
    while True:
        chunk = list(itertools.islice(iterator, n))
        if not chunk:
            break
        yield chunk


def group_by_key(iterable: Iterable[V], key_func: Callable[[V], K]) -> Dict[K, List[V]]:
    """Given an iterable, bucket the elements by some key function"""
    result = defaultdict(list)
    for item in iterable:
        result[key_func(item)].append(item)
    return dict(result)


def get_ids_list_str(
    ids: Sequence[Union[PdsProductIdentifier, str]],
    default_id_display_limit: int = 5,
    debug_id_display_limit: Union[int, None] = None,
) -> str:
    ids_count = len(ids)

    if log.isEnabledFor(logging.DEBUG):
        display_ids = ids[:debug_id_display_limit]
    else:
        display_ids = ids[:default_id_display_limit]

    display_ids_count = len(display_ids)
    display_id_str = str([str(id) for id in display_ids])

    if ids_count <= display_ids_count:
        return display_id_str
    else:
        return f"{display_id_str} <list of {ids_count} ids truncated - enable DEBUG logging or increase display limit in code to see more>"


def limit_log_length(log_msg: str, max_str_length: int = 5012) -> str:
    log_msg = str(log_msg)

    if len(log_msg) > max_str_length:
        return log_msg[:max_str_length] + " ... <TRUNCATED>"
    else:
        return log_msg
