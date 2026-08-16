from collections.abc import Iterator
from collections import defaultdict
from typing import Dict
from typing import List
from pds.registrysweepers.utils.db import Update
from typing import Iterable


class UpdateDeferralTracker:
    """
        There are circumstances where it is necessary to catch Updates targeting documents which do not (yet) exist in
        the db, and store their content in a separate index for later retrieval.

        This class acts as a MITM buffer for such updates, and
         - stores Updates during iteration, by target document _id
         - accepts calls confirming that content for a given document _id is no longer required
         - makes remaining Updates available after initial consumption, for writing into a deferral index
         This class also accepts calls to "lock" the current contents, preventing it from being discarded during the
         following race condition:
           1. An update for a missing document is processed and held for deferral
           2. The document is indexed during processing
           3. A later, different update for that document is processed in the same stream and is marked for release.

           In this case, it is necessary to avoid releasing the earlier update as it has not been written successfully.


    """
    _source: Iterator[Update]
    _temp_store: Dict[str, List[Update]] = defaultdict(list)
    _store: Dict[str, List[Update]] = defaultdict(list)

    def __init__(self, source: Iterator[Update]):
        self._source = source

    def __iter__(self):
        return self

    def __next__(self):
        next_update = self._source.next()
        stored_updates: List[Update] = self._temp_store.setdefault(next_update.id, [])
        stored_updates.append(next_update)
        return next_update

    def discard(self, update_id: str):
        # intentionally non-safe as key should always exist
        del self._temp_store[update_id]

    def lock_in_deferrals(self):
        # merge everything from temp_store into store
        for id, updates in self._temp_store.items():
            existing_locked = self._store.setdefault(id, [])
            existing_temp = updates
            self._store[id] = existing_locked + existing_temp

        # reset temp_store
        self._temp_store = {}