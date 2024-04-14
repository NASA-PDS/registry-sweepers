from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Union

SerializableAncestryRecordTypeDef = Dict[str, Union[str, List[str]]]
DbMockTypeDef = Optional[Callable[[str], Iterable[Dict]]]
