"""Module of misscelanius utilities"""

import random
import string


def id_generator(
    size: int = 6,
    chars: str = (
        string.ascii_uppercase + string.digits + string.ascii_lowercase
    )
) -> str:
    """Random ID generator. By default the IDs are 6 characters long.
    """
    return ''.join(random.choice(chars) for _ in range(size))
