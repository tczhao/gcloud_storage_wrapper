from .Storage import Storage
from functools import partial

ls = partial(Storage, action="ls", recursive=False)
lsr = partial(Storage, action="ls", recursive=True)
cp = partial(Storage, action="cp", recursive=False)
cpr = partial(Storage, action="cp", recursive=True)
