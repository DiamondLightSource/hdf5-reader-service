import io
import multiprocessing as mp
import sys
from collections.abc import Callable
from typing import Any


def fork_and_do(func: Callable[..., Any], args: tuple[Any, ...]):
    queue: mp.Queue = mp.Queue()

    def do_func(other_end_of_queue: mp.Queue, *args, **kwargs):
        out_file = open(sys.stdout.fileno(), "wb", 0)
        sys.stdout = io.TextIOWrapper(out_file, write_through=True)

        retval = func(*args, **kwargs)
        other_end_of_queue.put(retval)

    p = mp.Process(target=do_func, args=(queue,) + args)
    p.start()
    p.join()

    if p.exitcode != 0:
        raise mp.ProcessError(
            f"Task failed for {func.__name__} with args {args}, see log"
        )

    return queue.get()
