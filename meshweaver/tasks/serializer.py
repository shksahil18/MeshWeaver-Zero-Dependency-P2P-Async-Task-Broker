import cloudpickle


def serialize_task(function, args=(), kwargs=None) -> bytes:
    """
    Serialize a Python function and its arguments.
    """

    if kwargs is None:
        kwargs = {}

    task = {
        "function": function,
        "args": args,
        "kwargs": kwargs,
    }

    return cloudpickle.dumps(task)


def deserialize_task(data: bytes) -> dict:
    """
    Deserialize a task received from another node.
    """

    return cloudpickle.loads(data)


def execute_task(data: bytes):
    """
    Deserialize and execute a serialized task.
    """

    task = deserialize_task(data)

    function = task["function"]
    args = task["args"]
    kwargs = task["kwargs"]

    return function(*args, **kwargs)