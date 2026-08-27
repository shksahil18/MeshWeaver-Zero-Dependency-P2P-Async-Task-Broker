from meshweaver.tasks.serializer import (
    serialize_task,
    deserialize_task,
    execute_task,
)


def multiply(a, b):
    return a * b


def test_task_serialization():

    data = serialize_task(
        multiply,
        args=(5, 4),
    )

    assert isinstance(data, bytes)

    task = deserialize_task(data)

    assert callable(task["function"])
    assert task["args"] == (5, 4)


def test_task_execution():

    data = serialize_task(
        multiply,
        args=(5, 4),
    )

    result = execute_task(data)

    assert result == 20