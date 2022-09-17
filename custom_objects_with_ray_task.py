import typing
from dataclasses import dataclass

import ray
from dataclasses_json import dataclass_json
from flytekit import Resources, task, workflow
from flytekitplugins.ray import HeadNodeConfig, RayJobConfig, WorkerNodeConfig


ray_config = RayJobConfig(
    head_node_config=HeadNodeConfig(ray_start_params={"log-color": "True"}),
    worker_node_config=[WorkerNodeConfig(group_name="ray-group", replicas=2)],
)


@dataclass_json
@dataclass
class Datum(object):
    """
    Example of a simple custom class that is modeled as a dataclass
    """

    x: int
    y: str
    z: typing.Dict[int, str]


@ray.remote
def create_datum(x: int, y: str, z: typing.Dict[int, str]) -> Datum:
    return Datum(x=x, y=y, z=z)


@task
def stringify(x: int) -> Datum:
    """
    A dataclass return will be regarded as a complex single json return.
    """
    return Datum(x=x, y=str(x), z={x: str(x)})


@task(task_config=ray_config, limits=Resources(mem="2000Mi", cpu="1"))
def add(x: Datum, y: Datum) -> Datum:
    """
    Flytekit will automatically convert the passed in json into a DataClass. If the structures dont match, it will raise
    a runtime failure
    """
    x.z.update(y.z)
    return ray.get(create_datum.remote(x.x + y.x, x.y + y.y, x.z))


@workflow
def wf(x: int, y: int) -> Datum:
    """
    Dataclasses (JSON) can be returned from a workflow as well.
    """
    return add(x=stringify(x=x), y=stringify(x=y))


if __name__ == "__main__":
    """
    This workflow can be run locally. During local execution also, the dataclasses will be marshalled to and from json.
    """
    print(wf(x=10, y=20))
