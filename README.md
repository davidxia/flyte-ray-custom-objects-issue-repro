This repo provides a minimal repro of what seems like an issue in using custom Python objects with
Flyte's Ray integration.

## Repro steps

1. Run `pip install -r requirements.txt` in a fresh Python env.
1. `python custom_objects_with_ray_task.py` fails with `TypeError: __init__() missing 3 required
   positional arguments: 'x', 'y', and 'z'`. Example full stracktrace below.
1. `python custom_objects_with_ray_actor.py` fails with `AttributeError: 'ActorClass(Datum)' object
   has no attribute 'x'` Example full stracktrace below.

Notice that `python custom_objects_example.py` that's copied straight from [docs here][1] works.

## Example full stacktrace of `custom_objects_with_ray_task.py`

```
python custom_objects_with_ray_task.py

2022-09-17 16:46:10,935 INFO worker.py:1509 -- Started a local Ray instance. View the dashboard at http://127.0.0.1:8265 
Traceback (most recent call last):
  File "/Users/dxia/.pyenv/versions/3.8.12/envs/hray/lib/python3.8/site-packages/flytekit/core/promise.py", line 472, in create_native_named_tuple
    return TypeEngine.to_python_value(ctx, promises.val, v)
  File "/Users/dxia/.pyenv/versions/3.8.12/envs/hray/lib/python3.8/site-packages/flytekit/core/type_engine.py", line 769, in to_python_value
    return transformer.to_python_value(ctx, lv, expected_python_type)
  File "/Users/dxia/.pyenv/versions/3.8.12/envs/hray/lib/python3.8/site-packages/flytekit/core/type_engine.py", line 543, in to_python_value
    dc = cast(DataClassJsonMixin, expected_python_type).from_json(_json_format.MessageToJson(lv.scalar.generic))
  File "/Users/dxia/.pyenv/versions/3.8.12/envs/hray/lib/python3.8/site-packages/dataclasses_json/api.py", line 65, in from_json
    return cls.from_dict(kvs, infer_missing=infer_missing)
  File "/Users/dxia/.pyenv/versions/3.8.12/envs/hray/lib/python3.8/site-packages/dataclasses_json/api.py", line 72, in from_dict
    return _decode_dataclass(cls, kvs, infer_missing)
  File "/Users/dxia/.pyenv/versions/3.8.12/envs/hray/lib/python3.8/site-packages/dataclasses_json/core.py", line 208, in _decode_dataclass
    return cls(**init_kwargs)
TypeError: __init__() missing 3 required positional arguments: 'x', 'y', and 'z'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/private/tmp/flyte-ray-custom-objects-issue-repro/custom_objects_with_ray_task.py", line 63, in <module>
    print(wf(x=10, y=20))
  File "/Users/dxia/.pyenv/versions/3.8.12/envs/hray/lib/python3.8/site-packages/flytekit/core/workflow.py", line 238, in __call__
    return flyte_entity_call_handler(self, *args, **input_kwargs)
  File "/Users/dxia/.pyenv/versions/3.8.12/envs/hray/lib/python3.8/site-packages/flytekit/core/promise.py", line 1046, in flyte_entity_call_handler
    return create_native_named_tuple(ctx, result, cast(SupportsNodeCreation, entity).python_interface)
  File "/Users/dxia/.pyenv/versions/3.8.12/envs/hray/lib/python3.8/site-packages/flytekit/core/promise.py", line 474, in create_native_named_tuple
    raise AssertionError(f"Failed to convert value of output {k}, expected type {v}.") from e
AssertionError: Failed to convert value of output o0, expected type <class '__main__.Datum'>.
```

## Example full stacktrace of `custom_objects_with_ray_actor.py`

```
python custom_objects_with_ray_actor.py

Traceback (most recent call last):
  File "/private/tmp/flyte-ray-custom-objects-issue-repro/custom_objects_with_ray_actor.py", line 34, in <module>
    def stringify(x: int) -> Datum:
  File "/Users/dxia/.pyenv/versions/3.8.12/envs/hray/lib/python3.8/site-packages/flytekit/core/task.py", line 212, in task
    return wrapper(_task_function)
  File "/Users/dxia/.pyenv/versions/3.8.12/envs/hray/lib/python3.8/site-packages/flytekit/core/task.py", line 195, in wrapper
    task_instance = TaskPlugins.find_pythontask_plugin(type(task_config))(
  File "/Users/dxia/.pyenv/versions/3.8.12/envs/hray/lib/python3.8/site-packages/flytekit/core/tracker.py", line 35, in __call__
    o = super(InstanceTrackingMeta, cls).__call__(*args, **kwargs)
  File "/Users/dxia/.pyenv/versions/3.8.12/envs/hray/lib/python3.8/site-packages/flytekit/core/python_function_task.py", line 118, in __init__
    self._native_interface = transform_function_to_interface(task_function, Docstring(callable_=task_function))
  File "/Users/dxia/.pyenv/versions/3.8.12/envs/hray/lib/python3.8/site-packages/flytekit/core/interface.py", line 295, in transform_function_to_interface
    outputs = extract_return_annotation(return_annotation)
  File "/Users/dxia/.pyenv/versions/3.8.12/envs/hray/lib/python3.8/site-packages/flytekit/core/interface.py", line 422, in extract_return_annotation
    logger.debug(f"Task returns unnamed native tuple {return_annotation}")
  File "/Users/dxia/.pyenv/versions/3.8.12/envs/hray/lib/python3.8/site-packages/ray/util/tracing/tracing_helper.py", line 466, in _resume_span
    return method(self, *_args, **_kwargs)
  File "/Users/dxia/.pyenv/versions/3.8.12/lib/python3.8/dataclasses.py", line 368, in wrapper
    result = user_function(self)
  File "<string>", line 3, in __repr__
AttributeError: 'ActorClass(Datum)' object has no attribute 'x'
```


[1]: https://docs.flyte.org/projects/cookbook/en/stable/auto/core/type_system/custom_objects.html
