from typing import Any
import importlib
import json


class ConfigurableObject:
    @staticmethod
    def load_dict(config: dict[str, Any]):
        try:
            *module_names, class_name = config.pop("__class__").split(".")
        except KeyError:
            raise ValueError("Configuration must contain '__class__'")

        module = importlib.import_module(".".join(module_names))
        cls = getattr(module, class_name)
        return cls(**config)

    @classmethod
    def load_json(cls, config: str):
        return cls.load_dict(json.loads(config))

    @staticmethod
    def get_class_path(obj) -> str:
        t = type(obj)
        return f"{t.__module__}.{t.__name__}"

    def as_dict(self) -> dict[str, Any]:
        class_path = {"__class__": self.get_class_path(self)}
        config = {k: v for k, v in vars(self).items() if not k.startswith("_")}
        return class_path | config

    def as_json(self, indent: int | str | None = None) -> str:
        return json.dumps(self.as_dict(), indent=indent, default=str)
