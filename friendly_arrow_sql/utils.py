import importlib.util
import sys

from importlib.machinery import ModuleSpec


class ModuleNotInstalledError(ModuleNotFoundError):
    pass


def validate_package_installed(package_name: str) -> ModuleSpec:
    spec = importlib.util.find_spec(package_name)
    if spec is None:
        raise ModuleNotInstalledError(f"Package {package_name} not installed.")

    return spec


def package_load_validate(package_name: str, alias: str | None = None) -> None:
    spec = validate_package_installed(package_name)

    module = importlib.util.module_from_spec(spec)
    import_as_name = package_name if alias is None else alias
    sys.modules[import_as_name] = module
    spec.loader.exec_module(module)
