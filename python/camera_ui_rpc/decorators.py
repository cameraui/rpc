from collections.abc import Callable
from typing import Any, TypeVar, cast

F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T", bound=type)


def RPCMethod(func: F) -> F:
    """Mark a method for RPC exposure."""
    func.__rpc_exposed__ = True  # type: ignore
    return func


def RPCNested(func: Any) -> Any:
    """Mark a property containing nested RPC objects."""
    # This works differently in Python - we mark the property descriptor
    if isinstance(func, property):
        func.__rpc_nested__ = True  # type: ignore
    else:
        # For regular attributes, we need to mark them differently
        func.__rpc_nested__ = True  # type: ignore
    return func


def RPCClass(c: T) -> T:
    """Mark entire class for RPC exposure (all public methods)."""
    c.__rpc_expose_all__ = True  # type: ignore
    return c


class RPCPropertyDescriptor:
    """Property descriptor that marks a property for RPC exposure."""

    def __init__(self, name: str | None = None):
        self.name: str | None = name
        self.private_name: str | None = None

    def __set_name__(self, owner: type, name: str) -> None:
        self.name = name
        self.private_name = f"_{name}"

        # Mark the property itself as RPC method
        if not hasattr(owner, "__rpc_methods__"):
            owner.__rpc_methods__ = []  # type: ignore
        owner.__rpc_methods__.append(name)  # type: ignore

        # Create setter method name
        setter_name = f"set{name[0].upper()}{name[1:]}"

        # Create the setter method
        # We need to capture private_name in the closure
        private_name = self.private_name

        def setter(instance: Any, value: Any) -> None:
            setattr(instance, private_name, value)

        # Add setter to class
        setattr(owner, setter_name, setter)

        # Mark setter as RPC method
        owner.__rpc_methods__.append(setter_name)  # type: ignore

    def __get__(self, obj: Any, objtype: type | None = None) -> Any:
        if obj is None:
            return self
        assert self.private_name is not None, "Property not initialized via __set_name__"
        return getattr(obj, self.private_name, None)

    def __set__(self, obj: Any, value: Any) -> None:
        assert self.private_name is not None, "Property not initialized via __set_name__"
        setattr(obj, self.private_name, value)


# Convenience function that returns a property descriptor
RPCProperty = RPCPropertyDescriptor


def extract_nested_methods_without_decorators(
    obj: Any,
    path: list[str] | None = None,
    handlers: dict[str, Callable[..., Any]] | None = None,
    visited: set[int] | None = None,
) -> dict[str, Callable[..., Any]]:
    if path is None:
        path = []
    if handlers is None:
        handlers = {}
    if visited is None:
        visited = set()

    if obj is None:
        return handlers

    # Prevent circular references
    obj_id = id(obj)
    if obj_id in visited:
        return handlers
    visited.add(obj_id)

    # Handle dict objects
    if isinstance(obj, dict):
        for key, value in cast(dict[Any, Any], obj).items():
            if isinstance(key, str) and key.startswith("_"):
                continue

            full_path = ".".join(path + [key])

            if callable(value) and not isinstance(value, type):
                handlers[full_path] = value
            # Don't recurse into nested objects for without_decorators mode
            # Only direct methods should be exposed

        return handlers

    # Handle class instances
    for attr_name in dir(obj):
        # Skip private, dunder, and internal attributes
        if attr_name.startswith("_"):
            continue

        try:
            # Check if it's a static method or class method on the class
            # We need to check __dict__ directly because getattr resolves the descriptor
            for cls in obj.__class__.__mro__:
                if attr_name in cls.__dict__:
                    class_attr = cls.__dict__[attr_name]
                    if isinstance(class_attr, staticmethod | classmethod):
                        break
            else:
                class_attr = None

            if isinstance(class_attr, staticmethod | classmethod):
                continue

            # Get the actual value
            value = getattr(obj, attr_name)

            # Skip types/classes
            if isinstance(value, type):
                continue

            full_path = ".".join(path + [attr_name])

            if callable(value):
                # It's a method - expose it
                handlers[full_path] = value
            # Don't recurse into nested objects for without_decorators mode
            # Only direct methods should be exposed, nested objects require explicit @RPCNested

        except Exception:
            # Ignore errors in attribute access
            pass

    return handlers


def extract_nested_methods_with_decorators(
    obj: Any,
    path: list[str] | None = None,
    handlers: dict[str, Callable[..., Any]] | None = None,
    visited: set[int] | None = None,
) -> dict[str, Callable[..., Any]]:
    if path is None:
        path = []
    if handlers is None:
        handlers = {}
    if visited is None:
        visited = set()

    # Prevent circular references
    obj_id = id(obj)
    if obj_id in visited:
        return handlers
    visited.add(obj_id)

    # Check if it's a plain dict
    is_plain_dict = type(obj) is dict

    # Check for decorators
    has_decorators = (
        hasattr(obj.__class__, "__rpc_expose_all__")
        or any(
            hasattr(getattr(obj.__class__, attr, None), "__rpc_exposed__")
            for attr in dir(obj.__class__)
            if not attr.startswith("_")
        )
        or any(
            hasattr(getattr(obj.__class__, attr, None), "__rpc_nested__")
            for attr in dir(obj.__class__)
            if not attr.startswith("_")
        )
    )

    if is_plain_dict and not has_decorators:
        # Plain dict mode
        obj = cast(dict[str, Any], obj)
        for key, value in obj.items():
            if key.startswith("_"):
                continue

            full_path = ".".join(path + [key])

            if callable(value):
                handlers[full_path] = value
            elif (
                isinstance(value, dict)
                or hasattr(value, "__dict__")
                and not isinstance(value, str | int | float | bool | list | tuple | set)
            ):
                extract_nested_methods_with_decorators(value, path + [key], handlers, visited)
            else:
                # For non-callable values (like strings, numbers), create a getter function
                async def value_getter(val: Any = value) -> Any:
                    return val

                handlers[full_path] = value_getter
    else:
        # Decorator mode
        obj = cast(object, obj)
        expose_all = getattr(obj.__class__, "__rpc_expose_all__", False)

        # Collect decorated methods and nested properties
        rpc_methods: list[Any] = []
        rpc_nested: list[Any] = []

        # Check all attributes in the class
        for attr_name in dir(obj.__class__):
            if attr_name.startswith("_"):
                continue

            try:
                attr = getattr(obj.__class__, attr_name)
                if hasattr(attr, "__rpc_exposed__"):
                    rpc_methods.append(attr_name)
                if hasattr(attr, "__rpc_nested__"):
                    rpc_nested.append(attr_name)
                # Check if it's a property that has been decorated
                if isinstance(attr, property) and hasattr(attr.fget, "__rpc_nested__"):
                    rpc_nested.append(attr_name)
            except Exception:
                pass

        # Also check for methods marked via __rpc_methods__ (from RPCProperty)
        if hasattr(obj.__class__, "__rpc_methods__"):
            for method_name in obj.__class__.__rpc_methods__:
                if method_name not in rpc_methods:
                    rpc_methods.append(method_name)

        # Also check instance __dict__ for dynamically added nested objects
        if hasattr(obj, "__dict__"):
            for attr_name, _ in obj.__dict__.items():
                if not attr_name.startswith("_"):
                    # Check if this instance attribute corresponds to a decorated property
                    class_attr = getattr(obj.__class__, attr_name, None)
                    if class_attr and hasattr(class_attr, "__rpc_nested__") and attr_name not in rpc_nested:
                        rpc_nested.append(attr_name)

        # Check for instance-level nested attributes marking
        if hasattr(obj, "__rpc_nested_attrs__"):
            for attr_name in obj.__rpc_nested_attrs__:
                if attr_name not in rpc_nested:
                    rpc_nested.append(attr_name)

        # Process all attributes
        for attr_name in dir(obj):
            if attr_name.startswith("_") or attr_name == "constructor":
                continue

            try:
                value = getattr(obj, attr_name)
                full_path = ".".join(path + [attr_name])

                if callable(value) and not isinstance(value, type):
                    # It's a method
                    if expose_all or attr_name in rpc_methods:
                        handlers[full_path] = value
                elif attr_name in rpc_methods and not callable(value):
                    # It's a property marked with @RPCProperty
                    # Create an async getter function
                    async def property_getter(obj: Any = obj, attr: Any = attr_name) -> Any:
                        return getattr(obj, attr)

                    handlers[full_path] = property_getter
                elif attr_name in rpc_nested:
                    # It's a nested object marked with @RPCNested
                    # For properties, we might need to call them to get the actual object
                    nested_obj = value

                    # Check if it's a property getter (method that returns an object)
                    if callable(value) and hasattr(obj.__class__, attr_name):
                        class_attr = getattr(obj.__class__, attr_name)
                        if isinstance(class_attr, property):
                            # It's a property, call it to get the actual object
                            try:
                                nested_obj = value()  # Call the property getter
                            except Exception:
                                # If calling fails, use the value as is
                                nested_obj = value

                    if nested_obj is not None:
                        extract_nested_methods_with_decorators(
                            nested_obj, path + [attr_name], handlers, visited
                        )
            except Exception:
                pass

    return handlers
