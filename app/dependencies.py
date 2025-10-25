"""
Dependency Injection Configuration

Provides a simple dependency injection system for managing service dependencies.
This decouples services from their implementations, making code more testable
and flexible for different configurations (e.g., test vs production).
"""
import logging
from typing import Callable, Any, TypeVar, Generic


logger = logging.getLogger(__name__)

T = TypeVar('T')


class ServiceContainer(Generic[T]):
    """
    Generic service container for managing service instances.

    Supports both singleton patterns (same instance reused) and factory patterns
    (new instance created each time).
    """

    def __init__(self):
        """Initialize the service container."""
        self._singletons: dict[type, Any] = {}
        self._factories: dict[type, Callable[[], Any]] = {}

    def register_singleton(self, service_type: type[T], instance: T) -> None:
        """
        Register a singleton service instance.

        Args:
            service_type: The service interface/type
            instance: The concrete instance to use
        """
        self._singletons[service_type] = instance
        logger.debug(f"Registered singleton: {service_type.__name__}")

    def register_factory(self, service_type: type[T], factory: Callable[[], T]) -> None:
        """
        Register a factory function for creating service instances.

        Args:
            service_type: The service interface/type
            factory: A callable that creates new instances
        """
        self._factories[service_type] = factory
        logger.debug(f"Registered factory: {service_type.__name__}")

    def get(self, service_type: type[T]) -> T:
        """
        Get a service instance.

        Returns singleton if registered, otherwise creates new instance via factory.

        Args:
            service_type: The service type to retrieve

        Returns:
            The service instance

        Raises:
            KeyError: If service type is not registered
        """
        # Check singleton first
        if service_type in self._singletons:
            return self._singletons[service_type]

        # Check factory
        if service_type in self._factories:
            return self._factories[service_type]()

        raise KeyError(f"Service {service_type.__name__} not registered in container")


class ServiceLocator:
    """
    Simple service locator for managing application services.

    Provides a centralized place to access configured services throughout the application.
    This reduces coupling between components and makes testing easier.
    """

    def __init__(self):
        """Initialize the service locator."""
        self._container = ServiceContainer()

    def register_singleton(self, service_type: type[T], instance: T) -> None:
        """Register a singleton service."""
        self._container.register_singleton(service_type, instance)

    def register_factory(self, service_type: type[T], factory: Callable[[], T]) -> None:
        """Register a factory for creating service instances."""
        self._container.register_factory(service_type, factory)

    def get(self, service_type: type[T]) -> T:
        """Get a service instance."""
        return self._container.get(service_type)

    def reset(self) -> None:
        """Reset all registered services (mainly for testing)."""
        self._container = ServiceContainer()
        logger.debug("Service locator reset")


# Global service locator instance
_service_locator: ServiceLocator | None = None


def get_service_locator() -> ServiceLocator:
    """
    Get the global service locator instance.

    Returns:
        The global ServiceLocator
    """
    global _service_locator
    if _service_locator is None:
        _service_locator = ServiceLocator()
    return _service_locator


def reset_service_locator() -> None:
    """
    Reset the service locator (mainly for testing).

    WARNING: Only use this in test environments!
    """
    global _service_locator
    _service_locator = None
