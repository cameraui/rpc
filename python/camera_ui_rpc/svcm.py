"""NATS micro service implementation similar to TypeScript Svcm."""

from nats.aio.client import Client as NATSClient
from nats.micro import add_service  # pyright: ignore[reportUnknownVariableType]
from nats.micro.service import Service, ServiceConfig


class Svcm:
    """Service manager (matches TypeScript API)."""

    def __init__(self, nc: NATSClient):
        self.nc: NATSClient = nc

    async def add(self, config: ServiceConfig) -> Service:
        return await add_service(self.nc, config)
