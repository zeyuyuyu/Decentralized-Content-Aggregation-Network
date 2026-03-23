import asyncio
from typing import Dict, List, Optional
import aiohttp
import logging

class ServiceOrchestrator:
    def __init__(self):
        self.services: Dict[str, Dict] = {}
        self.health_check_interval = 30  # seconds
        self.logger = logging.getLogger(__name__)

    async def register_service(self, service_id: str, endpoint: str, metadata: dict) -> None:
        """Register a new service with the orchestrator"""
        self.services[service_id] = {
            'endpoint': endpoint,
            'metadata': metadata,
            'status': 'unknown',
            'last_check': None
        }
        self.logger.info(f'Registered new service: {service_id} at {endpoint}')

    async def deregister_service(self, service_id: str) -> None:
        """Remove a service from the orchestrator"""
        if service_id in self.services:
            del self.services[service_id]
            self.logger.info(f'Deregistered service: {service_id}')

    async def check_service_health(self, service_id: str) -> bool:
        """Check health status of a specific service"""
        if service_id not in self.services:
            return False

        service = self.services[service_id]
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{service['endpoint']}/health") as response:
                    healthy = response.status == 200
                    service['status'] = 'healthy' if healthy else 'unhealthy'
                    service['last_check'] = asyncio.get_event_loop().time()
                    return healthy
        except Exception as e:
            self.logger.error(f'Health check failed for {service_id}: {str(e)}')
            service['status'] = 'unhealthy'
            service['last_check'] = asyncio.get_event_loop().time()
            return False

    async def monitor_services(self) -> None:
        """Continuously monitor health of all registered services"""
        while True:
            for service_id in list(self.services.keys()):
                await self.check_service_health(service_id)
            await asyncio.sleep(self.health_check_interval)

    def get_healthy_services(self, service_type: Optional[str] = None) -> List[str]:
        """Get list of healthy services, optionally filtered by type"""
        healthy_services = []
        for service_id, service in self.services.items():
            if service['status'] == 'healthy':
                if service_type is None or service['metadata'].get('type') == service_type:
                    healthy_services.append(service_id)
        return healthy_services

    async def start(self) -> None:
        """Start the orchestrator"""
        self.logger.info('Starting service orchestrator')
        await self.monitor_services()

    async def stop(self) -> None:
        """Stop the orchestrator"""
        self.logger.info('Stopping service orchestrator')
        self.services.clear()
