import asyncio
from typing import Dict, Set, Optional
import time
import logging

class NetworkOrchestrator:
    def __init__(self):
        self.nodes: Dict[str, dict] = {}
        self.active_nodes: Set[str] = set()
        self.last_heartbeat: Dict[str, float] = {}
        self.heartbeat_interval = 30  # seconds
        self.node_timeout = 90  # seconds
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('NetworkOrchestrator')

    async def register_node(self, node_id: str, node_info: dict) -> bool:
        """Register a new node in the network"""
        if node_id in self.nodes:
            self.logger.warning(f'Node {node_id} already registered')
            return False

        self.nodes[node_id] = node_info
        self.active_nodes.add(node_id)
        self.last_heartbeat[node_id] = time.time()
        self.logger.info(f'Node {node_id} registered successfully')
        return True

    async def deregister_node(self, node_id: str) -> bool:
        """Remove a node from the network"""
        if node_id not in self.nodes:
            return False

        del self.nodes[node_id]
        self.active_nodes.discard(node_id)
        self.last_heartbeat.pop(node_id, None)
        self.logger.info(f'Node {node_id} deregistered')
        return True

    async def update_heartbeat(self, node_id: str) -> bool:
        """Update last heartbeat time for a node"""
        if node_id not in self.nodes:
            return False

        self.last_heartbeat[node_id] = time.time()
        return True

    async def check_node_health(self) -> None:
        """Monitor node health and remove inactive nodes"""
        while True:
            current_time = time.time()
            inactive_nodes = [
                node_id for node_id in self.active_nodes
                if current_time - self.last_heartbeat.get(node_id, 0) > self.node_timeout
            ]

            for node_id in inactive_nodes:
                self.logger.warning(f'Node {node_id} timed out, removing from network')
                await self.deregister_node(node_id)

            await asyncio.sleep(self.heartbeat_interval)

    async def get_network_status(self) -> dict:
        """Get current network status and statistics"""
        return {
            'total_nodes': len(self.nodes),
            'active_nodes': len(self.active_nodes),
            'nodes': self.nodes
        }

    async def get_healthy_nodes(self) -> Set[str]:
        """Get set of currently healthy nodes"""
        current_time = time.time()
        return {
            node_id for node_id in self.active_nodes
            if current_time - self.last_heartbeat.get(node_id, 0) <= self.node_timeout
        }

    async def start(self) -> None:
        """Start the orchestrator's background tasks"""
        self.logger.info('Starting Network Orchestrator')
        await self.check_node_health()

    async def stop(self) -> None:
        """Cleanup and shutdown orchestrator"""
        self.logger.info('Stopping Network Orchestrator')
        self.nodes.clear()
        self.active_nodes.clear()
        self.last_heartbeat.clear()
