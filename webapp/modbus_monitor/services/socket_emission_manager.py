"""
Socket Emission Manager - Compatibility layer for old services
"""
import logging

logger = logging.getLogger(__name__)

class EmissionManager:
    """Stub emission manager for compatibility"""
    
    def __init__(self):
        self.enabled = False
    
    def enable_emission(self):
        self.enabled = True
        logger.info("EmissionManager enabled")
    
    def disable_emission(self):
        self.enabled = False 
        logger.info("EmissionManager disabled")

# Global instance for compatibility
_emission_manager = EmissionManager()

def get_emission_manager():
    """Get the global emission manager instance"""
    return _emission_manager