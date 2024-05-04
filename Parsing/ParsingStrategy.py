from abc import ABC, abstractmethod
from typing import Dict, Any


class ParsingStrategy(ABC):
    @abstractmethod
    def extract_image_record(self, observation) -> Dict[str, Any]:
        pass

    @abstractmethod
    def build_observation(self, observation, buffer) -> Dict[str, Any]:
        return dict

    @abstractmethod
    def update_observation(self, existing_observation, observation, buffer):
        pass
