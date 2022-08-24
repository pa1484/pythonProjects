from dataclasses import dataclass
from datetime import date
from typing import List

@dataclass
class VolumeByPeriod:
    period: int
    volume: int


@dataclass
class PowerTrade:
    day: date
    volumeByPeriods: List[VolumeByPeriod]
