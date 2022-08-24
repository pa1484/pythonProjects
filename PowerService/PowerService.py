from PowerService.PowerTrade import PowerTrade
from PowerService.PowerTrade import VolumeByPeriod
import random

class PowerService:
    def getTrades(self, day):
        powerTrades = []
        for i in range(1, 3):
            volumeByPeriods = []
            for i in range(1, 25):
                volumeByPeriods.append(VolumeByPeriod(i, random.randint(-1000, 1000)))
            powerTrade = PowerTrade(day, volumeByPeriods)
            powerTrades.append(powerTrade)
        return powerTrades
