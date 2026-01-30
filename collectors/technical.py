"""
OpusDeiTradeMetaL - Coletor de Níveis Técnicos
===============================================
Calcula e monitora níveis de suporte, resistência e indicadores técnicos.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from config.settings import METAIS, TECHNICAL_PROXIMITY_PERCENT
from storage.database import get_database

logger = logging.getLogger(__name__)


class LevelType(Enum):
    SUPORTE = "suporte"
    RESISTENCIA = "resistencia"


@dataclass
class TechnicalLevel:
    """Representa um nível técnico."""
    metal: str
    level_type: LevelType
    name: str
    value: float
    strength: int = 1
    touches: int = 0
    last_touch: Optional[datetime] = None
    
    def to_dict(self) -> Dict:
        return {
            "metal": self.metal,
            "type": self.level_type.value,
            "name": self.name,
            "value": self.value,
            "strength": self.strength,
            "touches": self.touches,
        }


@dataclass
class PivotPoints:
    """Pivot Points calculados."""
    pp: float
    r1: float
    r2: float
    r3: float
    s1: float
    s2: float
    s3: float


class TechnicalAnalyzer:
    """Analisador de níveis técnicos."""
    
    def __init__(self):
        self.db = get_database()
        self.levels: Dict[str, List[TechnicalLevel]] = {}
        self.pivots: Dict[str, PivotPoints] = {}
    
    def calculate_pivot_points(self, high: float, low: float, close: float) -> PivotPoints:
        """Calcula Pivot Points padrão."""
        pp = (high + low + close) / 3
        r1 = (2 * pp) - low
        s1 = (2 * pp) - high
        r2 = pp + (high - low)
        s2 = pp - (high - low)
        r3 = high + 2 * (pp - low)
        s3 = low - 2 * (high - pp)
        return PivotPoints(pp=pp, r1=r1, r2=r2, r3=r3, s1=s1, s2=s2, s3=s3)
    
    def calculate_sma(self, prices: List[float], period: int) -> Optional[float]:
        """Calcula Média Móvel Simples."""
        if len(prices) < period:
            return None
        return sum(prices[-period:]) / period
    
    def calculate_vwap(self, prices: List[float], volumes: List[float]) -> Optional[float]:
        """Calcula VWAP."""
        if len(prices) != len(volumes) or not prices:
            return None
        total_value = sum(p * v for p, v in zip(prices, volumes))
        total_volume = sum(volumes)
        if total_volume == 0:
            return None
        return total_value / total_volume
    
    def find_high_volume_zones(self, prices: List[float], volumes: List[float], 
                                num_zones: int = 3) -> List[Tuple[float, float]]:
        """Encontra zonas de alto volume."""
        if not prices or not volumes or len(prices) != len(volumes):
            return []
        
        min_price = min(prices)
        max_price = max(prices)
        num_bins = 20
        bin_size = (max_price - min_price) / num_bins if max_price != min_price else 1
        
        volume_by_bin: Dict[int, Tuple[float, float]] = {}
        
        for price, volume in zip(prices, volumes):
            bin_idx = int((price - min_price) / bin_size)
            bin_idx = min(bin_idx, num_bins - 1)
            
            if bin_idx not in volume_by_bin:
                volume_by_bin[bin_idx] = (0, 0)
            
            total_vol, count = volume_by_bin[bin_idx]
            volume_by_bin[bin_idx] = (total_vol + volume, count + 1)
        
        sorted_bins = sorted(volume_by_bin.items(), key=lambda x: x[1][0], reverse=True)
        
        zones = []
        for bin_idx, (total_vol, count) in sorted_bins[:num_zones]:
            center_price = min_price + (bin_idx + 0.5) * bin_size
            zones.append((center_price, total_vol))
        
        return zones
    
    def find_multiple_touches(self, prices: List[float], 
                               tolerance_percent: float = 0.5) -> List[Tuple[float, int]]:
        """Encontra níveis tocados múltiplas vezes."""
        if not prices:
            return []
        
        local_extremes = []
        
        for i in range(1, len(prices) - 1):
            if prices[i] > prices[i-1] and prices[i] > prices[i+1]:
                local_extremes.append(prices[i])
            elif prices[i] < prices[i-1] and prices[i] < prices[i+1]:
                local_extremes.append(prices[i])
        
        if not local_extremes:
            return []
        
        groups: List[List[float]] = []
        tolerance = tolerance_percent / 100
        
        for extreme in sorted(local_extremes):
            added = False
            for group in groups:
                avg = sum(group) / len(group)
                if abs(extreme - avg) / avg <= tolerance:
                    group.append(extreme)
                    added = True
                    break
            
            if not added:
                groups.append([extreme])
        
        levels = []
        for group in groups:
            if len(group) >= 2:
                avg_level = sum(group) / len(group)
                levels.append((avg_level, len(group)))
        
        return sorted(levels, key=lambda x: x[1], reverse=True)
    
    async def update_levels_for_metal(self, metal: str) -> List[TechnicalLevel]:
        """Atualiza todos os níveis técnicos para um metal."""
        levels = []
        
        history = self.db.get_price_history(metal, hours=24*52)
        if not history:
            return levels
        
        prices = [h["price"] for h in history]
        volumes = [h.get("volume", 0) or 0 for h in history]
        
        if not prices:
            return levels
        
        current_price = prices[-1]
        
        # LONGO PRAZO
        max_52w = max(prices)
        min_52w = min(prices)
        
        levels.append(TechnicalLevel(
            metal=metal, level_type=LevelType.RESISTENCIA,
            name="max_52w", value=max_52w, strength=5
        ))
        levels.append(TechnicalLevel(
            metal=metal, level_type=LevelType.SUPORTE,
            name="min_52w", value=min_52w, strength=5
        ))
        
        sma_50 = self.calculate_sma(prices, 50)
        if sma_50:
            level_type = LevelType.SUPORTE if current_price > sma_50 else LevelType.RESISTENCIA
            levels.append(TechnicalLevel(
                metal=metal, level_type=level_type,
                name="sma_50", value=sma_50, strength=3
            ))
        
        sma_200 = self.calculate_sma(prices, 200)
        if sma_200:
            level_type = LevelType.SUPORTE if current_price > sma_200 else LevelType.RESISTENCIA
            levels.append(TechnicalLevel(
                metal=metal, level_type=level_type,
                name="sma_200", value=sma_200, strength=4
            ))
        
        # CURTO PRAZO - PIVOT POINTS
        prices_24h = prices[-96:]
        if len(prices_24h) >= 4:
            high = max(prices_24h[:-1])
            low = min(prices_24h[:-1])
            close = prices_24h[-2]
            
            pivots = self.calculate_pivot_points(high, low, close)
            self.pivots[metal] = pivots
            
            levels.append(TechnicalLevel(
                metal=metal,
                level_type=LevelType.SUPORTE if current_price > pivots.pp else LevelType.RESISTENCIA,
                name="pivot_pp", value=pivots.pp, strength=3
            ))
            
            for name, value in [("r1", pivots.r1), ("r2", pivots.r2), ("r3", pivots.r3)]:
                levels.append(TechnicalLevel(
                    metal=metal, level_type=LevelType.RESISTENCIA,
                    name=f"pivot_{name}", value=value,
                    strength=2 if name == "r1" else (3 if name == "r2" else 4)
                ))
            
            for name, value in [("s1", pivots.s1), ("s2", pivots.s2), ("s3", pivots.s3)]:
                levels.append(TechnicalLevel(
                    metal=metal, level_type=LevelType.SUPORTE,
                    name=f"pivot_{name}", value=value,
                    strength=2 if name == "s1" else (3 if name == "s2" else 4)
                ))
        
        # VWAP
        if volumes and any(v > 0 for v in volumes):
            prices_today = prices[-96:]
            volumes_today = volumes[-96:]
            vwap = self.calculate_vwap(prices_today, volumes_today)
            if vwap:
                level_type = LevelType.SUPORTE if current_price > vwap else LevelType.RESISTENCIA
                levels.append(TechnicalLevel(
                    metal=metal, level_type=level_type,
                    name="vwap", value=vwap, strength=2
                ))
        
        # ZONAS DE ALTO VOLUME
        if volumes:
            prices_48h = prices[-192:]
            volumes_48h = volumes[-192:]
            hv_zones = self.find_high_volume_zones(prices_48h, volumes_48h)
            for i, (zone_price, zone_vol) in enumerate(hv_zones):
                level_type = LevelType.SUPORTE if current_price > zone_price else LevelType.RESISTENCIA
                levels.append(TechnicalLevel(
                    metal=metal, level_type=level_type,
                    name=f"hv_zone_{i+1}", value=zone_price, strength=3
                ))
        
        # MÚLTIPLOS TOQUES
        prices_5d = prices[-480:]
        multi_touch = self.find_multiple_touches(prices_5d)
        for i, (level_price, touches) in enumerate(multi_touch[:3]):
            level_type = LevelType.SUPORTE if current_price > level_price else LevelType.RESISTENCIA
            levels.append(TechnicalLevel(
                metal=metal, level_type=level_type,
                name=f"multi_touch_{i+1}", value=level_price,
                strength=min(touches, 5), touches=touches
            ))
        
        # Salvar no banco
        for level in levels:
            self.db.update_technical_level(
                metal=metal, level_type=level.level_type.value,
                level_name=level.name, value=level.value
            )
        
        self.levels[metal] = levels
        logger.info(f"Calculados {len(levels)} níveis técnicos para {metal}")
        return levels
    
    async def update_all_levels(self) -> Dict[str, List[TechnicalLevel]]:
        """Atualiza níveis para todos os metais."""
        all_levels = {}
        for metal in METAIS.keys():
            levels = await self.update_levels_for_metal(metal)
            all_levels[metal] = levels
        return all_levels
    
    def get_levels_for_metal(self, metal: str) -> List[TechnicalLevel]:
        """Obtém níveis técnicos de um metal."""
        return self.levels.get(metal.upper(), [])
    
    def check_proximity_alerts(self, metal: str, current_price: float) -> List[Dict]:
        """Verifica se preço está próximo de algum nível."""
        alerts = []
        levels = self.get_levels_for_metal(metal)
        
        for level in levels:
            distance_percent = abs(current_price - level.value) / level.value * 100
            
            if distance_percent <= TECHNICAL_PROXIMITY_PERCENT:
                alerts.append({
                    "metal": metal,
                    "level": level,
                    "current_price": current_price,
                    "distance_percent": distance_percent,
                    "approaching": current_price < level.value if level.level_type == LevelType.RESISTENCIA else current_price > level.value,
                })
        
        return alerts
    
    def check_level_breaks(self, metal: str, current_price: float, 
                           previous_price: float) -> List[Dict]:
        """Verifica se algum nível foi rompido."""
        alerts = []
        levels = self.get_levels_for_metal(metal)
        
        for level in levels:
            crossed_up = previous_price < level.value <= current_price
            crossed_down = previous_price > level.value >= current_price
            
            if crossed_up or crossed_down:
                alerts.append({
                    "metal": metal,
                    "level": level,
                    "current_price": current_price,
                    "previous_price": previous_price,
                    "direction": "up" if crossed_up else "down",
                })
        
        return alerts
    
    def get_nearest_levels(self, metal: str, current_price: float, 
                           num_levels: int = 3) -> Dict[str, List[TechnicalLevel]]:
        """Obtém níveis mais próximos acima e abaixo do preço."""
        levels = self.get_levels_for_metal(metal)
        
        above = sorted(
            [l for l in levels if l.value > current_price],
            key=lambda x: x.value
        )[:num_levels]
        
        below = sorted(
            [l for l in levels if l.value < current_price],
            key=lambda x: x.value, reverse=True
        )[:num_levels]
        
        return {"above": above, "below": below}
    
    def format_level_for_display(self, level: TechnicalLevel) -> str:
        """Formata nível para exibição."""
        name_map = {
            "max_52w": "Máx 52 sem",
            "min_52w": "Mín 52 sem",
            "sma_50": "MM50",
            "sma_200": "MM200",
            "pivot_pp": "Pivot",
            "pivot_r1": "R1", "pivot_r2": "R2", "pivot_r3": "R3",
            "pivot_s1": "S1", "pivot_s2": "S2", "pivot_s3": "S3",
            "vwap": "VWAP",
        }
        
        display_name = name_map.get(level.name, level.name)
        
        if level.name.startswith("hv_zone"):
            display_name = f"Zona Vol. {level.name[-1]}"
        elif level.name.startswith("multi_touch"):
            display_name = f"Nível ({level.touches} toques)"
        
        return f"{display_name}: ${level.value:,.2f}"


_analyzer: Optional[TechnicalAnalyzer] = None

def get_technical_analyzer() -> TechnicalAnalyzer:
    """Retorna instância singleton do analisador técnico."""
    global _analyzer
    if _analyzer is None:
        _analyzer = TechnicalAnalyzer()
    return _analyzer
