from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Any, List


class LLMRole(str, Enum):
    ANALYST = "analyst"        # Nemotron 3 Nano (analista/decisor)
    EXPLAINER = "explainer"    # Mistral Small (explicador sob demanda)
    GENERALIST = "generalist"  # Llama 3.1 Nemotron 70B (generalista / significado / fallback)


class LLMTask(str, Enum):
    ALERT_ANALYSIS = "alert_analysis"
    PRE_EVENT = "pre_event"
    POST_EVENT = "post_event"
    DIVERGENCE = "divergence"
    DIGEST = "digest"
    TERM = "term"
    COMMAND_HELP = "command_help"
    GENERIC = "generic"


@dataclass(frozen=True)
class RoutedModel:
    role: LLMRole
    model_id: str


class LLMRouter:
    """
    Professional router:
    - Picks the best model by TASK (not by random fallback).
    - Enforces a stable fallback order.
    - Allows env/config overrides for the 3 chosen models.
    """

    def __init__(
        self,
        model_analyst: Optional[str],
        model_explainer: Optional[str],
        model_generalist: Optional[str],
        fallback_pool: List[str],
    ) -> None:
        self.model_analyst = (model_analyst or "").strip()
        self.model_explainer = (model_explainer or "").strip()
        self.model_generalist = (model_generalist or "").strip()

        # Fallback pool must be non-empty; use as last-resort cascade
        self.fallback_pool = [m.strip() for m in fallback_pool if m and m.strip()]

    def _first_valid(self, *candidates: str) -> Optional[str]:
        for c in candidates:
            c = (c or "").strip()
            if c:
                return c
        return None

    def route(self, task: LLMTask, force_role: Optional[LLMRole] = None) -> List[RoutedModel]:
        """
        Returns an ORDERED list of models to try.
        """
        if force_role:
            primary = self._role_to_model(force_role)
            return self._with_fallback(primary)

        # Task-based routing
        if task in (LLMTask.ALERT_ANALYSIS, LLMTask.PRE_EVENT, LLMTask.POST_EVENT, LLMTask.DIVERGENCE):
            primary = self._role_to_model(LLMRole.ANALYST)
            return self._with_fallback(primary)

        if task in (LLMTask.TERM, LLMTask.COMMAND_HELP):
            primary = self._role_to_model(LLMRole.GENERALIST)
            return self._with_fallback(primary)

        if task in (LLMTask.DIGEST,):
            # Digests: analyst first; generalist as fallback; explainer last
            primary = self._role_to_model(LLMRole.ANALYST)
            return self._with_fallback(primary)

        # Generic default: generalist first, then analyst, then explainer
        primary = self._role_to_model(LLMRole.GENERALIST)
        return self._with_fallback(primary, extra_first=[
            self._role_to_model(LLMRole.ANALYST),
            self._role_to_model(LLMRole.EXPLAINER),
        ])

    def _role_to_model(self, role: LLMRole) -> Optional[RoutedModel]:
        if role == LLMRole.ANALYST:
            mid = self._first_valid(self.model_analyst)
            return RoutedModel(role=role, model_id=mid) if mid else None
        if role == LLMRole.EXPLAINER:
            mid = self._first_valid(self.model_explainer)
            return RoutedModel(role=role, model_id=mid) if mid else None
        if role == LLMRole.GENERALIST:
            mid = self._first_valid(self.model_generalist)
            return RoutedModel(role=role, model_id=mid) if mid else None
        return None

    def _with_fallback(
        self,
        primary: Optional[RoutedModel],
        extra_first: Optional[List[Optional[RoutedModel]]] = None,
    ) -> List[RoutedModel]:
        ordered: List[RoutedModel] = []
        seen: set[str] = set()

        def add(rm: Optional[RoutedModel]) -> None:
            if not rm or not rm.model_id:
                return
            if rm.model_id in seen:
                return
            seen.add(rm.model_id)
            ordered.append(rm)

        add(primary)
        if extra_first:
            for rm in extra_first:
                add(rm)

        # Add any missing chosen roles as fallback candidates (stable order)
        add(self._role_to_model(LLMRole.GENERALIST))
        add(self._role_to_model(LLMRole.ANALYST))
        add(self._role_to_model(LLMRole.EXPLAINER))

        # Finally, add the pool
        for mid in self.fallback_pool:
            if mid and mid not in seen:
                seen.add(mid)
                ordered.append(RoutedModel(role=LLMRole.GENERIC if hasattr(LLMRole, "GENERIC") else LLMRole.GENERALIST, model_id=mid))

        return ordered