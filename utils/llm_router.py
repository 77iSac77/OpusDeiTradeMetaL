# =========================
# FILE: utils/llm_router.py
# =========================
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


# =========================
# FILE: utils/llm_client.py
# (REPLACE THE WHOLE FILE)
# =========================
from __future__ import annotations

"""
OpusDeiTradeMetaL - OpenRouter LLM client with professional routing
- One OpenRouter key.
- Three chosen models (by env/config) + fallback pool.
- Task-based routing (analyst/explainer/generalist).
- SQLite cache to reduce calls.
"""

import aiohttp
import asyncio
import hashlib
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any, List

from config.settings import (
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
    LLM_POOL,
    BOT_CONFIG,
)
from storage.database import get_database
from utils.llm_router import LLMRouter, LLMTask, LLMRole

logger = logging.getLogger(__name__)


def _env(name: str) -> str:
    # Lazy import to avoid hard dependency cycles
    import os
    return (os.getenv(name) or "").strip()


@dataclass(frozen=True)
class LLMCallResult:
    content: str
    model_id: str


class LLMClient:
    def __init__(self) -> None:
        self.api_key = OPENROUTER_API_KEY
        self.base_url = OPENROUTER_BASE_URL
        self.db = get_database()

        # Fallback pool from settings (kept for robustness)
        fallback_pool = [m.model_id for m in sorted(LLM_POOL, key=lambda x: x.priority)]

        # The 3 chosen models (set these in Koyeb env when ready)
        # OPENROUTER_MODEL_ANALYST     -> Nemotron 3 Nano
        # OPENROUTER_MODEL_EXPLAINER   -> Mistral Small
        # OPENROUTER_MODEL_GENERALIST  -> Llama 3.1 Nemotron 70B
        self.router = LLMRouter(
            model_analyst=_env("OPENROUTER_MODEL_ANALYST"),
            model_explainer=_env("OPENROUTER_MODEL_EXPLAINER"),
            model_generalist=_env("OPENROUTER_MODEL_GENERALIST"),
            fallback_pool=fallback_pool,
        )

        self.session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://github.com/77iSac77/OpusDeiTradeMetaL",
                    "X-Title": "OpusDeiTradeMetaL",
                }
            )
        return self.session

    async def close(self) -> None:
        if self.session and not self.session.closed:
            await self.session.close()

    def _hash_messages(self, messages: List[Dict[str, str]]) -> str:
        joined = "\n".join([f'{m.get("role","")}::{m.get("content","")}' for m in messages])
        return hashlib.md5(joined.encode("utf-8")).hexdigest()

    def _within_daily_quota(self) -> bool:
        current = self.db.get_counter("llm_calls")
        limit = int(BOT_CONFIG.get("max_llm_calls_per_day", 1000))
        return current < limit

    async def _call_openrouter(
        self,
        model_id: str,
        messages: List[Dict[str, str]],
        temperature: float = 0.4,
        max_tokens: int = 900,
        timeout_s: int = 60,
    ) -> Optional[LLMCallResult]:
        session = await self._get_session()
        payload = {
            "model": model_id,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        try:
            async with session.post(self.base_url, json=payload, timeout=timeout_s) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                    content = (content or "").strip()
                    if content:
                        return LLMCallResult(content=content, model_id=model_id)
                    return None

                if resp.status == 429:
                    logger.warning("OpenRouter rate limit (429) on model=%s", model_id)
                    return None

                err = await resp.text()
                logger.error("OpenRouter error status=%s model=%s body=%s", resp.status, model_id, err[:500])
                return None

        except asyncio.TimeoutError:
            logger.warning("OpenRouter timeout on model=%s", model_id)
            return None
        except Exception as exc:
            logger.exception("OpenRouter call failed on model=%s: %s", model_id, exc)
            return None

    async def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        use_cache: bool = True,
        task: LLMTask = LLMTask.GENERIC,
        force_role: Optional[LLMRole] = None,
        temperature: float = 0.4,
        max_tokens: int = 900,
    ) -> Optional[str]:
        # Quota guard
        if not self._within_daily_quota():
            self.db.log_error("llm_quota", "LLMClient", "Daily quota reached", "Returning None")
            return None

        messages: List[Dict[str, str]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        # Cache
        cache_key = self._hash_messages(messages)
        if use_cache:
            cached = self.db.get_cached_response(cache_key)
            if cached:
                return cached

        # Routing
        candidates = self.router.route(task=task, force_role=force_role)

        for cand in candidates:
            if not cand.model_id:
                continue

            res = await self._call_openrouter(
                model_id=cand.model_id,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            if res:
                self.db.increment_counter("llm_calls")
                if use_cache:
                    self.db.cache_response(
                        cache_key,
                        prompt,
                        res.content,
                        res.model_id,
                        ttl_seconds=int(BOT_CONFIG.get("cache_ttl_seconds", 3600)),
                    )
                return res.content

            self.db.log_error("llm_failure", cand.model_id, "No response", "Trying next model")

        self.db.log_error("llm_all_failed", "LLMClient", "All models failed", "Returning None")
        return None

    # -----------------------------
    # High-level helpers (your bot uses these)
    # -----------------------------

    async def explain_term(self, term: str) -> Optional[str]:
        system_prompt = (
            "Você é um educador financeiro especializado em mercado de metais.\n"
            "Explique termos com clareza, de forma prática.\n"
            "Responda em português."
        )
        prompt = (
            f'Explique o termo "{term}" no contexto do mercado de metais.\n\n'
            "Formato:\n"
            "- Definição simples\n"
            "- Como funciona na prática\n"
            "- Exemplo curto\n"
            "- Por que importa para traders"
        )
        return await self.generate(prompt, system_prompt, use_cache=True, task=LLMTask.TERM, temperature=0.35, max_tokens=700)

    async def analyze_divergence(self, data: Dict[str, Any]) -> Optional[str]:
        system_prompt = (
            "Você é um analista quantitativo de mercado de metais.\n"
            "Identifique divergências/correlações quebradas e implicações.\n"
            "Responda em português, direto e acionável."
        )
        prompt = f"Dados:\n{data}\n\nEntregue:\n- Divergência detectada\n- Possíveis causas\n- O que observar a seguir"
        return await self.generate(prompt, system_prompt, task=LLMTask.DIVERGENCE, temperature=0.35, max_tokens=900)

    async def analyze_pre_event(self, event_type: str, event_data: Dict[str, Any]) -> Optional[str]:
        system_prompt = (
            "Você é um analista macro focado em metais.\n"
            "Monte cenários pré-evento e possíveis impactos.\n"
            "Responda em português, com números aproximados quando fizer sentido."
        )
        prompt = (
            f"Evento: {event_type}\nDados: {event_data}\n\n"
            "Entregue:\n"
            "- Cenário base\n- Cenário hawkish/forte\n- Cenário dovish/fraco\n"
            "- Impacto provável: XAU, XAG, DXY, Yields\n"
            "- Dica 🧠 do que monitorar"
        )
        return await self.generate(prompt, system_prompt, task=LLMTask.PRE_EVENT, temperature=0.35, max_tokens=900)

    async def analyze_post_event(self, event_type: str, expected: Dict[str, Any], actual: Dict[str, Any], market_reaction: Dict[str, Any]) -> Optional[str]:
        system_prompt = (
            "Você é um analista macro de metais.\n"
            "Faça um pós-evento curto: esperado vs realizado e reação do mercado.\n"
            "Responda em português, objetivo."
        )
        prompt = (
            f"Evento: {event_type}\n"
            f"Esperado: {expected}\n"
            f"Realizado: {actual}\n"
            f"Reação do mercado (primeiros minutos): {market_reaction}\n\n"
            "Entregue:\n"
            "- Surpresa (acima/abaixo do esperado)\n"
            "- Por que o mercado reagiu assim\n"
            "- Risco de continuação vs reversão\n"
            "- Próximo catalisador"
        )
        return await self.generate(prompt, system_prompt, task=LLMTask.POST_EVENT, temperature=0.35, max_tokens=900)

    def get_stats(self) -> Dict[str, Any]:
        calls_today = self.db.get_counter("llm_calls")
        max_calls = int(BOT_CONFIG.get("max_llm_calls_per_day", 1000))
        db_stats = self.db.get_stats()

        return {
            "calls_today": calls_today,
            "max_calls": max_calls,
            "remaining": max_calls - calls_today,
            "cache_entries": db_stats.get("llm_cache_count", 0),
        }


_llm_client: Optional[LLMClient] = None


def get_llm_client() -> LLMClient:
    global _llm_client
    if _llm_client is None:
        _llm_client = LLMClient()
    return _llm_client