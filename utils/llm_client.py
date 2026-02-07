"""
OpusDeiTradeMetaL - Cliente LLM com Pool e Fallback
====================================================
Gerencia chamadas a modelos LLM via OpenRouter com fallback em cascata.
"""

import aiohttp
import asyncio
import hashlib
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime

# =========================
# ADDED (router + env helper)
# =========================
import os
from utils.llm_router import LLMRouter, LLMTask, LLMRole

from config.settings import (
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
    LLM_POOL,
    BOT_CONFIG
)
from storage.database import get_database

logger = logging.getLogger(__name__)


# =========================
# ADDED
# =========================
def _env(name: str) -> str:
    return (os.getenv(name) or "").strip()


class LLMClient:
    """Cliente para chamadas LLM via OpenRouter."""

    def __init__(self):
        """Inicializa cliente LLM."""
        self.api_key = OPENROUTER_API_KEY
        self.base_url = OPENROUTER_BASE_URL
        self.db = get_database()
        self.models = sorted(LLM_POOL, key=lambda x: x.priority)
        self.current_model_index = 0
        self.session: Optional[aiohttp.ClientSession] = None

        # =========================
        # ADDED (router profissional)
        # - Só ativa se as env vars estiverem setadas
        # - Se não estiverem, seu fallback atual continua igual
        # =========================
        fallback_pool = [m.model_id for m in self.models]
        self.router = LLMRouter(
            model_analyst=_env("OPENROUTER_MODEL_ANALYST"),
            model_explainer=_env("OPENROUTER_MODEL_EXPLAINER"),
            model_generalist=_env("OPENROUTER_MODEL_GENERALIST"),
            fallback_pool=fallback_pool,
        )
        self._model_name_by_id = {m.model_id: m.name for m in self.models}

    async def _get_session(self) -> aiohttp.ClientSession:
        """Obtém ou cria sessão HTTP."""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://github.com/opusdeitrade/metal",
                    "X-Title": "OpusDeiTradeMetaL"
                }
            )
        return self.session

    async def close(self):
        """Fecha sessão HTTP."""
        if self.session and not self.session.closed:
            await self.session.close()

    def _hash_prompt(self, prompt: str) -> str:
        """Gera hash do prompt para cache."""
        return hashlib.md5(prompt.encode()).hexdigest()

    def _check_rate_limit(self) -> bool:
        """
        Verifica se ainda há quota de requests.

        Returns:
            True se pode fazer request
        """
        current_calls = self.db.get_counter("llm_calls")
        max_calls = BOT_CONFIG.get("max_llm_calls_per_day", 1000)
        return current_calls < max_calls

    async def _call_model(self, model_id: str, messages: List[Dict],
                          temperature: float = 0.7,
                          max_tokens: int = 1000) -> Optional[str]:
        """
        Faz chamada a um modelo específico.

        Args:
            model_id: ID do modelo no OpenRouter
            messages: Lista de mensagens
            temperature: Temperatura
            max_tokens: Máximo de tokens na resposta

        Returns:
            Resposta do modelo ou None se falhar
        """
        session = await self._get_session()

        payload = {
            "model": model_id,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        try:
            async with session.post(self.base_url, json=payload, timeout=60) as response:
                if response.status == 200:
                    data = await response.json()
                    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                    return content.strip() if content else None

                elif response.status == 429:
                    # Rate limit - tentar próximo modelo
                    logger.warning(f"Rate limit atingido para {model_id}")
                    return None

                else:
                    error_text = await response.text()
                    logger.error(f"Erro {response.status} em {model_id}: {error_text}")
                    return None

        except asyncio.TimeoutError:
            logger.error(f"Timeout em {model_id}")
            return None
        except Exception as e:
            logger.error(f"Erro em chamada LLM {model_id}: {e}")
            return None

    async def generate(self, prompt: str,
                       system_prompt: Optional[str] = None,
                       use_cache: bool = True,
                       task_type: str = "geral",
                       max_tokens: int = 1000) -> Optional[str]:
        """
        Gera resposta usando pool de modelos com fallback.

        Args:
            prompt: Prompt do usuário
            system_prompt: Prompt de sistema (opcional)
            use_cache: Usar cache se disponível
            task_type: Tipo de tarefa (geral, analise, raciocinio)

        Returns:
            Resposta ou None se todos falharem
        """
        # Verificar rate limit
        if not self._check_rate_limit():
            logger.warning("Rate limit diário atingido")
            return None

        # Verificar cache
        prompt_hash = self._hash_prompt(prompt + (system_prompt or ""))
        if use_cache:
            cached = self.db.get_cached_response(prompt_hash)
            if cached:
                logger.debug("Resposta obtida do cache")
                return cached

        # Montar mensagens
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        # =========================
        # ADDED (router opcional)
        # - Se OPENROUTER_MODEL_* estiverem definidos, usa roteamento profissional
        # - Caso contrário, cai no seu fluxo atual (logo abaixo) sem mudar nada
        # =========================
        if (
            _env("OPENROUTER_MODEL_ANALYST")
            and _env("OPENROUTER_MODEL_EXPLAINER")
            and _env("OPENROUTER_MODEL_GENERALIST")
        ):
            task_map = {
                "analise": LLMTask.ALERT_ANALYSIS,
                "raciocinio": LLMTask.DIVERGENCE,
                "geral": LLMTask.GENERIC,
                "digest": LLMTask.DIGEST,
                "termo": LLMTask.TERM,
                "ajuda": LLMTask.COMMAND_HELP,
                "pre_event": LLMTask.PRE_EVENT,
                "post_event": LLMTask.POST_EVENT,
            }
            task = task_map.get((task_type or "").lower().strip(), LLMTask.GENERIC)

            candidates = self.router.route(task=task, force_role=None)

            for cand in candidates:
                model_id = (cand.model_id or "").strip()
                if not model_id:
                    continue

                display_name = self._model_name_by_id.get(model_id, model_id)
                logger.info(f"Tentando modelo: {display_name}")

                response = await self._call_model(model_id, messages, max_tokens=max_tokens)
                if response:
                    self.db.increment_counter("llm_calls")

                    if use_cache:
                        self.db.cache_response(
                            prompt_hash, prompt, response, display_name,
                            ttl_seconds=BOT_CONFIG.get("cache_ttl_seconds", 3600)
                        )

                    logger.info(f"Resposta obtida de {display_name}")
                    return response

                self.db.log_error(
                    "llm_failure", display_name,
                    "Falha ao obter resposta",
                    "Trying next routed model"
                )
            # Se o router falhar em tudo, continua para seu fluxo antigo (pool)
            # (não dá return aqui)

        # ---------------------------------------------------------------------
        # SEU FLUXO ATUAL (INTACTO)
        # ---------------------------------------------------------------------

        # Selecionar modelo inicial baseado na tarefa
        start_index = 0
        if task_type == "analise":
            # Preferir Nemotron para análise
            start_index = next(
                (i for i, m in enumerate(self.models) if "nemotron" in m.model_id.lower()),
                0
            )
        elif task_type == "raciocinio":
            # Preferir DeepSeek para raciocínio
            start_index = next(
                (i for i, m in enumerate(self.models) if "deepseek" in m.model_id.lower()),
                0
            )

        # Tentar cada modelo em ordem
        for i in range(len(self.models)):
            model_index = (start_index + i) % len(self.models)
            model = self.models[model_index]

            logger.info(f"Tentando modelo: {model.name}")
            response = await self._call_model(model.model_id, messages, max_tokens=max_tokens)

            if response:
                # Incrementar contador
                self.db.increment_counter("llm_calls")

                # Salvar no cache
                if use_cache:
                    self.db.cache_response(
                        prompt_hash, prompt, response, model.name,
                        ttl_seconds=BOT_CONFIG.get("cache_ttl_seconds", 3600)
                    )

                logger.info(f"Resposta obtida de {model.name}")
                return response

            # Modelo falhou, log e tentar próximo
            self.db.log_error(
                "llm_failure", model.name,
                f"Falha ao obter resposta",
                f"Tentando próximo modelo"
            )

        # Todos falharam
        logger.error("Todos os modelos LLM falharam")
        self.db.log_error(
            "llm_all_failed", "LLMClient",
            "Todos os modelos falharam",
            "Retornando None"
        )
        return None

    async def summarize_news(self, title: str, content: str,
                             metal: str) -> Optional[Dict[str, str]]:
        """
        Resume notícia e analisa impacto.

        Args:
            title: Título da notícia
            content: Conteúdo
            metal: Metal relacionado

        Returns:
            Dict com resumo e análise
        """
        system_prompt = """Você é um analista de mercado de metais preciosos.
Responda sempre em português brasileiro.
Seja conciso e direto."""

        prompt = f"""Analise esta notícia sobre {metal}:

Título: {title}

Conteúdo: {content[:2000]}

Responda no formato:
RESUMO: (2-3 frases)
IMPACTO: (bullish/bearish/neutro para o metal)
CONTEXTO: (1 frase de contexto relevante)"""

        response = await self.generate(prompt, system_prompt, task_type="analise")

        if response:
            # Parsear resposta
            lines = response.strip().split("\n")
            result = {}
            for line in lines:
                if line.startswith("RESUMO:"):
                    result["resumo"] = line.replace("RESUMO:", "").strip()
                elif line.startswith("IMPACTO:"):
                    result["impacto"] = line.replace("IMPACTO:", "").strip()
                elif line.startswith("CONTEXTO:"):
                    result["contexto"] = line.replace("CONTEXTO:", "").strip()
            return result if result else {"resumo": response}

        return None

    async def analyze_correlation(self, data: Dict[str, Any]) -> Optional[str]:
        """
        Analisa correlações e divergências.

        Args:
            data: Dados de mercado

        Returns:
            Análise em texto
        """
        system_prompt = """Você é um analista quantitativo de mercado de metais.
Identifique divergências e correlações incomuns.
Responda sempre em português brasileiro.
Seja técnico mas compreensível."""

        prompt = f"""Analise estes dados de mercado:

{data}

Identifique:
1. Correlações quebradas (ex: DXY sobe mas ouro também)
2. Divergências relevantes
3. Possíveis explicações
4. Implicações para trading"""

        return await self.generate(prompt, system_prompt, task_type="raciocinio")

    async def explain_term(self, term: str) -> Optional[str]:
        """
        Explica termo de mercado.

        Args:
            term: Termo a explicar

        Returns:
            Explicação
        """
        system_prompt = """Você é um educador financeiro especializado em mercado de metais.
Explique termos de forma clara e didática.
Use exemplos práticos.
Responda em português brasileiro."""

        prompt = f"""Explique o termo "{term}" no contexto do mercado de metais.

Formato:
- Definição simples
- Como funciona na prática
- Exemplo real
- Por que é importante para traders"""

        return await self.generate(prompt, system_prompt, use_cache=True)

    async def generate_digest(self, events: List[Dict],
                              prices: Dict[str, float],
                              period: str) -> Optional[str]:
        """
        Gera digest de mercado.

        Args:
            events: Lista de eventos do período
            prices: Preços atuais
            period: Período (asia, eu_us, weekly)

        Returns:
            Texto do digest
        """
        system_prompt = """Você é um analista de mercado de metais preciosos experiente.
Responda em português brasileiro.

REGRAS DE FORMATAÇÃO (OBRIGATÓRIO):
- NÃO use Markdown: nada de ##, **, ```, tabelas, ---, listas numeradas
- Escreva em texto corrido com parágrafos curtos
- Use emojis para separar seções: 📈 📉 ⚠️ 🎯 🔍 💡
- Máximo 1200 caracteres no total
- Termine SEMPRE com uma frase completa, nunca corte no meio
- Seja direto como um trader falando com outro trader"""

        prompt = f"""Analise o mercado de metais agora.

Preços atuais: {prices}
Destaques: {events}

Estruture assim (texto corrido, sem listas):

📈 MOVIMENTOS: O que subiu, o que caiu e por quanto. Destaque os 3 mais relevantes.

🔍 CONTEXTO: Possíveis drivers (macro, geopolítica, oferta/demanda). Correlações entre os movimentos.

🎯 FICAR DE OLHO: Níveis técnicos chave, eventos próximos, cenários possíveis.

Seja completo mas conciso. Cada seção deve ter 2-3 frases no máximo."""

        return await self.generate(prompt, system_prompt, task_type="analise",
                                   max_tokens=600)

    async def analyze_technical_level(self, metal: str, current_price: float,
                                       level_name: str, level_value: float,
                                       level_type: str) -> Optional[str]:
        """
        Analisa aproximação/teste de nível técnico.

        Args:
            metal: Código do metal
            current_price: Preço atual
            level_name: Nome do nível
            level_value: Valor do nível
            level_type: Tipo (suporte/resistência)

        Returns:
            Análise do nível
        """
        system_prompt = """Você é um analista técnico de metais.
Analise níveis de suporte e resistência.
Seja prático e objetivo.
Responda em português brasileiro."""

        direction = "acima" if current_price > level_value else "abaixo"
        distance = abs(current_price - level_value) / level_value * 100

        prompt = f"""Analise esta situação técnica:

Metal: {metal}
Preço atual: ${current_price:.2f}
Nível ({level_type}): {level_name} = ${level_value:.2f}
Posição: {distance:.2f}% {direction} do nível

Forneça:
1. Relevância deste nível
2. Cenário se romper
3. Cenário se rejeitar
4. Volume/momentum necessário para confirmação"""

        return await self.generate(prompt, system_prompt, task_type="analise")

    async def analyze_impact(self, event_type: str, event_data: Dict) -> Optional[str]:
        """
        Analisa possível impacto de evento futuro.

        Args:
            event_type: Tipo do evento (FOMC, CPI, etc)
            event_data: Dados do evento

        Returns:
            Análise de impacto
        """
        system_prompt = """Você é um analista macro especializado em metais.
Analise impactos de eventos econômicos nos metais preciosos e industriais.
Seja específico sobre cenários.
Responda em português brasileiro."""

        prompt = f"""Analise o possível impacto deste evento:

Evento: {event_type}
Dados: {event_data}

Para cada cenário, descreva:
1. Impacto em XAU Ouro (% estimado)
2. Impacto em XAG Prata (% estimado)
3. Impacto em metais industriais
4. Correlações a observar (DXY, Yields, etc)"""

        return await self.generate(prompt, system_prompt, task_type="raciocinio")

    def get_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas de uso do LLM."""
        calls_today = self.db.get_counter("llm_calls")
        max_calls = BOT_CONFIG.get("max_llm_calls_per_day", 1000)

        # Contar cache hits (aproximado pelo número de entradas)
        db_stats = self.db.get_stats()

        return {
            "calls_today": calls_today,
            "max_calls": max_calls,
            "remaining": max_calls - calls_today,
            "cache_entries": db_stats.get("llm_cache_count", 0),
            "current_model": self.models[0].name if self.models else "none",
        }


# Singleton
_llm_client: Optional[LLMClient] = None


def get_llm_client() -> LLMClient:
    """Retorna instância singleton do cliente LLM."""
    global _llm_client
    if _llm_client is None:
        _llm_client = LLMClient()
    return _llm_client