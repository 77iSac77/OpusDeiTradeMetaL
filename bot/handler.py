"""
OpusDeiTradeMetaL - Bot Telegram Handler
==========================================
Handlers de comandos e interações do Telegram.
"""

import asyncio
import logging
import re
from datetime import datetime, timedelta
from utils.time_utils import utcnow
from typing import Dict, Optional
import psutil

from telegram import Update, Bot
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ContextTypes, filters
)

from config.settings import (
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, METAIS,
    BOT_CONFIG, formato_metal, resolve_metal,
    FRED_API_KEY, ETHERSCAN_API_KEY
)
from storage.database import get_database
from utils.llm_client import get_llm_client
from utils.time_utils import format_timestamp_all_zones
from bot.formatter import MessageFormatter
from collectors.prices import get_price_collector
from collectors.technical import get_technical_analyzer
from collectors.macro import get_macro_collector
from collectors.institutional import get_institutional_collector
from processors.alerts import get_alert_processor

logger = logging.getLogger(__name__)


class TelegramBot:
    """Handler principal do bot Telegram."""
    
    def __init__(self):
        self.db = get_database()
        self.llm = get_llm_client()
        self.formatter = MessageFormatter()
        
        # Coletores
        self.price_collector = get_price_collector()
        self.technical = get_technical_analyzer()
        self.macro = get_macro_collector()
        self.institutional = get_institutional_collector()
        
        # Bot
        self.app: Optional[Application] = None
        self.bot: Optional[Bot] = None
        self.start_time = utcnow()
        
        # Chat autorizado
        self.authorized_chat = int(TELEGRAM_CHAT_ID) if TELEGRAM_CHAT_ID else None
    
    def _is_authorized(self, chat_id: int) -> bool:
        """Verifica se chat é autorizado."""
        if not self.authorized_chat:
            return True  # Sem restrição se não configurado
        return chat_id == self.authorized_chat
    
    async def send_message(self, text: str, chat_id: int = None):
        """Envia mensagem para o chat."""
        if not self.bot:
            logger.error("Bot não inicializado")
            return
        
        target = chat_id or self.authorized_chat
        if not target:
            logger.error("Nenhum chat_id disponível")
            return
        
        try:
            # Telegram tem limite de 4096 caracteres
            if len(text) > 4000:
                # Dividir em partes
                parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
                for part in parts:
                    await self.bot.send_message(chat_id=target, text=part, parse_mode="HTML")
                    await asyncio.sleep(0.3)
            else:
                await self.bot.send_message(chat_id=target, text=text, parse_mode="HTML")
                
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem: {e}")
    
    # =========================================================================
    # COMANDOS GERAIS
    # =========================================================================
    
    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler do comando /start."""
        if not self._is_authorized(update.effective_chat.id):
            return
        
        # Salvar chat_id se não tiver
        if not self.authorized_chat:
            self.authorized_chat = update.effective_chat.id
            self.db.set_config("telegram_chat_id", self.authorized_chat)
        
        msg = """🤖 <b>OpusDeiTradeMetaL</b> ativo!

Monitorando 12 metais em 9 mercados globais:
🥇 Preciosos: Ouro, Prata, Platina, Paládio
⚙️ Industriais: Cobre, Alumínio, Níquel, Chumbo, Zinco, Estanho
☢️ Estratégicos: Urânio, Minério de Ferro

📊 Alertas em tempo real
📅 Calendário econômico
🏦 Dados institucionais (COT, ETFs)
🐋 Movimentos on-chain

Use /comandos para ver opções disponíveis."""
        
        await update.message.reply_text(msg, parse_mode="HTML")
    
    async def cmd_comandos(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler do comando /comandos."""
        if not self._is_authorized(update.effective_chat.id):
            return
        
        msg = self.formatter.format_help()
        await update.message.reply_text(msg)
    
    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler do comando /status."""
        if not self._is_authorized(update.effective_chat.id):
            return
        
        # Calcular uptime
        uptime = utcnow() - self.start_time
        hours = int(uptime.total_seconds() // 3600)
        minutes = int((uptime.total_seconds() % 3600) // 60)
        uptime_str = f"{hours}h {minutes}m"
        
        # RAM
        process = psutil.Process()
        ram_mb = process.memory_info().rss / (1024 * 1024)
        
        # Stats do banco
        db_stats = self.db.get_stats()
        llm_stats = self.llm.get_stats()
        
        # Último alerta
        last_alert = self.db.get_config("last_alert_time", "Nenhum")
        
        stats = {
            "uptime": uptime_str,
            "connections_ok": True,
            "ram_mb": ram_mb,
            "last_alert": last_alert,
            "metals_live": self.price_collector.get_all_last_prices().get("XAU") is not None,
            "fred": bool(FRED_API_KEY),
            "etherscan": bool(ETHERSCAN_API_KEY),
            "openrouter": llm_stats.get("remaining", 0) > 0,
            "alerts_24h": self.db.get_alerts_count_today(),
            "llm_calls": llm_stats.get("calls_today", 0),
            "llm_max": llm_stats.get("max_calls", 1000),
            "errors_24h": self.db.get_error_count_24h(),
        }
        
        msg = self.formatter.format_status(stats)
        await update.message.reply_text(msg)
    
    async def cmd_config(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler do comando /config."""
        if not self._is_authorized(update.effective_chat.id):
            return
        
        llm_stats = self.llm.get_stats()
        
        config = {
            "timezone": self.db.get_config("timezone", -3),
            "alertas_ativos": self.db.get_config("alertas_ativos", True),
            "filtros": self.db.get_config("filtros", []),
            "digest_asia": self.db.get_config("digest_asia", True),
            "digest_eu_us": self.db.get_config("digest_eu_us", True),
            "digest_weekly": self.db.get_config("digest_weekly", True),
            "llm_calls_today": llm_stats.get("calls_today", 0),
            "llm_remaining": llm_stats.get("remaining", 1000),
        }
        
        msg = self.formatter.format_config(config)
        await update.message.reply_text(msg)
    
    async def cmd_teste(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler do comando /teste."""
        if not self._is_authorized(update.effective_chat.id):
            return
        
        await update.message.reply_text("🔄 Testando conexões...")
        
        results = []
        
        # Teste Metals.live
        try:
            prices = await self.price_collector.collect_all_prices()
            results.append(f"✅ Metals.live: {len(prices)} preços")
        except Exception as e:
            results.append(f"❌ Metals.live: {str(e)[:30]}")
        
        # Teste OpenRouter
        try:
            response = await self.llm.generate("Responda apenas: OK", use_cache=False)
            results.append(f"✅ OpenRouter: {'OK' if response else 'Sem resposta'}")
        except Exception as e:
            results.append(f"❌ OpenRouter: {str(e)[:30]}")
        
        # Teste Database
        try:
            self.db.get_config("teste")
            results.append("✅ Database: OK")
        except Exception as e:
            results.append(f"❌ Database: {str(e)[:30]}")
        
        msg = "🔌 <b>Teste de Conexões</b>\n\n" + "\n".join(results)
        await update.message.reply_text(msg, parse_mode="HTML")
    
    async def cmd_erros(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler do comando /erros."""
        if not self._is_authorized(update.effective_chat.id):
            return
        
        errors = self.db.get_recent_errors(10)
        
        # Calcular taxa de sucesso
        total_ops = self.db.get_counter("total_ops") or 1
        error_count = len(errors)
        success_rate = ((total_ops - error_count) / total_ops) * 100
        
        msg = f"📊 Taxa de sucesso (24h): {success_rate:.1f}%\n\n"
        msg += self.formatter.format_erros(errors)
        
        await update.message.reply_text(msg)
    
    # =========================================================================
    # COMANDOS DE DADOS
    # =========================================================================
    
    async def cmd_ativos(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler do comando /ativos."""
        if not self._is_authorized(update.effective_chat.id):
            return
        
        await update.message.reply_text("📊 Coletando preços...")
        
        prices = await self.price_collector.collect_all_prices()
        
        # Converter para formato esperado pelo formatter
        prices_dict = {}
        for code, price_data in prices.items():
            prices_dict[code] = {
                "price": price_data.price,
                "change_percent": price_data.change_percent,
            }
        
        msg = self.formatter.format_ativos_response(prices_dict)
        await update.message.reply_text(msg)
    
    async def cmd_preco(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler do comando /preco [metal]."""
        if not self._is_authorized(update.effective_chat.id):
            return
        
        args = context.args
        if not args:
            await update.message.reply_text(
                "Uso: /preco [metal]\n"
                "Exemplo: /preco XAU ou /preco ouro\n\n"
                "Metais: XAU, XAG, XPT, XPD, XCU, XAL, XNI, XPB, XZN, XSN, UX, FE\n"
                "Ou use nomes: ouro, prata, cobre, platina..."
            )
            return
        
        input_str = " ".join(args)
        metal = resolve_metal(input_str)
        if not metal:
            await update.message.reply_text(
                f"Metal '{input_str}' não encontrado.\n"
                "Use código (XAU, XAG) ou nome (ouro, prata, cobre...)."
            )
            return
        
        price_data = self.price_collector.get_last_price(metal)
        if not price_data:
            await update.message.reply_text(f"Preço de {formato_metal(metal)} não disponível.")
            return
        
        m = METAIS[metal]
        msg = f"""{m.emoji} <b>{formato_metal(metal)}</b>

💰 Preço: ${price_data.price:,.2f}
📈 Variação: {price_data.change_percent:+.2f}%
"""
        if price_data.high_24h:
            msg += f"📊 Máx 24h: ${price_data.high_24h:,.2f}\n"
        if price_data.low_24h:
            msg += f"📊 Mín 24h: ${price_data.low_24h:,.2f}\n"
        
        msg += f"\n{format_timestamp_all_zones()}"
        
        await update.message.reply_text(msg, parse_mode="HTML")
    
    async def cmd_resumo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler do comando /resumo [metal]."""
        if not self._is_authorized(update.effective_chat.id):
            return
        
        args = context.args
        if args:
            input_str = " ".join(args)
            metal = resolve_metal(input_str)
            if not metal:
                await update.message.reply_text(
                    f"Metal '{input_str}' não encontrado.\n"
                    "Use código (XAU, XAG) ou nome (ouro, prata, cobre...)."
                )
                return
        else:
            metal = "XAU"
        
        await update.message.reply_text(f"📊 Gerando resumo de {formato_metal(metal)}...")
        
        # Coletar dados
        price_data = self.price_collector.get_last_price(metal)
        levels = self.technical.get_levels_for_metal(metal)
        cot = self.institutional.get_cot_for_metal(metal)
        
        # Montar dados para o formatter
        data = {
            "price": price_data.price if price_data else 0,
            "change_24h": price_data.change_percent if price_data else 0,
            "high_24h": price_data.high_24h if price_data else 0,
            "low_24h": price_data.low_24h if price_data else 0,
        }
        
        # Adicionar níveis técnicos
        for level in levels:
            if level.name == "sma_50":
                data["sma_50"] = level.value
            elif level.name == "sma_200":
                data["sma_200"] = level.value
            elif level.name == "pivot_pp":
                data["pivot"] = level.value
            elif level.name == "pivot_r1":
                data["r1"] = level.value
            elif level.name == "pivot_s1":
                data["s1"] = level.value
        
        # Adicionar COT
        if cot:
            data["cot"] = {
                "mm_net": cot.mm_net,
                "mm_change": cot.mm_change,
            }
        
        msg = self.formatter.format_resumo_metal(metal, data)
        await update.message.reply_text(msg)
    
    async def cmd_cot(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler do comando /cot [metal]."""
        if not self._is_authorized(update.effective_chat.id):
            return
        
        args = context.args
        if args:
            input_str = " ".join(args)
            metal = resolve_metal(input_str)
            if not metal:
                await update.message.reply_text(
                    f"Metal '{input_str}' não encontrado.\n"
                    "Use código (XAU, XAG) ou nome (ouro, prata, cobre...)."
                )
                return
        else:
            metal = "XAU"
        
        cot = self.institutional.get_cot_for_metal(metal)
        if not cot:
            await update.message.reply_text(f"Dados COT não disponíveis para {metal}.")
            return
        
        cot_dict = cot.to_dict()
        cot_dict["managed_money"] = {
            "long": cot.mm_long,
            "short": cot.mm_short,
            "net": cot.mm_net,
        }
        
        msg = self.formatter.format_cot_alert(metal, {
            "report_date": cot.report_date.strftime("%d/%m/%Y"),
            "mm_long": cot.mm_long,
            "mm_short": cot.mm_short,
            "mm_net": cot.mm_net,
            "mm_change": cot.mm_change,
            "comm_long": cot.comm_long,
            "comm_short": cot.comm_short,
            "comm_net": cot.comm_net,
            "open_interest": cot.open_interest,
        })
        
        await update.message.reply_text(msg)
    
    async def cmd_digest(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler do comando /digest."""
        if not self._is_authorized(update.effective_chat.id):
            return
        
        await update.message.reply_text("📊 Gerando digest com análise...")
        
        prices = await self.price_collector.collect_all_prices()
        
        prices_dict = {}
        for code, price_data in prices.items():
            prices_dict[code] = {
                "price": price_data.price,
                "change": price_data.change_percent,
            }
        
        highlights = []
        sorted_by_change = sorted(
            prices.items(),
            key=lambda x: abs(x[1].change_percent),
            reverse=True
        )
        
        for code, data in sorted_by_change[:3]:
            direction = "📈" if data.change_percent > 0 else "📉"
            highlights.append(f"{direction} {formato_metal(code)}: {data.change_percent:+.2f}%")
        
        # Eventos próximos
        upcoming_events = self.macro.get_upcoming_events(hours=24)
        upcoming = [f"{e.title} ({e.country})" for e in upcoming_events[:3]]
        
        msg = self.formatter.format_digest_eu_us(prices_dict, highlights, upcoming)
        
        # Gerar análise LLM
        llm_analysis = await self.llm.generate_digest(
            events=[{"highlight": h} for h in highlights],
            prices=prices_dict,
            period="atual",
        )
        
        if llm_analysis:
            msg += f"\n\n🧠 ANÁLISE\n{llm_analysis}"
        
        await update.message.reply_text(msg)
    
    async def cmd_agenda(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler do comando /agenda [dias]."""
        if not self._is_authorized(update.effective_chat.id):
            return
        
        args = context.args
        days = int(args[0]) if args and args[0].isdigit() else 7
        
        events = self.db.get_upcoming_events(days)
        
        events_list = []
        for event in events:
            events_list.append({
                "title": event.get("title", "Evento"),
                "event_time": event.get("event_time", ""),
                "impact": event.get("impact", "medium"),
            })
        
        msg = self.formatter.format_agenda(events_list, days)
        await update.message.reply_text(msg)
    
    # =========================================================================
    # COMANDOS DE CONTROLE
    # =========================================================================
    
    async def cmd_silenciar(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler do comando /silenciar [tempo]."""
        if not self._is_authorized(update.effective_chat.id):
            return
        
        args = context.args
        if not args:
            await update.message.reply_text(
                "Uso: /silenciar [tempo]\nExemplo: /silenciar 2h ou /silenciar 30min"
            )
            return
        
        time_str = args[0].lower()
        
        # Parsear tempo
        match = re.match(r"(\d+)(h|min|m)?", time_str)
        if not match:
            await update.message.reply_text("Formato inválido. Use: 2h, 30min, etc.")
            return
        
        value = int(match.group(1))
        unit = match.group(2) or "min"
        
        if unit == "h":
            minutes = value * 60
        else:
            minutes = value
        
        processor = get_alert_processor(self.send_message)
        processor.silence(minutes)
        
        until = utcnow() + timedelta(minutes=minutes)
        await update.message.reply_text(
            f"🔕 Alertas silenciados por {minutes} minutos.\n"
            f"Volta às {until.strftime('%H:%M')} UTC."
        )
    
    async def cmd_ativar(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler do comando /ativar."""
        if not self._is_authorized(update.effective_chat.id):
            return
        
        processor = get_alert_processor(self.send_message)
        processor.unsilence()
        
        await update.message.reply_text("🔔 Alertas reativados!")
    
    async def cmd_pausartudo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler do comando /pausartudo."""
        if not self._is_authorized(update.effective_chat.id):
            return
        
        self.db.set_config("alertas_ativos", False)
        await update.message.reply_text(
            "⏸️ TODOS os alertas pausados.\n"
            "Use /despausar para voltar."
        )
    
    async def cmd_despausar(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler do comando /despausar."""
        if not self._is_authorized(update.effective_chat.id):
            return
        
        self.db.set_config("alertas_ativos", True)
        self.db.set_config("silenciado_ate", None)
        await update.message.reply_text("▶️ Alertas retomados!")
    
    async def cmd_filtrar(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler do comando /filtrar [metais]."""
        if not self._is_authorized(update.effective_chat.id):
            return
        
        args = context.args
        if not args:
            await update.message.reply_text(
                "Uso: /filtrar [metais]\n"
                "Exemplo: /filtrar XAU XAG\n"
                "Use /filtrar todos para receber todos."
            )
            return
        
        if args[0].lower() == "todos":
            processor = get_alert_processor(self.send_message)
            processor.set_filter([])
            await update.message.reply_text("🔔 Alertas de TODOS os metais ativados.")
        else:
            metals = []
            invalid = []
            for arg in args:
                resolved = resolve_metal(arg)
                if resolved:
                    metals.append(resolved)
                else:
                    invalid.append(arg)
            
            if not metals:
                await update.message.reply_text(
                    "Nenhum metal válido especificado.\n"
                    "Use código (XAU, XAG) ou nome (ouro, prata, cobre...)."
                )
                return
            
            processor = get_alert_processor(self.send_message)
            processor.set_filter(metals)
            nomes = [formato_metal(m) for m in metals]
            msg = f"🔔 Alertas filtrados para: {', '.join(nomes)}"
            if invalid:
                msg += f"\n⚠️ Não reconhecidos: {', '.join(invalid)}"
            await update.message.reply_text(msg)
    
    async def cmd_timezone(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler do comando /timezone [UTC offset]."""
        if not self._is_authorized(update.effective_chat.id):
            return
        
        args = context.args
        if not args:
            current = self.db.get_config("timezone", -3)
            await update.message.reply_text(
                f"Timezone atual: UTC{current:+d}\n"
                f"Uso: /timezone [offset]\n"
                f"Exemplo: /timezone -3 (Brasil) ou /timezone +1 (Espanha)"
            )
            return
        
        try:
            offset = int(args[0])
            if -12 <= offset <= 14:
                self.db.set_config("timezone", offset)
                await update.message.reply_text(f"🕐 Timezone alterado para UTC{offset:+d}")
            else:
                await update.message.reply_text("Offset deve estar entre -12 e +14.")
        except ValueError:
            await update.message.reply_text("Formato inválido. Use: /timezone -3")
    
    # =========================================================================
    # COMANDOS DE INTERAÇÃO
    # =========================================================================
    
    async def cmd_buscarmais(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler do comando /buscarmais."""
        if not self._is_authorized(update.effective_chat.id):
            return
        
        args = context.args
        topic = " ".join(args) if args else None
        
        # Se for resposta a uma mensagem
        if update.message.reply_to_message:
            original_text = update.message.reply_to_message.text
            
            await update.message.reply_text("🔍 Analisando...")
            
            prompt = f"""Analise este alerta e forneça mais contexto detalhado{f' sobre {topic}' if topic else ''}:

{original_text}

Inclua:
1. Contexto histórico relevante
2. Correlações importantes
3. Possíveis implicações
4. O que observar a seguir"""
            
            response = await self.llm.generate(prompt, task_type="raciocinio")
            
            if response:
                await update.message.reply_text(f"📊 <b>Análise Detalhada</b>\n\n{response}", parse_mode="HTML")
            else:
                await update.message.reply_text("Não foi possível gerar análise no momento.")
        else:
            await update.message.reply_text(
                "Responda a um alerta com /buscarmais para mais detalhes.\n"
                "Exemplo: responda a um alerta e digite /buscarmais liquidações"
            )
    
    async def cmd_significado(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler do comando /significado [termo]."""
        if not self._is_authorized(update.effective_chat.id):
            return
        
        args = context.args
        if not args:
            await update.message.reply_text(
                "Uso: /significado [termo]\n"
                "Exemplo: /significado contango"
            )
            return
        
        term = " ".join(args)
        
        await update.message.reply_text(f"📚 Buscando significado de '{term}'...")
        
        explanation = await self.llm.explain_term(term)
        
        if explanation:
            msg = self.formatter.format_significado(term, explanation)
            await update.message.reply_text(msg)
        else:
            await update.message.reply_text("Não foi possível obter explicação no momento.")
    
    # =========================================================================
    # SETUP
    # =========================================================================
    
    def setup_handlers(self, app: Application):
        """Configura handlers de comandos."""
        # Gerais
        app.add_handler(CommandHandler("start", self.cmd_start))
        app.add_handler(CommandHandler("comandos", self.cmd_comandos))
        app.add_handler(CommandHandler("status", self.cmd_status))
        app.add_handler(CommandHandler("config", self.cmd_config))
        app.add_handler(CommandHandler("teste", self.cmd_teste))
        app.add_handler(CommandHandler("erros", self.cmd_erros))
        
        # Dados
        app.add_handler(CommandHandler("ativos", self.cmd_ativos))
        app.add_handler(CommandHandler("preco", self.cmd_preco))
        app.add_handler(CommandHandler("resumo", self.cmd_resumo))
        app.add_handler(CommandHandler("cot", self.cmd_cot))
        app.add_handler(CommandHandler("digest", self.cmd_digest))
        app.add_handler(CommandHandler("agenda", self.cmd_agenda))
        
        # Controle
        app.add_handler(CommandHandler("silenciar", self.cmd_silenciar))
        app.add_handler(CommandHandler("ativar", self.cmd_ativar))
        app.add_handler(CommandHandler("pausartudo", self.cmd_pausartudo))
        app.add_handler(CommandHandler("despausar", self.cmd_despausar))
        app.add_handler(CommandHandler("filtrar", self.cmd_filtrar))
        app.add_handler(CommandHandler("timezone", self.cmd_timezone))
        
        # Interação
        app.add_handler(CommandHandler("buscarmais", self.cmd_buscarmais))
        app.add_handler(CommandHandler("significado", self.cmd_significado))
    
    async def start(self):
        """Inicia o bot."""
        if not TELEGRAM_BOT_TOKEN:
            raise ValueError("TELEGRAM_BOT_TOKEN não configurado")
        
        self.app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        self.bot = self.app.bot
        
        self.setup_handlers(self.app)
        
        # Inicializar processor de alertas
        get_alert_processor(self.send_message)
        
        logger.info("Bot Telegram iniciado")
        
        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling()
    
    async def stop(self):
        """Para o bot."""
        if self.app:
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()
        
        await self.llm.close()
        await self.price_collector.close()
        await self.macro.close()
        await self.institutional.close()
        
        logger.info("Bot Telegram parado")


# Singleton
_bot: Optional[TelegramBot] = None


def get_telegram_bot() -> TelegramBot:
    """Retorna instância singleton do bot Telegram."""
    global _bot
    if _bot is None:
        _bot = TelegramBot()
    return _bot
