"""
OpusDeiTradeMetaL - Formatador de Mensagens
=============================================
Formata alertas e mensagens para envio no Telegram.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any

from config.settings import METAIS, formato_metal, AlertLevel
from utils.time_utils import (
    format_timestamp_all_zones, format_price, format_percent,
    format_large_number, format_change_emoji
)


class MessageFormatter:
    """Formatador de mensagens do bot."""
    
    @staticmethod
    def format_price_alert(level: AlertLevel, metal: str, current_price: float,
                           change_percent: float, change_value: float,
                           timeframe_minutes: int, context: Dict = None) -> str:
        """Formata alerta de movimento de preço."""
        metal_info = METAIS.get(metal.upper())
        emoji = metal_info.emoji if metal_info else "📊"
        
        direction = "📈" if change_percent > 0 else "📉"
        movimento = "Alta" if change_percent > 0 else "Queda"
        
        if timeframe_minutes <= 15:
            timeframe_text = "15min"
        elif timeframe_minutes <= 60:
            timeframe_text = "1h"
        else:
            timeframe_text = "24h"
        
        msg = f"""{level.value} {movimento.upper()} | {formato_metal(metal)}

{direction} {movimento} de {format_percent(abs(change_percent))} em {timeframe_text}
💰 Preço: {format_price(current_price)}
"""
        if context:
            msg += "\n📊 Contexto:\n"
            if "dxy" in context:
                msg += f"├─ DXY: {context['dxy']}\n"
            if "yields" in context:
                msg += f"├─ Yields 10Y: {context['yields']}\n"
            if "liquidations" in context:
                msg += f"└─ Liquidações: {format_large_number(context['liquidations'])}\n"
        
        msg += f"\n{format_timestamp_all_zones()}"
        return msg
    
    @staticmethod
    def format_technical_proximity_alert(metal: str, current_price: float,
                                          level_name: str, level_value: float,
                                          level_type: str, distance_percent: float,
                                          context: Dict = None) -> str:
        """Formata alerta de proximidade de nível técnico."""
        tipo = "RESISTÊNCIA" if level_type == "resistencia" else "SUPORTE"
        
        msg = f"""📍 TÉCNICO | {formato_metal(metal)} (Intraday)

⚠️ Aproximando de zona de {tipo.lower()}

Preço atual: {format_price(current_price)}
Zona S/R: {format_price(level_value)}
Distância: {format_percent(distance_percent, include_sign=False)}

Por que é relevante:
"""
        if context:
            if context.get("touches"):
                msg += f"├─ {context['touches']} toques nos últimos dias\n"
            if context.get("high_volume"):
                msg += f"├─ Alto volume nessa região\n"
            if context.get("coincides"):
                msg += f"├─ Coincide com {context['coincides']}\n"
            if context.get("vwap"):
                msg += f"└─ VWAP do dia: {format_price(context['vwap'])}\n"
        
        msg += f"""
Cenários:
├─ Segura: bounce para {format_price(context.get('target_up', level_value * 1.01))}
└─ Perde: próximo {tipo.lower()} {format_price(context.get('target_down', level_value * 0.99))}

{format_timestamp_all_zones()}"""
        return msg
    
    @staticmethod
    def format_technical_break_alert(metal: str, current_price: float,
                                      level_name: str, level_value: float,
                                      direction: str) -> str:
        """Formata alerta de rompimento de nível."""
        if direction == "up":
            emoji = "🚀"
            acao = "ROMPEU RESISTÊNCIA"
        else:
            emoji = "💥"
            acao = "PERDEU SUPORTE"
        
        msg = f"""🔴 TÉCNICO | {formato_metal(metal)}

{emoji} {acao}

Preço: {format_price(current_price)}
Nível rompido: {level_name} = {format_price(level_value)}

⚠️ Aguardar confirmação de fechamento

{format_timestamp_all_zones()}"""
        return msg
    
    @staticmethod
    def format_cot_alert(metal: str, cot_data: Dict) -> str:
        """Formata alerta do COT Report."""
        mm_net = cot_data.get('mm_net', 0)
        mm_change = cot_data.get('mm_change', 0)
        comm_net = cot_data.get('comm_net', 0)
        
        mm_emoji = "🔺" if mm_change > 0 else "🔻"
        comm_emoji = "🔺" if cot_data.get('comm_change', 0) > 0 else "🔻"
        
        msg = f"""🏦 INSTITUCIONAL | {formato_metal(metal)}

📊 COT Report (dados de {cot_data.get('report_date', 'N/A')})

Managed Money:
├─ Long: {cot_data.get('mm_long', 0):,} ({cot_data.get('mm_long_change', 0):+,})
├─ Short: {cot_data.get('mm_short', 0):,} ({cot_data.get('mm_short_change', 0):+,})
└─ Net: {mm_net:+,} ({mm_change:+,}) {mm_emoji}

Comerciais:
├─ Long: {cot_data.get('comm_long', 0):,}
├─ Short: {cot_data.get('comm_short', 0):,}
└─ Net: {comm_net:+,} {comm_emoji}
"""
        if cot_data.get('signal'):
            msg += f"\n⚠️ Sinal: {cot_data['signal']}\n"
        
        msg += f"\n{format_timestamp_all_zones()}"
        return msg
    
    @staticmethod
    def format_etf_flow_alert(etf: str, metal: str, flow_data: Dict) -> str:
        """Formata alerta de fluxo de ETF."""
        flow = flow_data.get('flow_tons', 0)
        direction = "📈 INFLOW" if flow > 0 else "📉 OUTFLOW"
        
        msg = f"""📦 ETF | {formato_metal(metal)}

{direction} em {etf}

Holdings: {flow_data.get('holdings_tons', 0):,.1f} ton
Fluxo: {abs(flow):,.2f} ton ({format_large_number(flow_data.get('flow_usd', 0))})

{format_timestamp_all_zones()}"""
        return msg
    
    @staticmethod
    def format_whale_alert(movement: Dict) -> str:
        """Formata alerta de whale on-chain."""
        type_map = {
            "mint": "🟢 Cunhagem",
            "burn": "🔴 Queima",
            "exchange_deposit": "📥 Depósito Exchange",
            "exchange_withdrawal": "📤 Saque Exchange",
            "transfer": "↔️ Transferência",
        }
        movement_type = type_map.get(movement.get('type', ''), '↔️ Transferência')
        
        msg = f"""🐋 WHALE ALERT | {movement.get('token', 'PAXG')}

{movement_type}

Quantidade: {movement.get('amount', 0):,.2f} oz
Valor: {format_large_number(movement.get('value_usd', 0))}

🔗 etherscan.io/tx/{movement.get('tx_hash', '')[:16]}...

{format_timestamp_all_zones()}"""
        return msg
    
    @staticmethod
    def format_calendar_7d(event: Dict) -> str:
        """Formata lembrete 7 dias antes."""
        msg = f"""📅 AGENDA | Próxima semana

🏛️ {event.get('title', 'Evento')}
📆 {event.get('event_time', '')}

Impacto esperado: {'🔴 Alto' if event.get('impact') == 'high' else '🟡 Médio'}

{format_timestamp_all_zones()}"""
        return msg
    
    @staticmethod
    def format_calendar_1d(event: Dict, impact_analysis: str = None) -> str:
        """Formata lembrete 1 dia antes com análise de impacto."""
        msg = f"""📅 AMANHÃ | {event.get('title', 'Evento')}

🏛️ {event.get('event_time', '')}

Contexto atual:
├─ Mercado espera: {event.get('forecast', 'N/A')}
└─ Anterior: {event.get('previous', 'N/A')}
"""
        if impact_analysis:
            msg += f"\n⚠️ POSSÍVEL IMPACTO:\n{impact_analysis}\n"
        
        msg += f"\n{format_timestamp_all_zones()}"
        return msg
    
    @staticmethod
    def format_calendar_1h(event: Dict, market_context: Dict = None) -> str:
        """Formata lembrete 1 hora antes."""
        msg = f"""⏰ EM 1 HORA | {event.get('title', 'Evento')}

🏛️ {event.get('event_time', '')}
"""
        if market_context:
            msg += f"""
Posição atual do mercado:
├─ XAU: {format_price(market_context.get('xau_price', 0))}
├─ DXY: {market_context.get('dxy', 'N/A')}
└─ Volatilidade: {market_context.get('volatility', 'Normal')}
"""
        msg += f"\n{format_timestamp_all_zones()}"
        return msg
    
    @staticmethod
    def format_calendar_result(event: Dict, result_analysis: str = None) -> str:
        """Formata resultado de evento."""
        actual = event.get('actual', 'N/A')
        forecast = event.get('forecast', 'N/A')
        
        try:
            a = float(str(actual).replace('%', '').replace(',', '.'))
            f = float(str(forecast).replace('%', '').replace(',', '.'))
            if a > f:
                emoji = "📈"
                desc = "ACIMA do esperado"
            elif a < f:
                emoji = "📉"
                desc = "ABAIXO do esperado"
            else:
                emoji = "➡️"
                desc = "Em linha"
        except:
            emoji = "📊"
            desc = "Resultado"
        
        msg = f"""🔴 {event.get('event_type', 'MACRO')} | {desc}

Atual: {actual} {emoji}
Esperado: {forecast}
Anterior: {event.get('previous', 'N/A')}
"""
        if result_analysis:
            msg += f"\n{result_analysis}\n"
        
        msg += f"\n{format_timestamp_all_zones()}"
        return msg
    
    @staticmethod
    def format_swiss_flow(data: Dict) -> str:
        """Formata dados de fluxo físico suíço."""
        msg = f"""📦 SUÍÇA | Fluxo Físico XAU Ouro

Exportações {data.get('month', 'N/A')}: {data.get('total', 0):.1f} ton

Destinos:
"""
        for dest in data.get('destinations', [])[:5]:
            msg += f"├─ {dest['flag']} {dest['country']}: {dest['tons']:.1f} ton ({dest['percent']:.0f}%)\n"
        
        msg += f"""
Origem:
"""
        for orig in data.get('origins', [])[:3]:
            msg += f"├─ {orig['flag']} {orig['country']}: {orig['tons']:.1f} ton\n"
        
        msg += f"""
Tendência: {data.get('trend', 'N/A')}
Sinal: {data.get('signal', 'N/A')}

⏱ Dados de {data.get('data_date', 'N/A')}"""
        return msg
    
    @staticmethod
    def format_digest_asia(prices: Dict, highlights: List[str]) -> str:
        """Formata digest do fechamento da Ásia."""
        msg = "🌏 DIGEST | Fechamento Ásia\n\n"
        
        for metal in ["XAU", "XAG"]:
            if metal in prices:
                p = prices[metal]
                emoji = METAIS[metal].emoji
                change = format_percent(p.get('change', 0))
                msg += f"{emoji} {formato_metal(metal)}: {format_price(p['price'])} ({change})\n"
        
        if highlights:
            msg += "\n📌 Destaques:\n"
            for h in highlights[:4]:
                msg += f"├─ {h}\n"
        
        msg += f"\n{format_timestamp_all_zones()}"
        return msg
    
    @staticmethod
    def format_digest_eu_us(prices: Dict, highlights: List[str], upcoming: List[str] = None) -> str:
        """Formata digest do fechamento EU/US."""
        msg = "🌍 DIGEST | Fechamento EU/US\n\n"
        
        for metal in ["XAU", "XAG", "XPT", "XCU"]:
            if metal in prices:
                p = prices[metal]
                emoji = METAIS[metal].emoji
                change = format_percent(p.get('change', 0))
                msg += f"{emoji} {formato_metal(metal)}: {format_price(p['price'])} ({change})\n"
        
        if highlights:
            msg += "\n📌 Destaques:\n"
            for h in highlights[:4]:
                msg += f"├─ {h}\n"
        
        if upcoming:
            msg += "\n📅 Amanhã:\n"
            for u in upcoming[:3]:
                msg += f"├─ {u}\n"
        
        msg += f"\n{format_timestamp_all_zones()}"
        return msg
    
    @staticmethod
    def format_digest_weekly(data: Dict) -> str:
        """Formata digest semanal."""
        msg = "📊 DIGEST | Resumo Semanal\n\n"
        
        msg += "Performance da semana:\n"
        for metal, perf in data.get('performance', {}).items():
            emoji = METAIS.get(metal, {}).emoji if metal in METAIS else "📊"
            msg += f"{emoji} {formato_metal(metal)}: {format_percent(perf)}\n"
        
        if data.get('cot_highlights'):
            msg += "\n🏦 COT Highlights:\n"
            for h in data['cot_highlights'][:3]:
                msg += f"├─ {h}\n"
        
        if data.get('next_week'):
            msg += "\n📅 Próxima semana:\n"
            for e in data['next_week'][:5]:
                msg += f"├─ {e}\n"
        
        msg += f"\n{format_timestamp_all_zones()}"
        return msg
    
    @staticmethod
    def format_ativos_response(prices: Dict) -> str:
        """Formata resposta do comando /ativos."""
        msg = "📊 ATIVOS | Preços Atuais\n\n"
        
        msg += "🥇 PRECIOSOS\n"
        for metal in ["XAU", "XAG", "XPT", "XPD"]:
            if metal in prices:
                p = prices[metal]
                emoji = METAIS[metal].emoji
                change = format_percent(p.get('change_percent', 0))
                msg += f"{emoji} {formato_metal(metal)}: {format_price(p['price'])} ({change})\n"
        
        msg += "\n⚙️ INDUSTRIAIS\n"
        for metal in ["XCU", "XAL", "XNI", "XPB", "XZN", "XSN"]:
            if metal in prices:
                p = prices[metal]
                emoji = METAIS[metal].emoji
                change = format_percent(p.get('change_percent', 0))
                msg += f"{emoji} {formato_metal(metal)}: {format_price(p['price'])} ({change})\n"
        
        msg += "\n☢️ ESTRATÉGICOS\n"
        for metal in ["UX", "FE"]:
            if metal in prices:
                p = prices[metal]
                emoji = METAIS[metal].emoji if metal in METAIS else "📊"
                change = format_percent(p.get('change_percent', 0))
                msg += f"{emoji} {formato_metal(metal)}: {format_price(p['price'])} ({change})\n"
        
        msg += f"\n{format_timestamp_all_zones()}"
        return msg
    
    @staticmethod
    def format_resumo_metal(metal: str, data: Dict) -> str:
        """Formata resumo completo de um metal."""
        m = METAIS.get(metal.upper())
        emoji = m.emoji if m else "📊"
        
        msg = f"""{emoji} RESUMO | {formato_metal(metal)}

💰 PREÇO
├─ Atual: {format_price(data.get('price', 0))}
├─ Variação 24h: {format_percent(data.get('change_24h', 0))}
├─ Máx 24h: {format_price(data.get('high_24h', 0))}
└─ Mín 24h: {format_price(data.get('low_24h', 0))}

📊 TÉCNICO
├─ MM50: {format_price(data.get('sma_50', 0))}
├─ MM200: {format_price(data.get('sma_200', 0))}
├─ Pivot: {format_price(data.get('pivot', 0))}
├─ R1: {format_price(data.get('r1', 0))}
└─ S1: {format_price(data.get('s1', 0))}
"""
        if data.get('cot'):
            msg += f"""
🏦 INSTITUCIONAL (COT)
├─ MM Net: {data['cot'].get('mm_net', 0):+,}
└─ MM Change: {data['cot'].get('mm_change', 0):+,}
"""
        if data.get('etf'):
            msg += f"""
📦 ETF
├─ Holdings: {data['etf'].get('holdings', 0):,.1f} ton
└─ Fluxo: {data['etf'].get('flow', 0):+,.2f} ton
"""
        if data.get('news'):
            msg += f"""
📰 Últimas notícias:
├─ {data['news'][0] if len(data.get('news', [])) > 0 else 'N/A'}
"""
        msg += f"\n{format_timestamp_all_zones()}"
        return msg
    
    @staticmethod
    def format_status(stats: Dict) -> str:
        """Formata resposta do comando /status."""
        msg = f"""🤖 STATUS | OpusDeiTradeMetaL

⏱ Uptime: {stats.get('uptime', 'N/A')}
📡 Conexões: {'✅' if stats.get('connections_ok') else '❌'}
💾 RAM: {stats.get('ram_mb', 0):.1f} MB
📊 Último alerta: {stats.get('last_alert', 'N/A')}

🔌 FONTES
├─ Metals.live: {'✅' if stats.get('metals_live') else '❌'}
├─ FRED: {'✅' if stats.get('fred') else '❌'}
├─ Etherscan: {'✅' if stats.get('etherscan') else '❌'}
└─ OpenRouter: {'✅' if stats.get('openrouter') else '❌'}

📈 STATS (24h)
├─ Alertas enviados: {stats.get('alerts_24h', 0)}
├─ Calls LLM: {stats.get('llm_calls', 0)}/{stats.get('llm_max', 1000)}
└─ Erros: {stats.get('errors_24h', 0)}
"""
        return msg
    
    @staticmethod
    def format_config(config: Dict) -> str:
        """Formata resposta do comando /config."""
        filtros = config.get('filtros', ['todos'])
        filtros_str = ', '.join(filtros) if filtros else 'todos'
        
        msg = f"""⚙️ CONFIG | Suas configurações

🕐 Timezone: UTC{config.get('timezone', -3):+d}
🔔 Alertas: {'✅ Ativos' if config.get('alertas_ativos', True) else '❌ Pausados'}
📊 Filtros: {filtros_str}

📬 DIGESTS
├─ Ásia: {'✅' if config.get('digest_asia', True) else '❌'}
├─ EU/US: {'✅' if config.get('digest_eu_us', True) else '❌'}
└─ Semanal: {'✅' if config.get('digest_weekly', True) else '❌'}

🤖 LLM
├─ Calls hoje: {config.get('llm_calls_today', 0)}
└─ Restante: {config.get('llm_remaining', 1000)}

Use /comandos para ver opções de configuração."""
        return msg
    
    @staticmethod
    def format_agenda(events: List[Dict], days: int = 7) -> str:
        """Formata resposta do comando /agenda."""
        msg = f"📅 AGENDA | Próximos {days} dias\n\n"
        
        if not events:
            msg += "Nenhum evento relevante no período."
        else:
            for event in events[:15]:
                impact = "🔴" if event.get('impact') == 'high' else "🟡"
                msg += f"{impact} {event.get('event_time', 'N/A')}\n"
                msg += f"   {event.get('title', 'Evento')}\n\n"
        
        return msg
    
    @staticmethod
    def format_erros(errors: List[Dict]) -> str:
        """Formata resposta do comando /erros."""
        total = len(errors)
        msg = f"⚠️ ERROS | Últimos {total}\n\n"
        
        if not errors:
            msg += "✅ Nenhum erro registrado!"
        else:
            for err in errors[:10]:
                msg += f"├─ [{err.get('source', 'N/A')}] {err.get('message', 'Erro')[:50]}\n"
                msg += f"   {err.get('created_at', 'N/A')}\n"
        
        return msg
    
    @staticmethod
    def format_significado(term: str, explanation: str) -> str:
        """Formata explicação de termo."""
        msg = f"""📚 SIGNIFICADO | {term}

{explanation}
"""
        return msg
    
    @staticmethod
    def format_help() -> str:
        """Formata lista de comandos."""
        return """🤖 COMANDOS | OpusDeiTradeMetaL

📊 DADOS
/ativos - Preços atuais dos 12 metais
/preco [metal] - Preço específico
/resumo [metal] - Resumo completo
/cot [metal] - Último COT Report
/etf [metal] - Flows ETFs
/digest - Gerar digest agora

📅 CALENDÁRIO
/agenda [dias] - Próximos eventos

⚙️ CONTROLE
/silenciar [tempo] - Pausar (ex: 2h)
/ativar - Reativar alertas
/filtrar [metais] - Filtrar alertas
/timezone [UTC] - Alterar fuso

🔧 SISTEMA
/status - Estado do sistema
/config - Suas configurações
/teste - Testar conexões
/erros - Últimos erros

💡 INTERAÇÃO
Responda a um alerta com:
/buscarmais - Mais detalhes
/significado [termo] - Explicar termo"""
