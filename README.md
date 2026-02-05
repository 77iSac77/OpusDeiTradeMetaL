# 🥇 OpusDeiTradeMetaL

Sistema de alertas em tempo real para **12 metais preciosos, industriais e estratégicos** via Telegram.

## 📊 Metais Monitorados

| Tipo | Metais |
|------|--------|
| 🥇 **Preciosos** | XAU Ouro, XAG Prata, XPT Platina, XPD Paládio |
| ⚙️ **Industriais** | XCU Cobre, XAL Alumínio, XNI Níquel, XPB Chumbo, XZN Zinco, XSN Estanho |
| ☢️ **Estratégicos** | UX Urânio, FE Minério de Ferro |

## 🚀 Features

### Alertas em Tempo Real
- 🔴 **Crítico**: Movimento >2% em 15 min
- 🟡 **Importante**: Movimento >1% em 1 hora
- 🟢 **Info**: Movimento >0.5% em 24h

### Análise Técnica
- Pivot Points diários (PP, R1-R3, S1-S3)
- VWAP (Volume Weighted Average Price)
- Médias Móveis (SMA 50, SMA 200)
- Zonas de alto volume
- Múltiplos toques (suportes/resistências confirmados)

### Dados Institucionais
- 🏦 COT Report (CFTC) - Posicionamento de Managed Money
- 📦 ETF Flows (GLD, SLV, IAU, PPLT)
- 🐋 Whale Alerts on-chain (PAXG, XAUT)

### Calendário Econômico
- FOMC, ECB, CPI, NFP, GDP, PMI
- Lembretes: 7 dias, 1 dia, 1 hora antes
- Análise de impacto por cenário

### Digests Automáticos
- 🌏 Ásia: 07:30 UTC (fechamento Shanghai)
- 🌍 EU/US: 21:30 UTC (fechamento COMEX)
- 📊 Semanal: Sábado à noite

## 🛠️ Comandos Telegram

### Dados
```
/ativos        - Preços atuais dos 12 metais
/preco [metal] - Preço específico (ex: /preco XAU)
/resumo [metal]- Resumo completo
/cot [metal]   - Último COT Report
/digest        - Gerar digest agora
/agenda [dias] - Próximos eventos
```

### Controle
```
/silenciar [tempo] - Pausar alertas (ex: /silenciar 2h)
/ativar            - Reativar alertas
/filtrar [metais]  - Filtrar (ex: /filtrar XAU XAG)
/timezone [UTC]    - Alterar fuso (ex: /timezone -3)
```

### Sistema
```
/status   - Estado do sistema
/config   - Suas configurações
/teste    - Testar conexões
/erros    - Últimos erros
/comandos - Lista de comandos
```

### Interação
```
Responda a um alerta com:
/buscarmais          - Mais detalhes
/buscarmais [tema]   - Detalhar tema específico
/significado [termo] - Explicar termo
```

### Todos Comandos
```

/start
/status
/config
/teste
/erros
/comandos

/ativos
/preco [metal]
/resumo [metal]
/cot [metal]
/etf [metal]
/digest
/agenda [dias]

/silenciar [tempo]
/ativar
/pausartudo
/despausar
/filtrar [metais]
/filtrar todos
/timezone [UTC]
/confluencia [1|2|3]

/buscarmais
/buscarmais [tema]
/significado [termo]


## 📦 Deploy

### Variáveis de Ambiente

```bash
# Obrigatórias
TELEGRAM_BOT_TOKEN=xxx    # @BotFather
OPENROUTER_API_KEY=xxx    # openrouter.ai/keys

# Recomendadas
ETHERSCAN_API_KEY=xxx     # etherscan.io/myapikey
FRED_API_KEY=xxx          # fred.stlouisfed.org
```

### Koyeb (Free Tier)

1. Crie conta em [koyeb.com](https://koyeb.com)
2. Conecte seu repositório GitHub
3. Configure variáveis de ambiente
4. Deploy automático!

### Docker Local

```bash
# Build
docker build -t opusdei-metal .

# Run
docker run -d \
  -e TELEGRAM_BOT_TOKEN=xxx \
  -e OPENROUTER_API_KEY=xxx \
  -e ETHERSCAN_API_KEY=xxx \
  -v opusdei_data:/app/data \
  opusdei-metal
```

### Python Direto

```bash
# Instalar dependências
pip install -r requirements.txt

# Exportar variáveis
export TELEGRAM_BOT_TOKEN=xxx
export OPENROUTER_API_KEY=xxx

# Rodar
python main.py
```

## 🏗️ Arquitetura

```
OpusDeiTradeMetaL/
├── main.py              # Entry point
├── config/
│   └── settings.py      # Configurações globais
├── collectors/
│   ├── prices.py        # Coleta preços (Metals.live, Kitco, Yahoo)
│   ├── technical.py     # Cálculo níveis técnicos
│   ├── macro.py         # Dados macro (FRED, calendário)
│   └── institutional.py # COT, ETFs, On-chain
├── processors/
│   └── alerts.py        # Processamento e envio de alertas
├── bot/
│   ├── handler.py       # Handlers Telegram
│   └── formatter.py     # Formatação de mensagens
├── storage/
│   └── database.py      # SQLite para persistência
└── utils/
    ├── time_utils.py    # Formatação de tempo/números
    └── llm_client.py    # Cliente OpenRouter
```

## 🤖 LLMs (OpenRouter - Grátis)

Pool de 5 modelos com fallback automático:

1. **Gemini 2.0 Flash** - Principal (128k contexto)
2. **Nemotron 3 Nano** - Análise (256k contexto)
3. **DeepSeek R1 Distill** - Raciocínio (64k contexto)
4. **Gemini 2.5 Flash** - Backup (128k contexto)
5. **LFM2.5 Thinking** - Último recurso (32k contexto)

## 📈 Fontes de Dados

| Categoria | Fontes |
|-----------|--------|
| **Preços** | Metals.live, Kitco, Yahoo Finance |
| **Macro US** | FRED API, Investing.com |
| **Institucional** | CFTC COT, SEC EDGAR, ETF Holdings |
| **On-chain** | Etherscan (PAXG, XAUT) |
| **China** | SGE, SHFE |
| **Físico** | COMEX, LBMA, Perth Mint |

## 📄 Licença

MIT License - Use livremente!

---

Feito com 🥇 por **OpusDeiTrade**
