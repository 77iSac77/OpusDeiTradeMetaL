"""
OpusDeiTradeMetaL - Módulo de Persistência SQLite
==================================================
Gerencia armazenamento de configurações, cache e histórico.
"""

import sqlite3
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


class Database:
    """Gerenciador de banco de dados SQLite."""
    
    def __init__(self, db_path: str = "data/opusdei.db"):
        """
        Inicializa conexão com banco de dados.
        
        Args:
            db_path: Caminho para arquivo SQLite
        """
        self.db_path = db_path
        
        # Criar diretório se não existir
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Inicializar schema
        self._init_schema()
    
    @contextmanager
    def get_connection(self):
        """Context manager para conexões."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def _init_schema(self):
        """Cria tabelas se não existirem."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Configurações do usuário
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_config (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Cache de respostas LLM
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS llm_cache (
                    hash TEXT PRIMARY KEY,
                    prompt TEXT NOT NULL,
                    response TEXT NOT NULL,
                    model TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP
                )
            """)
            
            # Alertas enviados (para evitar duplicatas)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS alerts_sent (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    alert_type TEXT NOT NULL,
                    metal TEXT,
                    content_hash TEXT NOT NULL,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(content_hash)
                )
            """)
            
            # Histórico de preços (para análise técnica)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS price_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    metal TEXT NOT NULL,
                    price REAL NOT NULL,
                    volume REAL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Níveis técnicos calculados
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS technical_levels (
                    metal TEXT NOT NULL,
                    level_type TEXT NOT NULL,
                    level_name TEXT NOT NULL,
                    value REAL NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (metal, level_type, level_name)
                )
            """)
            
            # Contadores (LLM calls, alertas, etc)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS counters (
                    key TEXT PRIMARY KEY,
                    count INTEGER DEFAULT 0,
                    reset_date DATE
                )
            """)
            
            # Log de erros
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS error_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    error_type TEXT NOT NULL,
                    source TEXT,
                    message TEXT,
                    action_taken TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Eventos do calendário
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS calendar_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    event_time TIMESTAMP NOT NULL,
                    impact TEXT,
                    notified_7d BOOLEAN DEFAULT FALSE,
                    notified_1d BOOLEAN DEFAULT FALSE,
                    notified_1h BOOLEAN DEFAULT FALSE,
                    notified_result BOOLEAN DEFAULT FALSE
                )
            """)
            
            # Índices para performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_alerts_hash ON alerts_sent(content_hash)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_metal ON price_history(metal, timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_errors_time ON error_log(created_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_calendar_time ON calendar_events(event_time)")
            
            conn.commit()
            logger.info("Schema do banco de dados inicializado")
    
    # =========================================================================
    # CONFIGURAÇÕES DO USUÁRIO
    # =========================================================================
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """
        Obtém configuração do usuário.
        
        Args:
            key: Chave da configuração
            default: Valor padrão se não existir
        
        Returns:
            Valor da configuração (deserializado de JSON)
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM user_config WHERE key = ?", (key,))
            row = cursor.fetchone()
            
            if row:
                try:
                    return json.loads(row["value"])
                except json.JSONDecodeError:
                    return row["value"]
            return default
    
    def set_config(self, key: str, value: Any):
        """
        Define configuração do usuário.
        
        Args:
            key: Chave da configuração
            value: Valor (será serializado para JSON)
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            json_value = json.dumps(value) if not isinstance(value, str) else value
            cursor.execute("""
                INSERT OR REPLACE INTO user_config (key, value, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, (key, json_value))
    
    def get_all_config(self) -> Dict[str, Any]:
        """Retorna todas as configurações."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT key, value FROM user_config")
            
            config = {}
            for row in cursor.fetchall():
                try:
                    config[row["key"]] = json.loads(row["value"])
                except json.JSONDecodeError:
                    config[row["key"]] = row["value"]
            return config
    
    # =========================================================================
    # CACHE LLM
    # =========================================================================
    
    def get_cached_response(self, prompt_hash: str) -> Optional[str]:
        """
        Busca resposta em cache.
        
        Args:
            prompt_hash: Hash do prompt
        
        Returns:
            Resposta em cache ou None
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT response FROM llm_cache 
                WHERE hash = ? AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
            """, (prompt_hash,))
            row = cursor.fetchone()
            return row["response"] if row else None
    
    def cache_response(self, prompt_hash: str, prompt: str, response: str, 
                       model: str, ttl_seconds: int = 3600):
        """
        Armazena resposta em cache.
        
        Args:
            prompt_hash: Hash do prompt
            prompt: Texto do prompt
            response: Resposta do LLM
            model: Nome do modelo
            ttl_seconds: Tempo de vida em segundos
        """
        expires_at = datetime.utcnow() + timedelta(seconds=ttl_seconds)
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO llm_cache (hash, prompt, response, model, created_at, expires_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
            """, (prompt_hash, prompt, response, model, expires_at))
    
    def clear_expired_cache(self):
        """Remove entradas expiradas do cache."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM llm_cache WHERE expires_at < CURRENT_TIMESTAMP")
            deleted = cursor.rowcount
            logger.info(f"Cache LLM: {deleted} entradas expiradas removidas")
    
    # =========================================================================
    # ALERTAS
    # =========================================================================
    
    def is_alert_sent(self, content_hash: str) -> bool:
        """
        Verifica se alerta já foi enviado (evita duplicatas).
        
        Args:
            content_hash: Hash do conteúdo do alerta
        
        Returns:
            True se já foi enviado
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM alerts_sent WHERE content_hash = ?", (content_hash,))
            return cursor.fetchone() is not None
    
    def mark_alert_sent(self, alert_type: str, content_hash: str, metal: str = None):
        """
        Marca alerta como enviado.
        
        Args:
            alert_type: Tipo do alerta
            content_hash: Hash do conteúdo
            metal: Código do metal (opcional)
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO alerts_sent (alert_type, metal, content_hash, sent_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """, (alert_type, metal, content_hash))
            except sqlite3.IntegrityError:
                pass  # Já existe
    
    def get_alerts_count_today(self) -> int:
        """Retorna quantidade de alertas enviados hoje."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) as count FROM alerts_sent 
                WHERE DATE(sent_at) = DATE('now')
            """)
            return cursor.fetchone()["count"]
    
    def cleanup_old_alerts(self, days: int = 7):
        """Remove alertas antigos."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM alerts_sent 
                WHERE sent_at < datetime('now', ?)
            """, (f"-{days} days",))
    
    # =========================================================================
    # HISTÓRICO DE PREÇOS
    # =========================================================================
    
    def add_price(self, metal: str, price: float, volume: float = None):
        """
        Adiciona preço ao histórico.
        
        Args:
            metal: Código do metal
            price: Preço
            volume: Volume (opcional)
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO price_history (metal, price, volume, timestamp)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, (metal.upper(), price, volume))
    
    def get_price_history(self, metal: str, hours: int = 24) -> List[Dict]:
        """
        Obtém histórico de preços.
        
        Args:
            metal: Código do metal
            hours: Horas de histórico
        
        Returns:
            Lista de dicts com price, volume, timestamp
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT price, volume, timestamp FROM price_history
                WHERE metal = ? AND timestamp > datetime('now', ?)
                ORDER BY timestamp ASC
            """, (metal.upper(), f"-{hours} hours"))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_price_at_time(self, metal: str, hours_ago: int) -> Optional[float]:
        """
        Obtém preço de X horas atrás.
        
        Args:
            metal: Código do metal
            hours_ago: Horas atrás
        
        Returns:
            Preço ou None
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT price FROM price_history
                WHERE metal = ? AND timestamp <= datetime('now', ?)
                ORDER BY timestamp DESC LIMIT 1
            """, (metal.upper(), f"-{hours_ago} hours"))
            row = cursor.fetchone()
            return row["price"] if row else None
    
    # =========================================================================
    # NÍVEIS TÉCNICOS
    # =========================================================================
    
    def update_technical_level(self, metal: str, level_type: str, 
                                level_name: str, value: float):
        """
        Atualiza nível técnico.
        
        Args:
            metal: Código do metal
            level_type: Tipo (long_term, short_term)
            level_name: Nome do nível (sma_50, pivot_r1, etc)
            value: Valor do nível
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO technical_levels 
                (metal, level_type, level_name, value, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (metal.upper(), level_type, level_name, value))
    
    def get_technical_levels(self, metal: str) -> Dict[str, float]:
        """
        Obtém todos os níveis técnicos de um metal.
        
        Args:
            metal: Código do metal
        
        Returns:
            Dict com level_name: value
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT level_name, value FROM technical_levels
                WHERE metal = ?
            """, (metal.upper(),))
            
            return {row["level_name"]: row["value"] for row in cursor.fetchall()}
    
    # =========================================================================
    # CONTADORES
    # =========================================================================
    
    def increment_counter(self, key: str) -> int:
        """
        Incrementa contador (reseta diariamente).
        
        Args:
            key: Nome do contador
        
        Returns:
            Novo valor
        """
        today = datetime.utcnow().date().isoformat()
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Verificar se precisa resetar
            cursor.execute("SELECT count, reset_date FROM counters WHERE key = ?", (key,))
            row = cursor.fetchone()
            
            if row and row["reset_date"] == today:
                # Incrementar
                new_count = row["count"] + 1
                cursor.execute(
                    "UPDATE counters SET count = ? WHERE key = ?",
                    (new_count, key)
                )
            else:
                # Resetar e começar em 1
                new_count = 1
                cursor.execute("""
                    INSERT OR REPLACE INTO counters (key, count, reset_date)
                    VALUES (?, ?, ?)
                """, (key, new_count, today))
            
            return new_count
    
    def get_counter(self, key: str) -> int:
        """Obtém valor do contador."""
        today = datetime.utcnow().date().isoformat()
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT count FROM counters WHERE key = ? AND reset_date = ?",
                (key, today)
            )
            row = cursor.fetchone()
            return row["count"] if row else 0
    
    # =========================================================================
    # LOG DE ERROS
    # =========================================================================
    
    def log_error(self, error_type: str, source: str, message: str, 
                  action_taken: str = None):
        """
        Registra erro no log.
        
        Args:
            error_type: Tipo do erro
            source: Origem do erro
            message: Mensagem de erro
            action_taken: Ação tomada (opcional)
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO error_log (error_type, source, message, action_taken, created_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (error_type, source, message, action_taken))
    
    def get_recent_errors(self, limit: int = 10) -> List[Dict]:
        """
        Obtém erros recentes.
        
        Args:
            limit: Quantidade máxima
        
        Returns:
            Lista de erros
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT error_type, source, message, action_taken, created_at
                FROM error_log
                ORDER BY created_at DESC
                LIMIT ?
            """, (limit,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_error_count_24h(self) -> int:
        """Retorna quantidade de erros nas últimas 24h."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) as count FROM error_log
                WHERE created_at > datetime('now', '-24 hours')
            """)
            return cursor.fetchone()["count"]
    
    # =========================================================================
    # CALENDÁRIO
    # =========================================================================
    
    def add_calendar_event(self, event_type: str, title: str, event_time: datetime,
                           description: str = None, impact: str = None):
        """
        Adiciona evento ao calendário.
        
        Args:
            event_type: Tipo (FOMC, CPI, etc)
            title: Título do evento
            event_time: Data/hora do evento
            description: Descrição (opcional)
            impact: Impacto esperado (opcional)
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO calendar_events 
                (event_type, title, description, event_time, impact)
                VALUES (?, ?, ?, ?, ?)
            """, (event_type, title, description, event_time, impact))
    
    def get_upcoming_events(self, days: int = 7) -> List[Dict]:
        """
        Obtém eventos próximos.
        
        Args:
            days: Dias à frente
        
        Returns:
            Lista de eventos
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM calendar_events
                WHERE event_time > CURRENT_TIMESTAMP
                AND event_time < datetime('now', ?)
                ORDER BY event_time ASC
            """, (f"+{days} days",))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def mark_event_notified(self, event_id: int, notification_type: str):
        """
        Marca evento como notificado.
        
        Args:
            event_id: ID do evento
            notification_type: Tipo (7d, 1d, 1h, result)
        """
        column = f"notified_{notification_type}"
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                UPDATE calendar_events SET {column} = TRUE WHERE id = ?
            """, (event_id,))
    
    # =========================================================================
    # UTILIDADES
    # =========================================================================
    
    def get_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas do banco de dados."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            stats = {}
            
            # Tamanho do banco
            stats["db_size_mb"] = os.path.getsize(self.db_path) / (1024 * 1024)
            
            # Contagens
            for table in ["alerts_sent", "price_history", "llm_cache", "error_log"]:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                stats[f"{table}_count"] = cursor.fetchone()["count"]
            
            return stats
    
    def vacuum(self):
        """Compacta banco de dados."""
        with self.get_connection() as conn:
            conn.execute("VACUUM")
            logger.info("Banco de dados compactado")


# Singleton para uso global
_db_instance: Optional[Database] = None


def get_database() -> Database:
    """Retorna instância singleton do banco de dados."""
    global _db_instance
    if _db_instance is None:
        from config.settings import BOT_CONFIG
        _db_instance = Database(BOT_CONFIG["db_path"])
    return _db_instance
