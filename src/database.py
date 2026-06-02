"""
SQLite database layer for trade tracking and analytics.
"""
import aiosqlite
import asyncio
from datetime import datetime
from typing import Optional, Dict, List, Any
import structlog

logger = structlog.get_logger()


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None
    
    async def connect(self):
        """Initialize database connection with WAL mode."""
        self.conn = await aiosqlite.connect(self.db_path)
        
        # Enable WAL mode for better concurrency
        await self.conn.execute("PRAGMA journal_mode=WAL")
        await self.conn.execute("PRAGMA synchronous=NORMAL")
        await self.conn.execute("PRAGMA cache_size=10000")
        
        await self._create_tables()
        logger.info("database_connected", path=self.db_path)
    
    async def _create_tables(self):
        """Create database schema."""
        await self.conn.executescript("""
            -- Trades table
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT NOT NULL,
                market_type TEXT NOT NULL,
                entry_time TIMESTAMP NOT NULL,
                exit_time TIMESTAMP,
                side TEXT NOT NULL,
                entry_price REAL NOT NULL,
                exit_price REAL,
                position_size REAL NOT NULL,
                pnl REAL,
                spread_at_entry REAL NOT NULL,
                time_of_day INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'open'
            );
            
            -- Orderbook snapshots
            CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                yes_bid REAL,
                yes_ask REAL,
                no_bid REAL,
                no_ask REAL,
                yes_depth REAL,
                no_depth REAL
            );
            
            -- Price ticks during position lifetime
            CREATE TABLE IF NOT EXISTS price_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id INTEGER REFERENCES trades(id),
                timestamp TIMESTAMP NOT NULL,
                yes_price REAL NOT NULL,
                no_price REAL NOT NULL,
                spread REAL NOT NULL
            );
            
            -- Spread opportunities (for analysis)
            CREATE TABLE IF NOT EXISTS spread_opportunities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT NOT NULL,
                market_type TEXT NOT NULL,
                detected_at TIMESTAMP NOT NULL,
                yes_price REAL NOT NULL,
                no_price REAL NOT NULL,
                spread REAL NOT NULL,
                time_until_resolution INTEGER,
                traded BOOLEAN DEFAULT FALSE
            );
            
            -- Create indexes
            CREATE INDEX IF NOT EXISTS idx_trades_market_id ON trades(market_id);
            CREATE INDEX IF NOT EXISTS idx_trades_entry_time ON trades(entry_time);
            CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
            CREATE INDEX IF NOT EXISTS idx_opportunities_detected_at ON spread_opportunities(detected_at);
        """)
        await self.conn.commit()
    
    async def log_opportunity(self, market_id: str, market_type: str, 
                            yes_price: float, no_price: float, 
                            time_until_resolution: int, traded: bool = False):
        """Log a detected spread opportunity."""
        spread = yes_price + no_price - 1.0
        await self.conn.execute("""
            INSERT INTO spread_opportunities 
            (market_id, market_type, detected_at, yes_price, no_price, spread, 
             time_until_resolution, traded)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (market_id, market_type, datetime.utcnow(), yes_price, no_price, 
              spread, time_until_resolution, traded))
        await self.conn.commit()
    
    async def create_trade(self, market_id: str, market_type: str, side: str,
                          entry_price: float, position_size: float, 
                          spread_at_entry: float) -> int:
        """Create a new trade record."""
        now = datetime.utcnow()
        cursor = await self.conn.execute("""
            INSERT INTO trades 
            (market_id, market_type, entry_time, side, entry_price, 
             position_size, spread_at_entry, time_of_day, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open')
        """, (market_id, market_type, now, side, entry_price, 
              position_size, spread_at_entry, now.hour))
        await self.conn.commit()
        return cursor.lastrowid
    
    async def update_trade(self, trade_id: int, exit_price: float, pnl: float):
        """Update trade with exit information."""
        await self.conn.execute("""
            UPDATE trades 
            SET exit_time = ?, exit_price = ?, pnl = ?, status = 'closed'
            WHERE id = ?
        """, (datetime.utcnow(), exit_price, pnl, trade_id))
        await self.conn.commit()
    
    async def log_price_tick(self, trade_id: int, yes_price: float, 
                            no_price: float):
        """Log price tick during position lifetime."""
        spread = yes_price + no_price - 1.0
        await self.conn.execute("""
            INSERT INTO price_ticks (trade_id, timestamp, yes_price, no_price, spread)
            VALUES (?, ?, ?, ?, ?)
        """, (trade_id, datetime.utcnow(), yes_price, no_price, spread))
        await self.conn.commit()
    
    async def log_orderbook_snapshot(self, market_id: str, 
                                    yes_bid: float, yes_ask: float,
                                    no_bid: float, no_ask: float,
                                    yes_depth: float, no_depth: float):
        """Log orderbook snapshot."""
        await self.conn.execute("""
            INSERT INTO orderbook_snapshots 
            (market_id, timestamp, yes_bid, yes_ask, no_bid, no_ask, 
             yes_depth, no_depth)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (market_id, datetime.utcnow(), yes_bid, yes_ask, no_bid, 
              no_ask, yes_depth, no_depth))
        await self.conn.commit()
    
    async def get_daily_stats(self) -> Dict[str, Any]:
        """Get statistics for today."""
        today = datetime.utcnow().date()
        
        # Get opportunity count
        cursor = await self.conn.execute("""
            SELECT COUNT(*) FROM spread_opportunities 
            WHERE DATE(detected_at) = ?
        """, (today,))
        opp_count = (await cursor.fetchone())[0]
        
        # Get trades and calculate win rate
        cursor = await self.conn.execute("""
            SELECT COUNT(*), SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END), 
                   SUM(pnl), AVG(pnl)
            FROM trades 
            WHERE DATE(entry_time) = ? AND status = 'closed'
        """, (today,))
        row = await cursor.fetchone()
        total_trades = row[0] or 0
        winning_trades = row[1] or 0
        total_pnl = row[2] or 0.0
        avg_pnl = row[3] or 0.0
        
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0.0
        
        return {
            "date": str(today),
            "opportunities_detected": opp_count,
            "total_trades": total_trades,
            "winning_trades": winning_trades,
            "win_rate": win_rate,
            "total_pnl": total_pnl,
            "avg_pnl_per_trade": avg_pnl
        }
    
    async def close(self):
        """Close database connection."""
        if self.conn:
            await self.conn.close()
            logger.info("database_closed")
