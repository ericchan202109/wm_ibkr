import wmill
import psycopg2

def main(symbol: str, conid: int, exchange: str, currency: str, secType: str, status: str = 'SUSPEND'):
    # 1. get credential
    db_cred = wmill.get_resource("u/admin2/supabase_postgresql")
    
    # 3. insert data
    conn = psycopg2.connect(
        host=db_cred.get('host'),
        port=db_cred.get('port', 5432),
        user=db_cred.get('user'),
        password=db_cred.get('password'),
        dbname=db_cred.get('dbname', 'neondb'),
        sslmode=db_cred.get('sslmode', 'require')
    )
    
    conn.autocommit = True
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS contracts (
            symbol TEXT PRIMARY KEY,
            conid INTEGER NOT NULL
        );
    """)
    
    # Handle DB schema migration safely
    cursor.execute("""
        ALTER TABLE contracts ADD COLUMN IF NOT EXISTS exchange TEXT;
        ALTER TABLE contracts ADD COLUMN IF NOT EXISTS currency TEXT;
        ALTER TABLE contracts ADD COLUMN IF NOT EXISTS sec_type TEXT;
        ALTER TABLE contracts ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'SUSPEND';
    """)
    
    cursor.execute("""
        INSERT INTO contracts (symbol, conid, exchange, currency, sec_type, status)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol) DO UPDATE SET 
            conid = EXCLUDED.conid,
            exchange = EXCLUDED.exchange,
            currency = EXCLUDED.currency,
            sec_type = EXCLUDED.sec_type,
            status = EXCLUDED.status;
    """, (symbol, conid, exchange, currency, secType, status))
    
    cursor.close()
    conn.close()
    
    return {
        "record_status": "success", 
        "symbol": symbol, 
        "conid": conid,
        "exchange": exchange,
        "currency": currency,
        "secType": secType,
        "status": status
    }
