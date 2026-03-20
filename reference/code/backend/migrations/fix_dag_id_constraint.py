#!/usr/bin/env python3
"""Migration: Update symbol_workflow_links constraint to use dag_id instead of workflow_id"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from backend.core.database import engine


def migrate():
    """Update the unique constraint to reference dag_id instead of workflow_id"""
    with engine.connect() as conn:
        print("Checking current schema...")
        
        # Drop the old constraint if it exists
        conn.execute(text("""
            ALTER TABLE symbol_workflow_links 
            DROP CONSTRAINT IF EXISTS uq_symbol_workflow_link;
        """))
        print("✓ Dropped old constraint (if existed)")
        
        # Add the new constraint
        conn.execute(text("""
            ALTER TABLE symbol_workflow_links 
            ADD CONSTRAINT uq_symbol_workflow_link 
            UNIQUE (workflow_symbol_id, dag_id);
        """))
        print("✓ Added new constraint on (workflow_symbol_id, dag_id)")
        
        conn.commit()
        print("\n✅ Migration completed successfully!")


if __name__ == "__main__":
    try:
        migrate()
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        sys.exit(1)
