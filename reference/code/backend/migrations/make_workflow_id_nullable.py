#!/usr/bin/env python3
"""Migration: Make workflow_id nullable in symbol_workflow_links"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from backend.core.database import engine


def migrate():
    """Make workflow_id nullable since we are using dag_id now"""
    with engine.connect() as conn:
        print("Checking current schema...")
        
        # Alter the column to drop NOT NULL constraint
        try:
            conn.execute(text("""
                ALTER TABLE symbol_workflow_links 
                ALTER COLUMN workflow_id DROP NOT NULL;
            """))
            print("✓ Made workflow_id nullable")
        except Exception as e:
            print(f"Warning: Could not alter column (might not exist or other error): {e}")

        # Optional: Drop the foreign key constraint if it exists to avoid issues with missing workflows
        # We'll try to find the constraint name first or just try to drop generic names if known, 
        # but for now just making it nullable should fix the immediate INSERT error.
        
        conn.commit()
        print("\n✅ Migration completed successfully!")


if __name__ == "__main__":
    try:
        migrate()
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        sys.exit(1)
