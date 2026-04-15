#!/usr/bin/env python3
"""
Diagnostic script to check database structure and column names.
Helps troubleshoot why company name/city are coming back as NULL.
"""

import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
_script_dir = os.path.dirname(os.path.abspath(__file__))
_env_file = os.path.join(_script_dir, ".env")
if os.path.exists(_env_file):
    load_dotenv(_env_file)
else:
    load_dotenv()

DB_URL = os.getenv(
    "NEON_DATABASE_URL",
    os.getenv("DATABASE_URL", "postgresql://user:password@host/dbname?sslmode=require")
)

def check_db_structure():
    """Check database tables and column names."""
    print("\n" + "="*70)
    print("  DATABASE STRUCTURE DIAGNOSTIC")
    print("="*70)

    try:
        conn = psycopg2.connect(DB_URL, connect_timeout=10)
        print("[OK] Connected to database\n")
    except Exception as e:
        print(f"[ERROR] Cannot connect to database: {e}")
        return

    try:
        with conn.cursor() as cur:
            # List all tables
            print(">>> TABLES IN DATABASE:")
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            tables = [r[0] for r in cur.fetchall()]
            for table in tables:
                print(f"  - {table}")

            print(f"\nTotal: {len(tables)} tables\n")

            # Identify main table
            candidates = [t for t in tables if any(
                kw in t.lower() for kw in ["company", "compan", "entreprise", "etablissement", "btp", "sirene"]
            )]
            if not candidates:
                candidates = [t for t in tables if t not in ("google_reviews",)]

            if candidates:
                main_table = candidates[0]
                print(f">>> MAIN TABLE DETECTED: {main_table}\n")

                # List columns
                print(f">>> COLUMNS IN '{main_table}':")
                cur.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{main_table}'
                    ORDER BY ordinal_position;
                """)
                cols_info = cur.fetchall()
                for col_name, col_type in cols_info:
                    print(f"  - {col_name:40s} ({col_type})")

                print(f"\nTotal: {len(cols_info)} columns\n")

                # Check for name/company columns
                print(">>> COLUMN MATCHING:")
                col_names = [c[0] for c in cols_info]

                # Look for SIRET
                siret_match = next((c for c in col_names if "siret" in c.lower()), None)
                print(f"  SIRET column: {siret_match or 'NOT FOUND'}")

                # Look for name/denomination
                name_candidates = [
                    "nom_entreprise", "denomination", "denominationusuelleetablissement",
                    "denominationunite", "name", "nom", "company_name"
                ]
                name_match = next((c for c in col_names if c.lower() in name_candidates), None)
                if not name_match:
                    name_match = next((c for c in col_names if any(x in c.lower() for x in ["name", "nom", "denomination"])), None)
                print(f"  NAME/DENOMINATION column: {name_match or 'NOT FOUND'}")
                if name_match:
                    print(f"    Will use: \"{name_match}\"")

                # Look for city/commune
                city_candidates = [
                    "libellecommuneetablissement", "commune", "city", "ville", "libelle_commune", "communne"
                ]
                city_match = next((c for c in col_names if c.lower() in city_candidates), None)
                if not city_match:
                    city_match = next((c for c in col_names if any(x in c.lower() for x in ["commune", "city", "ville"])), None)
                print(f"  CITY/COMMUNE column: {city_match or 'NOT FOUND'}")
                if city_match:
                    print(f"    Will use: \"{city_match}\"")

                # Check if name column has data
                print(f"\n>>> DATA QUALITY CHECK:")
                if siret_match and name_match:
                    cur.execute(f"""
                        SELECT
                            COUNT(*) as total,
                            COUNT("{name_match}") as with_name,
                            ROUND(COUNT("{name_match}") * 100.0 / COUNT(*), 1) as pct_filled
                        FROM "{main_table}";
                    """)
                    total, with_name, pct_filled = cur.fetchone()
                    print(f"  {name_match}: {with_name}/{total} records ({pct_filled}% filled)")

                    # If column is mostly empty, suggest alternatives
                    if with_name < total * 0.5:  # Less than 50% filled
                        print(f"  WARNING: '{name_match}' is mostly empty ({pct_filled}% filled)!")
                        print(f"  Checking alternative name columns...")

                        # Check for raison_sociale
                        if "raison_sociale" in col_names:
                            cur.execute(f"""
                                SELECT
                                    COUNT("raison_sociale") as with_raison,
                                    ROUND(COUNT("raison_sociale") * 100.0 / COUNT(*), 1) as pct
                                FROM "{main_table}";
                            """)
                            with_raison, pct_raison = cur.fetchone()
                            print(f"    - raison_sociale: {with_raison}/{total} records ({pct_raison}% filled) [BETTER]")

                # Sample data
                print(f"\n>>> SAMPLE DATA (first 5 rows):")
                if siret_match and city_match:
                    if name_match:
                        cur.execute(f"""
                            SELECT "{siret_match}", "{name_match}", "{city_match}"
                            FROM "{main_table}"
                            LIMIT 5;
                        """)
                        for row in cur.fetchall():
                            print(f"  SIRET: {row[0]}, NAME: {row[1]}, CITY: {row[2]}")
                    else:
                        cur.execute(f"""
                            SELECT "{siret_match}", "{city_match}"
                            FROM "{main_table}"
                            LIMIT 5;
                        """)
                        for row in cur.fetchall():
                            print(f"  SIRET: {row[0]}, CITY: {row[1]}")
                elif siret_match:
                    cur.execute(f"""
                        SELECT "{siret_match}"
                        FROM "{main_table}"
                        LIMIT 5;
                    """)
                    for row in cur.fetchall():
                        print(f"  SIRET: {row[0]}")

            else:
                print("[WARNING] No main table candidates found!")

    except Exception as e:
        print(f"[ERROR] Database query error: {e}")
    finally:
        conn.close()
        print("\n" + "="*70 + "\n")


if __name__ == "__main__":
    check_db_structure()
