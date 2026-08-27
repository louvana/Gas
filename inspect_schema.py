import os
from database import get_conn

def inspect_database():
    conn = get_conn()
    cur = conn.cursor()
    
    # 1. Lister toutes les tables de la base
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public';
    """)
    tables = [row[0] for row in cur.fetchall()]
    print("📋 Tables trouvées dans la base :", tables)
    
    # 2. Inspecter les colonnes de gasoil_prices (ou nom similaire)
    target_table = None
    for t in tables:
        if 'gasoil' in t or 'price' in t:
            target_table = t
            break
            
    if target_table:
        cur.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{target_table}';
        """)
        columns = cur.fetchall()
        print(f"\n🔍 Colonnes de la table `{target_table}` :")
        for col, dtype in columns:
            print(f"  - {col} ({dtype})")
    else:
        print("\n⚠️ Aucune table contenant 'price' ou 'gasoil' n'a été trouvée.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    inspect_database()