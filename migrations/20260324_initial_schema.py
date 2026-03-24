"""
Migration initiale — Documentation du schema v1.0
===================================================
Cette migration documente le schema initial des fichiers master.
Pas de transformation necessaire, sert de reference pour les migrations futures.
"""

SCHEMA_VERSION = "1.0.0"
DATE = "2026-03-24"
DESCRIPTION = "Schema initial des fichiers master"

PARTANTS_MASTER_FIELDS = [
    "partant_uid", "course_uid", "reunion_uid", "date",
    "hippodrome", "hippodrome_normalise", "discipline",
    "nom_cheval", "nom_jockey", "nom_entraineur",
    "position_finale", "cote_finale", "distance",
    "terrain", "meteo", "pedigree",
    # ... 97 colonnes au total dans le mega-merge
]

def migrate():
    """No-op: schema initial, pas de transformation."""
    print(f"Schema v{SCHEMA_VERSION} — {DESCRIPTION}")
    print("Pas de migration necessaire (schema initial)")

if __name__ == "__main__":
    migrate()
