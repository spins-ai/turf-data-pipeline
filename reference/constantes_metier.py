"""
constantes_metier.py — Constantes du domaine hippique
======================================================
Toutes les constantes metier centralisees pour eviter les magic numbers.
"""

# === DISCIPLINES ===
DISCIPLINES = {
    "trot_attele": "Trot Attele",
    "trot_monte": "Trot Monte",
    "galop_plat": "Galop Plat",
    "galop_obstacle": "Galop Obstacle",
    "galop_steeple": "Steeple-Chase",
    "galop_haies": "Haies",
    "galop_cross": "Cross-Country",
}

# === TERRAIN / GOING ===
TERRAIN_MAPPING_FR = {
    "tres_sec": 1, "sec": 2, "bon": 3, "assez_bon": 3,
    "bon_souple": 4, "souple": 5, "tres_souple": 6,
    "collant": 7, "lourd": 8, "tres_lourd": 9,
}

TERRAIN_MAPPING_UK = {
    "hard": 1, "firm": 2, "good_to_firm": 3, "good": 4,
    "good_to_soft": 5, "soft": 6, "heavy": 7,
}

TERRAIN_MAPPING_US = {
    "fast": 1, "good": 2, "yielding": 3, "muddy": 4,
    "sloppy": 5, "heavy": 6,
}

# === CONVERSIONS ===
FURLONG_TO_METERS = 201.168
MILE_TO_METERS = 1609.344
STONE_TO_KG = 6.35029
POUND_TO_KG = 0.453592

# === PLAGES REALISTES ===
COTE_MIN = 1.01
COTE_MAX = 1000.0
DISTANCE_MIN_M = 800
DISTANCE_MAX_M = 8000
POIDS_MIN_KG = 40
POIDS_MAX_KG = 90
NB_PARTANTS_MIN = 2
NB_PARTANTS_MAX = 24
AGE_CHEVAL_MIN = 2
AGE_CHEVAL_MAX = 15

# === TYPES DE COURSES ===
PRESTIGE_LEVELS = {
    "groupe_1": 10, "groupe_2": 9, "groupe_3": 8,
    "listed": 7, "conditions": 6, "handicap": 5,
    "reclamer": 4, "a_conditions": 3,
    "apprentis": 2, "amateurs": 1,
}

# === SEXE CHEVAL ===
SEXE_MAPPING = {
    "M": "male", "H": "hongre", "F": "femelle",
    "m": "male", "h": "hongre", "f": "femelle",
    "males": "male", "hongres": "hongre", "femelles": "femelle",
}

# === SAISONS (hemisphere nord) ===
FLAT_SEASON_START_MONTH = 3   # mars
FLAT_SEASON_END_MONTH = 11    # novembre
JUMP_SEASON_START_MONTH = 10  # octobre
JUMP_SEASON_END_MONTH = 4     # avril

# === AGE REFERENCE ===
# Hemisphere nord: age calcule a partir du 1er janvier
# Hemisphere sud: age calcule a partir du 1er aout
AGE_REF_DATE_NORTH = (1, 1)   # (mois, jour)
AGE_REF_DATE_SOUTH = (8, 1)   # (mois, jour)
