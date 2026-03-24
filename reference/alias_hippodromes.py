"""
alias_hippodromes.py — Mapping des noms alternatifs d'hippodromes
=================================================================
Utilise pour normaliser les noms provenant de differentes sources.
"""

ALIASES = {
    # France
    "vincennes": ["hippodrome de vincennes", "paris vincennes", "vincennes paris"],
    "longchamp": ["hippodrome de longchamp", "paris longchamp", "parislongchamp"],
    "auteuil": ["hippodrome d'auteuil", "paris auteuil"],
    "saint-cloud": ["hippodrome de saint-cloud", "st cloud", "st-cloud"],
    "chantilly": ["hippodrome de chantilly"],
    "deauville": ["hippodrome de deauville", "deauville la touques"],
    "maisons-laffitte": ["maisons laffitte", "m-laffitte", "m. laffitte"],
    "enghien": ["enghien-soisy", "enghien soisy"],
    "cagnes-sur-mer": ["cagnes sur mer", "cagnes"],

    # UK
    "ascot": ["royal ascot"],
    "epsom": ["epsom downs"],
    "newmarket": ["newmarket rowley", "newmarket july"],
    "cheltenham": ["cheltenham racecourse"],
    "aintree": ["aintree racecourse"],
    "york": ["york racecourse"],
    "goodwood": ["glorious goodwood"],
    "doncaster": ["doncaster racecourse"],

    # International
    "sha tin": ["sha tin racecourse", "shatin"],
    "happy valley": ["happy valley racecourse"],
    "flemington": ["flemington racecourse"],
    "meydan": ["meydan racecourse", "dubai meydan"],
    "churchill downs": ["churchill downs racecourse"],
    "belmont park": ["belmont park racecourse"],
    "tokyo": ["tokyo racecourse"],
    "nakayama": ["nakayama racecourse"],

    # Suede
    "aby": ["aby goteborg", "aby suede", "goteborg"],
    "taby": ["taby stockholm", "taby galoppbana"],
}


def normalize_hippodrome(name: str) -> str:
    """Normalise un nom d'hippodrome en utilisant les alias."""
    if not name:
        return name
    name_lower = name.strip().lower()
    for canonical, aliases in ALIASES.items():
        if name_lower == canonical or name_lower in aliases:
            return canonical
    return name_lower
