#!/usr/bin/env python3
"""
utils/normalize.py
==================
Fonctions partagees de normalisation de noms (chevaux, jockeys, entraineurs).

Centralise la logique de normalisation utilisee par 12+ scripts du pipeline
pour eviter la duplication de code et garantir un matching coherent.

Usage:
    from utils.normalize import normalize_name
    # ou
    from utils.normalize import normalize_name_for_matching
"""

from __future__ import annotations

import re
import unicodedata


def normalize_name(name: str | None, keep_digits: bool = True, strip_country: bool = True) -> str:
    """Normalise un nom pour le matching/jointure.

    Pipeline de normalisation :
      1. strip + UPPER
      2. Suppression des accents (NFKD)
      3. Suppression optionnelle des suffixes pays ex: (IRE), (FR), (USA)
      4. Remplacement apostrophes et tirets par espaces
      5. Conservation alphanum + espaces (ou lettres + espaces si keep_digits=False)
      6. Collapse des espaces multiples

    Parameters
    ----------
    name : str or None
        Nom a normaliser.
    keep_digits : bool
        Si True (defaut), garde les chiffres dans le nom.
        Si False, ne garde que les lettres et espaces.
    strip_country : bool
        Si True (defaut), supprime les suffixes pays comme (IRE), (FR), (USA).

    Returns
    -------
    str
        Nom normalise (toujours une string, jamais None).

    Examples
    --------
    >>> normalize_name("Prince d'Or")
    'PRINCE DOR'
    >>> normalize_name("ÉTOILE DU BERGER (FR)")
    'ETOILE DU BERGER'
    >>> normalize_name("Lucky Star 3rd", keep_digits=False)
    'LUCKY STAR RD'
    """
    if not name:
        return ""

    name = str(name).strip().upper()

    # Supprimer les accents via decomposition Unicode NFKD
    nfkd = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in nfkd if not unicodedata.combining(c))

    # Supprimer les suffixes pays ex: (IRE), (FR), (USA), (GER)
    if strip_country:
        name = re.sub(r"\s*\([A-Z]{2,4}\)\s*$", "", name)

    # Remplacer apostrophes et tirets par espaces
    name = name.replace("\u2019", " ").replace("'", " ").replace("-", " ")

    # Garder uniquement alphanum + espaces (ou lettres + espaces)
    if keep_digits:
        name = re.sub(r"[^A-Z0-9\s]", "", name)
    else:
        name = re.sub(r"[^A-Z\s]", "", name)

    # Normaliser les espaces
    return " ".join(name.split())


# Alias pour import direct
normalize_name_for_matching = normalize_name
