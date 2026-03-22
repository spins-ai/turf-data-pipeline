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

__all__ = [
    "normalize_name",
    "normaliser_texte",
    "strip_accents",
    "normalize_date",
    "normalize_name_for_matching",
]

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


def strip_accents(text: str) -> str:
    """Supprime les accents d'une chaine Unicode.

    Parameters
    ----------
    text : str
        Texte avec accents potentiels.

    Returns
    -------
    str
        Texte sans accents.

    Examples
    --------
    >>> strip_accents("étoile du berger")
    'etoile du berger'
    """
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def normalize_date(date_str: str | None) -> str:
    """Normalise une date au format ISO YYYY-MM-DD.

    Gere les formats : YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY.

    Parameters
    ----------
    date_str : str or None
        Date a normaliser.

    Returns
    -------
    str
        Date au format YYYY-MM-DD, ou "" si invalide.

    Examples
    --------
    >>> normalize_date("25/12/2024")
    '2024-12-25'
    >>> normalize_date("2024-12-25T14:30:00")
    '2024-12-25'
    """
    if not date_str:
        return ""
    date_str = str(date_str).strip()

    # Deja au format ISO
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", date_str)
    if m:
        return m.group(0)

    # Format DD/MM/YYYY
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})", date_str)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    # Format DD-MM-YYYY
    m = re.match(r"^(\d{2})-(\d{2})-(\d{4})", date_str)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    return date_str[:10] if len(date_str) >= 10 else ""


def normaliser_texte(texte: str) -> str:
    """Normalise un texte : strip, lower, supprime les accents.

    Parameters
    ----------
    texte : str
        Texte a normaliser.

    Returns
    -------
    str
        Texte en minuscules sans accents, ou ``""`` si vide.

    Examples
    --------
    >>> normaliser_texte("Étoile du Berger")
    'etoile du berger'
    >>> normaliser_texte("")
    ''
    """
    if not texte:
        return ""
    texte = texte.strip().lower()
    nfkd = unicodedata.normalize("NFKD", texte)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


# Alias pour import direct
normalize_name_for_matching = normalize_name
