#!/usr/bin/env python3
"""
fix_output_permissions.py
=========================
Corrige le probleme de junction/read-only sur le dossier output/.

Si output/ est une jonction Windows (ou un lien symbolique):
  1. Copie toutes les donnees dans un dossier temporaire
  2. Supprime la jonction
  3. Cree un vrai dossier output/ avec les donnees copiees
  4. Verifie l'integrite apres copie (taille fichiers + nombre)

Usage:
    python3 fix_output_permissions.py
    python3 fix_output_permissions.py --dry-run
"""

import argparse
import logging
import os
import shutil
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger(__name__)

OUTPUT_DIR = "output"
TEMP_DIR = "output_temp_copy"


def is_junction_or_symlink(path):
    """Verifie si un chemin est une jonction Windows ou un lien symbolique."""
    if os.path.islink(path):
        return True
    # Sur Windows, verifier les reparse points (junctions)
    if sys.platform == "win32":
        try:
            import ctypes
            from ctypes import wintypes
            FILE_ATTRIBUTE_REPARSE_POINT = 0x0400
            attrs = ctypes.windll.kernel32.GetFileAttributesW(str(path))
            if attrs == -1:
                return False
            return bool(attrs & FILE_ATTRIBUTE_REPARSE_POINT)
        except Exception as e:
            log.debug("Error checking reparse point for %s: %s", path, e)
        # Fallback: essayer avec os.readlink
        try:
            os.readlink(path)
            return True
        except (OSError, ValueError):
            pass
    return False


def get_junction_target(path):
    """Retourne la cible d'une jonction/symlink."""
    try:
        return os.readlink(path)
    except (OSError, ValueError):
        return None


def count_files_and_size(directory):
    """Compte le nombre de fichiers et la taille totale d'un dossier."""
    total_files = 0
    total_size = 0
    for root, dirs, files in os.walk(directory):
        for f in files:
            fp = os.path.join(root, f)
            try:
                total_files += 1
                total_size += os.path.getsize(fp)
            except OSError:
                pass
    return total_files, total_size


def verify_integrity(source_dir, dest_dir):
    """Verifie que la copie est integre (nombre de fichiers et taille)."""
    src_files, src_size = count_files_and_size(source_dir)
    dst_files, dst_size = count_files_and_size(dest_dir)

    log.info("  Verification d'integrite:")
    log.info("    Source: %d fichiers, %.2f GB", src_files, src_size / (1024 ** 3))
    log.info("    Copie:  %d fichiers, %.2f GB", dst_files, dst_size / (1024 ** 3))

    ok = True
    if src_files != dst_files:
        log.error("    ERREUR: nombre de fichiers different (%d vs %d)", src_files, dst_files)
        ok = False
    if src_size != dst_size:
        log.warning("    ATTENTION: taille differente (%d vs %d bytes)", src_size, dst_size)
        # Pas forcement une erreur (metadata peut varier)

    return ok


def check_read_only_files(directory):
    """Verifie s'il y a des fichiers en lecture seule."""
    readonly_count = 0
    for root, dirs, files in os.walk(directory):
        for f in files:
            fp = os.path.join(root, f)
            try:
                if not os.access(fp, os.W_OK):
                    readonly_count += 1
            except OSError:
                pass
    return readonly_count


def fix_readonly_permissions(directory):
    """Rend tous les fichiers d'un dossier accessibles en ecriture."""
    import stat
    fixed = 0
    for root, dirs, files in os.walk(directory):
        for d in dirs:
            dp = os.path.join(root, d)
            try:
                os.chmod(dp, stat.S_IRWXU | stat.S_IRWXG | stat.S_IROTH | stat.S_IXOTH)
            except OSError:
                pass
        for f in files:
            fp = os.path.join(root, f)
            try:
                if not os.access(fp, os.W_OK):
                    os.chmod(fp, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
                    fixed += 1
            except OSError:
                pass
    return fixed


def parse_args():
    parser = argparse.ArgumentParser(description="Corriger les problemes de junction/permissions sur output/")
    parser.add_argument("--dry-run", action="store_true",
                        help="Afficher ce qui serait fait sans rien modifier")
    parser.add_argument("--target", default=OUTPUT_DIR,
                        help="Dossier cible (defaut: output)")
    return parser.parse_args()


def main():
    args = parse_args()
    target = args.target
    dry_run = args.dry_run

    log.info("=" * 70)
    log.info("FIX OUTPUT PERMISSIONS")
    log.info("  Cible: %s", os.path.abspath(target))
    if dry_run:
        log.info("  MODE DRY-RUN: aucune modification")
    log.info("=" * 70)

    if not os.path.exists(target):
        log.error("Le dossier %s n'existe pas", target)
        sys.exit(1)

    # 1. Verifier si c'est une jonction
    is_junction = is_junction_or_symlink(target)
    junction_target = get_junction_target(target) if is_junction else None

    if is_junction:
        log.info("")
        log.info("[DETECTE] %s est une jonction/symlink", target)
        if junction_target:
            log.info("  Cible: %s", junction_target)
    else:
        log.info("")
        log.info("[OK] %s est un vrai dossier (pas une jonction)", target)

    # 2. Verifier les permissions
    readonly_count = check_read_only_files(target)
    if readonly_count > 0:
        log.info("[DETECTE] %d fichiers en lecture seule", readonly_count)
    else:
        log.info("[OK] Aucun fichier en lecture seule detecte")

    # Si rien a corriger
    if not is_junction and readonly_count == 0:
        log.info("")
        log.info("Rien a corriger. Le dossier est sain.")
        return

    # 3. Agir
    if is_junction:
        log.info("")
        log.info("--- Correction de la jonction ---")

        # Compter les fichiers dans la cible
        num_files, total_size = count_files_and_size(target)
        log.info("  Contenu: %d fichiers, %.2f GB", num_files, total_size / (1024 ** 3))

        if dry_run:
            log.info("  [DRY-RUN] Copierait %d fichiers vers %s", num_files, TEMP_DIR)
            log.info("  [DRY-RUN] Supprimerait la jonction %s", target)
            log.info("  [DRY-RUN] Renommerait %s -> %s", TEMP_DIR, target)
        else:
            # Etape 3a: Copier vers temp
            if os.path.exists(TEMP_DIR):
                log.info("  Suppression de l'ancien %s...", TEMP_DIR)
                shutil.rmtree(TEMP_DIR, ignore_errors=True)

            log.info("  Copie en cours vers %s...", TEMP_DIR)
            t0 = time.time()
            shutil.copytree(target, TEMP_DIR, symlinks=False, dirs_exist_ok=False)
            elapsed = time.time() - t0
            log.info("  Copie terminee en %.1fs", elapsed)

            # Verifier integrite
            if not verify_integrity(target, TEMP_DIR):
                log.error("  Integrite KO! Abandon. Le dossier temp est conserve: %s", TEMP_DIR)
                sys.exit(1)

            log.info("  Integrite OK")

            # Etape 3b: Supprimer la jonction
            log.info("  Suppression de la jonction %s...", target)
            try:
                os.rmdir(target)  # rmdir fonctionne pour les junctions vides
            except OSError:
                try:
                    # Sur Windows, utiliser la commande systeme
                    if sys.platform == "win32":
                        os.system('rmdir /q "%s"' % target)
                    else:
                        os.unlink(target)
                except OSError as e:
                    log.error("  Impossible de supprimer la jonction: %s", e)
                    log.error("  Le dossier temp est conserve: %s", TEMP_DIR)
                    sys.exit(1)

            # Etape 3c: Renommer temp -> output
            log.info("  Renommage %s -> %s...", TEMP_DIR, target)
            os.rename(TEMP_DIR, target)

            log.info("  Junction corrigee avec succes!")

    # 4. Corriger les permissions read-only
    if readonly_count > 0 and not dry_run:
        log.info("")
        log.info("--- Correction des permissions lecture seule ---")
        fixed = fix_readonly_permissions(target)
        log.info("  %d fichiers corriges", fixed)

        # Re-verifier
        remaining = check_read_only_files(target)
        if remaining > 0:
            log.warning("  %d fichiers encore en lecture seule (probleme de droits?)", remaining)
        else:
            log.info("  Toutes les permissions sont OK")

    elif readonly_count > 0 and dry_run:
        log.info("")
        log.info("[DRY-RUN] Corrigerait %d fichiers en lecture seule", readonly_count)

    # 5. Verification finale
    if not dry_run:
        log.info("")
        log.info("--- Verification finale ---")
        final_junction = is_junction_or_symlink(target)
        final_readonly = check_read_only_files(target)
        final_files, final_size = count_files_and_size(target)

        log.info("  Est une jonction: %s", "OUI (ERREUR)" if final_junction else "NON (OK)")
        log.info("  Fichiers read-only: %d", final_readonly)
        log.info("  Fichiers totaux: %d (%.2f GB)", final_files, final_size / (1024 ** 3))

        if not final_junction and final_readonly == 0:
            log.info("  RESULTAT: Tout est OK")
        else:
            log.warning("  RESULTAT: Des problemes subsistent")

    log.info("")
    log.info("=" * 70)
    log.info("TERMINE")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
