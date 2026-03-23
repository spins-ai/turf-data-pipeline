#!/usr/bin/env python3
"""
Script 29 — Arqana : Ventes de chevaux en France
Source : arqana.com (catalogues, résultats, Excel stats)
CRITIQUE pour : Valeur commerciale cheval, breeding value, ROI éleveur
"""

import json
import random
import os
import re
import sys
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.logging_setup import setup_logging
from utils.scraping import smart_pause, create_session

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "29_arqana_ventes")
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

log = setup_logging("29_arqana_ventes")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]

session = create_session(user_agents=USER_AGENTS)
session.headers.update({
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9",
})

def scrape_arqana_results():
    """Scraper les résultats de ventes Arqana (2019-2026)"""
    all_records = []
    
    # Types de ventes connus
    sale_types = [
        "vente-de-yearlings-aout",
        "vente-de-yearlings-octobre",
        "vente-delevage",
        "vente-de-chevaux-a-lentrainement",
        "breeze-up",
        "vente-de-yearlings",
        "arc-sale",
        "vente-dautomne",
    ]
    
    base_url = "https://www.arqana.com"
    
    # D'abord récupérer la liste des ventes depuis la page principale
    log.info("Récupération de la liste des ventes Arqana...")
    
    for year in range(2019, 2027):
        cache_file = os.path.join(CACHE_DIR, f"sales_list_{year}.json")
        if os.path.exists(cache_file):
            with open(cache_file, encoding="utf-8") as f:
                year_data = json.load(f)
                all_records.extend(year_data)
                log.info(f"  Cache {year}: {len(year_data)} lots")
            continue
            
        year_records = []
        
        # Essayer la page des résultats pour chaque année
        results_url = f"{base_url}/lots/results/{year}"
        try:
            resp = session.get(results_url, timeout=30)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                
                # Chercher les liens vers les ventes individuelles
                sale_links = []
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if "/lots/" in href and str(year) in href:
                        if href not in sale_links:
                            sale_links.append(href)
                
                log.info(f"  {year}: {len(sale_links)} liens de ventes trouvés")
                
                # Parser les tableaux de résultats directement
                tables = soup.find_all("table")
                for table in tables:
                    rows = table.find_all("tr")
                    for row in rows[1:]:  # skip header
                        cells = row.find_all(["td", "th"])
                        if len(cells) >= 4:
                            record = {
                                "year": year,
                                "source": "arqana",
                            }
                            for j, cell in enumerate(cells):
                                text = cell.get_text(strip=True)
                                record[f"col_{j}"] = text
                            year_records.append(record)
                
                # Si pas de table, chercher les lots individuels
                if not year_records:
                    lot_divs = soup.find_all(["div", "article"], class_=re.compile(r"lot|result|sale", re.I))
                    for div in lot_divs:
                        text = div.get_text(" ", strip=True)
                        if text and len(text) > 10:
                            year_records.append({
                                "year": year,
                                "source": "arqana",
                                "raw_text": text[:500],
                            })
            else:
                log.warning(f"  {year}: HTTP {resp.status_code}")
                
        except Exception as e:
            log.error(f"  {year}: {e}")
        
        # Essayer aussi la page catalogues/résultats
        for sale_type in sale_types:
            cat_url = f"{base_url}/lots/{sale_type}/{year}/results"
            try:
                resp = session.get(cat_url, timeout=30)
                if resp.status_code == 200 and len(resp.text) > 1000:
                    soup = BeautifulSoup(resp.text, "html.parser")
                    
                    # Chercher les lots
                    lots = soup.find_all(["div", "tr", "article"], class_=re.compile(r"lot|entry|result", re.I))
                    for lot in lots:
                        lot_data = {
                            "year": year,
                            "sale_type": sale_type,
                            "source": "arqana",
                        }
                        
                        # Extraire texte structuré
                        texts = lot.get_text(" | ", strip=True)
                        lot_data["raw"] = texts[:500]
                        
                        # Chercher prix
                        price_match = re.search(r'(\d[\d\s,.]*)\s*(?:€|EUR)', texts)
                        if price_match:
                            lot_data["prix"] = price_match.group(1).strip()
                        
                        # Chercher noms
                        strong = lot.find("strong")
                        if strong:
                            lot_data["nom_cheval"] = strong.get_text(strip=True)
                        
                        year_records.append(lot_data)
                    
                    if lots:
                        log.info(f"  {year}/{sale_type}: {len(lots)} lots")
                        
            except Exception as e:
                log.debug(f"  Erreur parsing arqana: {e}")

            smart_pause(1.5, 0.5)
        
        # Sauvegarder cache année
        if year_records:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(year_records, f, ensure_ascii=False)
        
        all_records.extend(year_records)
        log.info(f"  {year}: {len(year_records)} records total")
        smart_pause(2.0, 1.0)
    
    return all_records

def scrape_arqana_excel_links():
    """Chercher et télécharger les fichiers Excel/CSV de stats Arqana"""
    excel_files = []
    base_url = "https://www.arqana.com"
    
    # Pages susceptibles d'avoir des liens Excel
    pages = [
        f"{base_url}/statistiques",
        f"{base_url}/statistics",
        f"{base_url}/results",
        f"{base_url}/lots/results",
    ]
    
    for url in pages:
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if any(ext in href.lower() for ext in [".xlsx", ".xls", ".csv", ".pdf"]):
                        full_url = href if href.startswith("http") else base_url + href
                        excel_files.append({
                            "url": full_url,
                            "text": a.get_text(strip=True),
                            "page": url,
                        })
                        log.info(f"  Trouvé: {a.get_text(strip=True)} → {full_url}")
        except Exception as e:
            log.debug(f"  Erreur {url}: {e}")
        smart_pause(2.0, 1.0)
    
    return excel_files

def main():
    log.info("=" * 60)
    log.info("SCRIPT 29 — Arqana Ventes de Chevaux")
    log.info("=" * 60)
    
    # 1. Scraper les résultats de ventes
    records = scrape_arqana_results()
    log.info(f"Total résultats: {len(records)} records")
    
    # 2. Chercher les fichiers Excel
    excel_links = scrape_arqana_excel_links()
    log.info(f"Fichiers Excel/CSV trouvés: {len(excel_links)}")
    
    # Sauvegarder
    output_file = os.path.join(OUTPUT_DIR, "arqana_ventes.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    
    excel_file = os.path.join(OUTPUT_DIR, "arqana_excel_links.json")
    with open(excel_file, "w", encoding="utf-8") as f:
        json.dump(excel_links, f, ensure_ascii=False, indent=2)
    
    log.info("=" * 60)
    log.info(f"TERMINÉ: {len(records)} lots, {len(excel_links)} fichiers Excel")
    log.info("=" * 60)

if __name__ == "__main__":
    main()
