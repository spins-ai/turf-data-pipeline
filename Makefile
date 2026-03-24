# ============================================================
# Makefile — turf-data-pipeline
# ============================================================
# Commandes principales pour installer, tester, lancer le
# pipeline et maintenir le projet.
# ============================================================

PYTHON ?= python
PIP    ?= pip

.PHONY: install check test pipeline scrape backup diagnostic clean help

# ----------------------------------------------------------
# install : installer les dependances + navigateurs Playwright
# ----------------------------------------------------------
install:
	$(PIP) install -r requirements.txt
	$(PYTHON) -m playwright install

# ----------------------------------------------------------
# check : verification CI (ci_check.py + py_compile all .py)
# ----------------------------------------------------------
check:
	$(PYTHON) scripts/ci_check.py
	$(PYTHON) -m compileall -q .

# ----------------------------------------------------------
# test : lancer la suite de tests pytest
# ----------------------------------------------------------
test:
	$(PYTHON) -m pytest tests/ -v

# ----------------------------------------------------------
# pipeline : executer le pipeline complet
# ----------------------------------------------------------
pipeline:
	$(PYTHON) run_pipeline.py

# ----------------------------------------------------------
# scrape : lancer le batch scraper
# ----------------------------------------------------------
scrape:
	$(PYTHON) batch_scraper.py

# ----------------------------------------------------------
# backup : sauvegarder les donnees
# ----------------------------------------------------------
backup:
	$(PYTHON) scripts/backup_data.py

# ----------------------------------------------------------
# diagnostic : rapport de diagnostic du pipeline
# ----------------------------------------------------------
diagnostic:
	$(PYTHON) scripts/diagnostic.py

# ----------------------------------------------------------
# clean : nettoyer les artefacts temporaires
#   - __pycache__ et .pyc
#   - fichiers .tmp
#   - logs de plus de 30 jours
# ----------------------------------------------------------
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.tmp" -delete 2>/dev/null || true
	find ./logs -type f -name "*.log" -mtime +30 -delete 2>/dev/null || true

# ----------------------------------------------------------
# help : afficher les cibles disponibles
# ----------------------------------------------------------
help:
	@echo "Cibles disponibles :"
	@echo "  make install     - Installer dependances + Playwright"
	@echo "  make check       - CI : ci_check.py + compileall"
	@echo "  make test        - Lancer pytest"
	@echo "  make pipeline    - Executer run_pipeline.py"
	@echo "  make scrape      - Lancer batch_scraper.py"
	@echo "  make backup      - Sauvegarder les donnees"
	@echo "  make diagnostic  - Rapport diagnostic"
	@echo "  make clean       - Nettoyer __pycache__, .tmp, vieux logs"
