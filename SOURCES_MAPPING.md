# SOURCES MAPPING -- External Data Sources to 68 Pipeline Modules
# Courses Hippiques France -- Mars 2026

> **Document type**: Reference for data acquisition
> **Last updated**: 2026-03-15
> **Total sources catalogued**: 210+
> **Pipeline modules**: 68 (16 phases)
> **Current features**: 67 built / 481 catalogued

---

## TABLE OF CONTENTS

1. [What We Already Have -- Hedge Fund Signal Coverage](#1-what-we-already-have----hedge-fund-signal-coverage)
2. [Sources by Category](#2-sources-by-category)
   - A. PMU Official APIs
   - B. PMU Derived / Internal Endpoints
   - C. Institutional / Government Open Data
   - D. Pedigree / Breeding Sources
   - E. Weather / Meteorological Sources
   - F. Betting Exchanges & Bookmakers
   - G. Community / Pronostic Sites
   - H. Expert / Press Sources
   - I. International Racing Databases
   - J. Commercial / Sales Data
   - K. Kaggle / Open Datasets
   - L. Video / Tracking / Biomechanics
3. [Pipeline Module Dependency Matrix](#3-pipeline-module-dependency-matrix)
4. [Acquisition Priorities](#4-acquisition-priorities)

---

## 1. WHAT WE ALREADY HAVE -- Hedge Fund Signal Coverage

The 10 key signals used by professional horse racing hedge funds, and our coverage status:

| # | Signal | Status | Source(s) | Coverage | Notes |
|---|--------|--------|-----------|----------|-------|
| 1 | Odds bookmakers (cotes) | PARTIAL | PMU API (07_cotes_marche), Unibet (34), Smarkets (30) | PMU 2013-2026; exchanges TO ACQUIRE | Multi-source odds comparison not yet operational |
| 2 | Resultats historiques | YES | PMU API (02, 02b, 04), Nanaelie (16), LeTrot | 2.93M partants 2004-2026 | Core dataset, well covered |
| 3 | Pedigree | PARTIAL | PedigreeQuery (14), PMU (pere/mere), SIRE/IFCE (17) | 24K/58K chevaux detailed; basic for all | Deep pedigree (4 gen) partial; SIRE downloaded |
| 4 | Meteo | YES | Meteostat (13), Open-Meteo (35) | 31,778 courses | 12% coverage of all courses; historical gaps |
| 5 | Terrain / piste | YES | PMU (penetrometre), Equidia patch | ~100 hippodromes | Penetrometre + type_piste available |
| 6 | Vitesse sectionnelle | PARTIAL | PMU sectionals (11), Performances detaillees (22) | Tracked courses only | Only courses with timing infrastructure |
| 7 | Biomecanique | NO | -- | 0% | Requires video analysis / sensor data; not available in France |
| 8 | Tracking GPS | NO | -- | 0% | Not publicly available for FR racing |
| 9 | Historique jockey | YES | PMU (06_historique_jockeys), rolling stats | 12,319 jockeys | Full coverage with temporal windows |
| 10 | Historique entraineur | YES | PMU (06_historique_jockeys), rolling stats | ~5,000 entraineurs | Full coverage with temporal windows |

**Score: 6/10 fully covered, 3/10 partially covered, 1/10 missing (biomecanique/GPS combined)**

### Signal gap analysis

| Gap | Impact | Mitigation |
|-----|--------|------------|
| Multi-source odds (Betfair BSP, Smarkets, Unibet) | HIGH -- needed for value detection | Scripts 30, 34 written; need to run + historical backfill |
| Deep pedigree for trot | MEDIUM -- 42% of races | LeTrot pedigree data not structured; PedigreeQuery covers galop only |
| Sectional timing gaps | MEDIUM -- only tracked courses | Racing Post Top Speed (37) can fill galop gaps |
| Biomecanique + GPS | LOW short term | No public source exists; proxy via pace profile + sectionals |

---

## 2. SOURCES BY CATEGORY

### A. PMU OFFICIAL APIS (Core Data)

#### A1. PMU Offline API -- Programme / Partants
| Field | Value |
|-------|-------|
| **Source name** | PMU Offline API -- Programme |
| **URL** | offline.turfinfo.api.pmu.fr/rest/client/7/programme/{date} |
| **Script** | 02_liste_courses.py, 02b_liste_courses_2013.py |
| **Data provided** | Courses, partants, cotes, equipements, poids, musique, conditions |
| **Volume** | ~2.93M partants, ~257K courses |
| **Period** | 2013-2026 |
| **Modules fed** | 01 (ingestion), 02 (schema), 03 (dataset builder), 09 (features), 10 (rolling), 11 (temporal), 12 (odds), 16 (pace), 18 (field strength) |
| **Priority** | HIGH |
| **Accessibility** | FREE API |
| **Status** | ALREADY HAVE |

#### A2. PMU Offline API -- Calendrier Reunions
| Field | Value |
|-------|-------|
| **Source name** | PMU Offline API -- Calendrier |
| **URL** | offline.turfinfo.api.pmu.fr/rest/client/7/programme/{date} |
| **Script** | 01_calendrier_reunions.py |
| **Data provided** | Liste des reunions par jour, hippodromes, disciplines |
| **Modules fed** | 01, 03, 11, 64 (orchestrateur) |
| **Priority** | HIGH |
| **Accessibility** | FREE API |
| **Status** | ALREADY HAVE |

#### A3. PMU Offline API -- Resultats
| Field | Value |
|-------|-------|
| **Source name** | PMU Offline API -- Resultats |
| **URL** | offline.turfinfo.api.pmu.fr/rest/client/7/programme/{date}/R{r}/C{c}/resultats |
| **Script** | 04_resultats.py |
| **Data provided** | Positions arrivee, ecarts, temps, rapports |
| **Modules fed** | 01, 03, 04, 10, 41-43 (calibration), 58 (recalibration), 61-63 (monitoring) |
| **Priority** | HIGH |
| **Accessibility** | FREE API |
| **Status** | ALREADY HAVE |

#### A4. PMU Offline API -- Rapports Definitifs (National)
| Field | Value |
|-------|-------|
| **Source name** | PMU Rapports Definitifs |
| **URL** | offline.turfinfo.api.pmu.fr/rest/client/1/programme/{date}/R{r}/C{c}/rapports-definitifs |
| **Script** | 21_rapports_definitifs.py |
| **Data provided** | Rapports simple/couple/tierce/quarte/quinte, mise base 200 |
| **Volume** | 124,287 courses |
| **Modules fed** | 12 (odds features), 44 (anomaly), 47 (ROI predictor), 48 (value hunter), 53-57 (bet sizing) |
| **Priority** | HIGH |
| **Accessibility** | FREE API |
| **Status** | ALREADY HAVE |

#### A5. PMU Offline API -- Rapports Internet
| Field | Value |
|-------|-------|
| **Source name** | PMU Rapports Internet (e-paris) |
| **URL** | offline.turfinfo.api.pmu.fr/rest/client/1/programme/{date}/R{r}/C{c}/rapports-definitifs?specialisation=INTERNET |
| **Script** | 38_rapports_internet.py |
| **Data provided** | Rapports e-paris, mise base 100 (different odds pool from national) |
| **Modules fed** | 12, 44 (anomaly -- compare national vs internet), 47, 48 |
| **Priority** | MEDIUM |
| **Accessibility** | FREE API |
| **Status** | PARTIALLY HAVE |

#### A6. PMU Offline API -- Historique Chevaux
| Field | Value |
|-------|-------|
| **Source name** | PMU Historique Chevaux |
| **URL** | offline.turfinfo.api.pmu.fr/rest/client/7/programme/{date}/R{r}/C{c}/participants/{num}/historiquePerformances |
| **Script** | 05_historique_chevaux.py |
| **Data provided** | Stats aggregees par cheval (nb courses, victoires, gains, disciplines) |
| **Volume** | 80,656 chevaux |
| **Modules fed** | 09, 10, 13 (synergy), 18 (field strength), 31 (GNN) |
| **Priority** | HIGH |
| **Accessibility** | FREE API |
| **Status** | ALREADY HAVE |

#### A7. PMU Offline API -- Historique Jockeys/Entraineurs
| Field | Value |
|-------|-------|
| **Source name** | PMU Historique Jockeys & Entraineurs |
| **Script** | 06_historique_jockeys.py |
| **Data provided** | Stats jockeys (12,319) et entraineurs (~5,000): victoires, places, gains, chevaux montes |
| **Modules fed** | 09, 10, 13 (synergy), 31 (GNN), 40 (meta model) |
| **Priority** | HIGH |
| **Accessibility** | FREE API |
| **Status** | ALREADY HAVE |

#### A8. PMU Offline API -- Cotes Marche
| Field | Value |
|-------|-------|
| **Source name** | PMU Cotes Marche |
| **Script** | 07_cotes_marche.py |
| **Data provided** | Cote finale, cote reference, proba implicite, surcote |
| **Modules fed** | 12 (odds features), 44 (anomaly), 47-48 (betting), 50 (ZURI) |
| **Priority** | HIGH |
| **Accessibility** | FREE API |
| **Status** | ALREADY HAVE |

#### A9. PMU Offline API -- Equipements
| Field | Value |
|-------|-------|
| **Source name** | PMU Equipements |
| **Script** | 09_equipements.py |
| **Data provided** | Oeilleres, deferre, changements equipement |
| **Modules fed** | 09, 44 (anomaly -- equipment changes signal), 45 (retour forme) |
| **Priority** | MEDIUM |
| **Accessibility** | FREE API |
| **Status** | ALREADY HAVE |

#### A10. PMU Offline API -- Poids Handicaps
| Field | Value |
|-------|-------|
| **Source name** | PMU Poids & Handicaps |
| **Script** | 10_poids_handicaps.py |
| **Data provided** | Poids porte, poids base, surcharge/decharge, handicap valeur, handicap distance |
| **Modules fed** | 09, 18 (field strength), 52 (race simulation) |
| **Priority** | MEDIUM |
| **Accessibility** | FREE API |
| **Status** | ALREADY HAVE |

#### A11. PMU Offline API -- Sectionals
| Field | Value |
|-------|-------|
| **Source name** | PMU Sectionals (temps par section) |
| **Script** | 11_sectionals.py |
| **Data provided** | Reduction km, vitesse, ecarts, temps sections |
| **Modules fed** | 16 (pace profile), 17 (sectional features), 52 (race simulation) |
| **Priority** | HIGH |
| **Accessibility** | FREE API |
| **Status** | ALREADY HAVE (tracked courses only) |

#### A12. PMU Offline API -- Pedigree Parents
| Field | Value |
|-------|-------|
| **Source name** | PMU Pedigree (pere, mere, pere_mere) |
| **Script** | 08_pedigree.py |
| **Data provided** | Nom pere, mere, pere de la mere (1 generation) |
| **Modules fed** | 14 (pedigree features), 31 (GNN) |
| **Priority** | MEDIUM |
| **Accessibility** | FREE API |
| **Status** | ALREADY HAVE |

---

### B. PMU DERIVED / INTERNAL ENDPOINTS

#### B1. PMU Performances Detaillees
| Field | Value |
|-------|-------|
| **Source name** | PMU Performances Detaillees |
| **URL** | online.turfinfo.api.pmu.fr/rest/client/61/programme/{date}/R{n}/C{n}/performances-detaillees/pretty |
| **Script** | 22_performances_detaillees.py |
| **Data provided** | 5 dernieres performances detaillees (date, hippodrome, distance, position, ecart, temps, terrain, cote, nb_partants) |
| **Volume** | 917,805 partants |
| **Modules fed** | 09, 10 (rolling), 27-28 (LSTM/GRU sequences), 30 (TFT), 45 (retour forme) |
| **Priority** | HIGH |
| **Accessibility** | FREE API (non-documented) |
| **Status** | PARTIALLY HAVE (31% of partants) |

#### B2. PMU Pronostics Officiels
| Field | Value |
|-------|-------|
| **Source name** | PMU Pronostics via Equidia |
| **URL** | offline.turfinfo.api.pmu.fr/rest/client/7/programme/{date}/R{r}/C{c}/pronostics |
| **Script** | 23_pronostics_equidia.py |
| **Data provided** | Pronostics officiels avec cotes probables |
| **Modules fed** | 44 (anomaly -- pronostics vs market), 45 (retour forme), 50 (ZURI outsider) |
| **Priority** | MEDIUM |
| **Accessibility** | FREE API |
| **Status** | PARTIALLY HAVE |

#### B3. PMU Citations & Enjeux
| Field | Value |
|-------|-------|
| **Source name** | PMU Citations (distribution des paris) |
| **URL** | offline.turfinfo.api.pmu.fr/rest/client/7/programme/{date}/R{r}/C{c}/citations |
| **Script** | 27_citations_enjeux.py |
| **Data provided** | Pourcentage de citations par cheval, masse d'enjeux, repartition paris |
| **Modules fed** | 12 (odds -- crowd wisdom), 44 (anomaly), 47-48 (betting), 50 (ZURI) |
| **Priority** | HIGH |
| **Accessibility** | FREE API |
| **Status** | PARTIALLY HAVE |

#### B4. PMU Combinaisons & Masse d'Enjeux
| Field | Value |
|-------|-------|
| **Source name** | PMU Combinaisons Marche |
| **URL** | offline.turfinfo.api.pmu.fr/rest/client/7/programme/{date}/R{r}/C{c}/combinaisons |
| **Script** | 28_combinaisons_marche.py |
| **Data provided** | Structure du marche des paris, rapports probables, combinaisons jouees |
| **Modules fed** | 12, 47 (ROI predictor), 48 (value hunter), 53-57 (bet sizing -- rapport probable) |
| **Priority** | HIGH |
| **Accessibility** | FREE API |
| **Status** | PARTIALLY HAVE |

#### B5. PMU Reunions Enrichies
| Field | Value |
|-------|-------|
| **Source name** | PMU Reunions Enrichies |
| **URL** | offline.turfinfo.api.pmu.fr/rest/client/1/programme/{DDMMYYYY}/R{r} |
| **Script** | 39_reunions_enrichies.py |
| **Data provided** | Meteo reunion, incidents, conditions specifiques, duree, commentaires, types paris |
| **Modules fed** | 09, 11 (temporal), 15 (track bias), 52 (race simulation) |
| **Priority** | MEDIUM |
| **Accessibility** | FREE API |
| **Status** | PARTIALLY HAVE |

#### B6. PMU Enrichissement Partants (cache-derived)
| Field | Value |
|-------|-------|
| **Source name** | PMU Enrichissement Partants |
| **Script** | 40_enrichissement_partants.py |
| **Data provided** | gains_victoires, gains_place, gains_annee_precedente, cote_tendance, is_favori_direct |
| **Modules fed** | 09, 10, 12 |
| **Priority** | MEDIUM |
| **Accessibility** | FREE (derived from existing cache) |
| **Status** | ALREADY HAVE |

---

### C. INSTITUTIONAL / GOVERNMENT OPEN DATA

#### C1. IFCE/SIRE Fichier des Equides
| Field | Value |
|-------|-------|
| **Source name** | IFCE/SIRE -- Fichier National des Equides |
| **URL** | https://www.data.gouv.fr/datasets/fichier-des-equides |
| **Script** | 17_process_sire.py |
| **Data provided** | 4M equides: race, sexe, robe, date_naissance, pays_naissance, date_mort |
| **Format** | CSV, Licence Ouverte |
| **Modules fed** | 09 (age exact, race confirmee), 14 (pedigree), 31 (GNN -- entity attributes), 33 (survival -- date_mort) |
| **Priority** | HIGH |
| **Accessibility** | FREE API |
| **Status** | ALREADY HAVE |

#### C2. IFCE Stats & Cartes
| Field | Value |
|-------|-------|
| **Source name** | IFCE Stats & Cartes Dashboard |
| **URL** | https://statscartes.ifce.fr/dashboard/47 |
| **Script** | 20_ifce_stats.py |
| **Data provided** | Stats officielles filiere: par hippodrome, entraineur, jockey (agreges) |
| **Format** | JSON API interne |
| **Modules fed** | 15 (track bias), 18 (field strength), 31 (GNN) |
| **Priority** | MEDIUM |
| **Accessibility** | FREE API |
| **Status** | PARTIALLY HAVE |

#### C3. France Galop -- Valeurs Handicap
| Field | Value |
|-------|-------|
| **Source name** | France Galop Handicap Ratings |
| **URL** | https://www.france-galop.com/en/horses-and-people/ratings |
| **Data provided** | Valeurs handicap officielles (echelle 20-62) |
| **Modules fed** | 09, 18 (field strength -- official ratings), 52 (race simulation) |
| **Priority** | MEDIUM |
| **Accessibility** | SCRAPING (JS heavy, anti-bot) |
| **Status** | PARTIALLY HAVE (via PMU handicap_valeur field) |

#### C4. France Galop -- Statistiques Generales
| Field | Value |
|-------|-------|
| **Source name** | France Galop Stats |
| **URL** | https://www.france-galop.com/fr/statistiques |
| **Data provided** | Classements jockeys, entraineurs, proprietaires, eleveurs |
| **Modules fed** | 09, 13 (synergy), 31 (GNN) |
| **Priority** | LOW |
| **Accessibility** | SCRAPING |
| **Status** | TO ACQUIRE |

#### C5. LeTrot -- Statistiques Officielles
| Field | Value |
|-------|-------|
| **Source name** | LeTrot Stats |
| **URL** | https://www.letrot.com/stats |
| **Data provided** | Classements trot: drivers, entraineurs, eleveurs, hippodromes |
| **Modules fed** | 09, 13, 15 (track bias trot), 31 (GNN) |
| **Priority** | LOW |
| **Accessibility** | SCRAPING |
| **Status** | TO ACQUIRE |

#### C6. LeTrot -- Records de Piste
| Field | Value |
|-------|-------|
| **Source name** | LeTrot Records |
| **URL** | https://www.letrot.com/stats/champrecords/hippodrome |
| **Script** | 18_letrot_records.py |
| **Data provided** | Records par hippodrome, distance, specialite (attele/monte) |
| **Modules fed** | 15 (track bias), 16 (pace profile), 52 (race simulation) |
| **Priority** | MEDIUM |
| **Accessibility** | SCRAPING |
| **Status** | ALREADY HAVE |

#### C7. LeTrot -- Fiches Chevaux
| Field | Value |
|-------|-------|
| **Source name** | LeTrot Fiches |
| **URL** | https://www.letrot.com/fiche-cheval/{nom} |
| **Data provided** | Fiche complete trot: pedigree, carriere, engagements, qualifications |
| **Modules fed** | 14 (pedigree trot), 09, 33 (survival) |
| **Priority** | MEDIUM |
| **Accessibility** | SCRAPING |
| **Status** | PARTIALLY HAVE (via 02b) |

#### C8. LeTrot -- Resultats Courses
| Field | Value |
|-------|-------|
| **Source name** | LeTrot Resultats |
| **URL** | https://www.letrot.com/courses |
| **Script** | 02b_scraper_letrot.py |
| **Data provided** | Courses trot hors PMU, resultats complementaires |
| **Volume** | 36K courses |
| **Modules fed** | 01, 03, 10 |
| **Priority** | MEDIUM |
| **Accessibility** | SCRAPING |
| **Status** | ALREADY HAVE |

---

### D. PEDIGREE / BREEDING SOURCES

#### D1. PedigreeQuery.com
| Field | Value |
|-------|-------|
| **Source name** | PedigreeQuery |
| **URL** | https://www.pedigreequery.com |
| **Script** | 14_pedigree_scraper.py, 36_pedigree_query.py |
| **Data provided** | Pedigree 4 generations (pur-sang seulement) |
| **Volume** | 24,484 / 58K chevaux |
| **Modules fed** | 14 (pedigree features -- inbreeding, sire lines), 31 (GNN -- ancestor graph), 33 (survival) |
| **Priority** | HIGH |
| **Accessibility** | SCRAPING |
| **Status** | PARTIALLY HAVE (42% covered) |

#### D2. Racing Post Bloodstock / Pedigree
| Field | Value |
|-------|-------|
| **Source name** | Racing Post Pedigree |
| **URL** | https://www.racingpost.com/profile/horse/{id}/form |
| **Data provided** | Pedigree + racing stats UK/IRE/FR |
| **Modules fed** | 14, 09 (RPR, Top Speed) |
| **Priority** | MEDIUM |
| **Accessibility** | SCRAPING (protected) |
| **Status** | TO ACQUIRE |

#### D3. Thoroughbred Heritage / Bloodlines
| Field | Value |
|-------|-------|
| **Source name** | Thoroughbred Heritage |
| **URL** | https://www.tbheritage.com |
| **Data provided** | Sire line analysis, progeny statistics, stallion profiles |
| **Modules fed** | 14 (sire performance metrics), 31 (GNN -- bloodline graph) |
| **Priority** | LOW |
| **Accessibility** | SCRAPING |
| **Status** | TO ACQUIRE |

#### D4. Haras Nationaux (IFCE Haras)
| Field | Value |
|-------|-------|
| **Source name** | IFCE Haras / InfoChevaux |
| **URL** | https://infochevaux.ifce.fr |
| **Data provided** | Fiche signaletique, identification, origines, production |
| **Modules fed** | 14, 09, 33 |
| **Priority** | MEDIUM |
| **Accessibility** | FREE API (limited) |
| **Status** | PARTIALLY HAVE (via SIRE) |

#### D5. Stallion Book (France Sire)
| Field | Value |
|-------|-------|
| **Source name** | France Sire |
| **URL** | https://www.france-sire.com |
| **Data provided** | Fiches etalons, stats descendants, index genetiques |
| **Modules fed** | 14 (sire index, breeding value), 31 (GNN) |
| **Priority** | MEDIUM |
| **Accessibility** | SCRAPING |
| **Status** | TO ACQUIRE |

#### D6. Etalons.info
| Field | Value |
|-------|-------|
| **Source name** | Etalons.info |
| **URL** | https://www.etalons.info |
| **Data provided** | Fiches etalons trotteurs, stats juments saillies |
| **Modules fed** | 14 (trot-specific breeding), 31 (GNN) |
| **Priority** | LOW |
| **Accessibility** | SCRAPING |
| **Status** | TO ACQUIRE |

---

### E. WEATHER / METEOROLOGICAL SOURCES

#### E1. Meteostat
| Field | Value |
|-------|-------|
| **Source name** | Meteostat Historical |
| **Script** | 13_meteo_historique.py |
| **Data provided** | Temperature, humidite, vent, precipitations par hippodrome |
| **Volume** | 31,778 courses |
| **Modules fed** | 09, 11, 15 (track bias -- weather impact on going) |
| **Priority** | HIGH |
| **Accessibility** | FREE API |
| **Status** | ALREADY HAVE |

#### E2. Open-Meteo (Meteo France AROME 1.5km)
| Field | Value |
|-------|-------|
| **Source name** | Open-Meteo / Meteo France AROME |
| **URL** | open-meteo.com/en/docs/meteofrance-api |
| **Script** | 35_meteo_france_api.py |
| **Data provided** | Meteo haute resolution (1.5km): temperature, precipitation, vent, humidite sol |
| **Modules fed** | 09, 15 (track bias -- moisture), 52 (race simulation -- wind impact) |
| **Priority** | HIGH |
| **Accessibility** | FREE API |
| **Status** | PARTIALLY HAVE |

#### E3. Open-Meteo Archive API
| Field | Value |
|-------|-------|
| **Source name** | Open-Meteo Historical Archive |
| **URL** | archive-api.open-meteo.com |
| **Script** | fetch_openmeteo_missing.py |
| **Data provided** | Historical weather backfill for missing dates |
| **Modules fed** | 09, 15 |
| **Priority** | MEDIUM |
| **Accessibility** | FREE API |
| **Status** | PARTIALLY HAVE |

#### E4. NASA POWER API
| Field | Value |
|-------|-------|
| **Source name** | NASA POWER |
| **URL** | https://power.larc.nasa.gov/api |
| **Script** | enrichissement_meteo_nasa.py |
| **Data provided** | Solar radiation, soil moisture, wind at altitude, evapotranspiration |
| **Modules fed** | 09 (ground condition proxy), 15 (track bias -- deep moisture) |
| **Priority** | LOW |
| **Accessibility** | FREE API |
| **Status** | PARTIALLY HAVE |

#### E5. Equidia -- Etat du Terrain / Penetrometre
| Field | Value |
|-------|-------|
| **Source name** | Equidia Penetrometre |
| **Script** | patch_terrain_equidia.py |
| **Data provided** | Etat terrain mesure au penetrometre (~100 hippodromes) |
| **Modules fed** | 15 (track bias), 09, 52 (race simulation) |
| **Priority** | MEDIUM |
| **Accessibility** | SCRAPING |
| **Status** | ALREADY HAVE |

#### E6. Meteo-France Infoclimat
| Field | Value |
|-------|-------|
| **Source name** | Infoclimat |
| **URL** | https://www.infoclimat.fr |
| **Data provided** | Donnees meteo communales ultra-precises, historique heures |
| **Modules fed** | 09, 15 |
| **Priority** | LOW |
| **Accessibility** | SCRAPING |
| **Status** | TO ACQUIRE |

---

### F. BETTING EXCHANGES & BOOKMAKERS

#### F1. Smarkets Exchange
| Field | Value |
|-------|-------|
| **Source name** | Smarkets |
| **URL** | https://api.smarkets.com/v3 |
| **Script** | 30_smarkets_exchange.py |
| **Data provided** | Cotes back/lay courses FR, volumes echanges |
| **Modules fed** | 12 (odds -- sharp market), 44 (anomaly -- exchange vs PMU), 47-48 (betting -- true value), 50 (ZURI) |
| **Priority** | HIGH |
| **Accessibility** | FREE API |
| **Status** | PARTIALLY HAVE (script ready, historical backfill needed) |

#### F2. Betfair Exchange (BSP)
| Field | Value |
|-------|-------|
| **Source name** | Betfair Exchange BSP |
| **URL** | https://www.betfair.com/exchange |
| **Data provided** | Betfair Starting Price, volume echanges, odds history |
| **Modules fed** | 12, 44, 47-48, 50 |
| **Priority** | HIGH |
| **Accessibility** | PAID API (Betfair API key required) |
| **Status** | TO ACQUIRE |

#### F3. Betfair Historical Data
| Field | Value |
|-------|-------|
| **Source name** | Betfair Historical Data Download |
| **URL** | https://historicdata.betfair.com |
| **Data provided** | Historical BSP, traded volumes, price movement |
| **Modules fed** | 12, 44, 47-48, 51 (Monte Carlo -- true odds calibration) |
| **Priority** | HIGH |
| **Accessibility** | PAID API (~150EUR/year) |
| **Status** | TO ACQUIRE |

#### F4. Unibet FR
| Field | Value |
|-------|-------|
| **Source name** | Unibet France |
| **URL** | https://www.unibet.fr + Kambi API |
| **Script** | 34_unibet_cotes.py |
| **Data provided** | Cotes bookmaker FR, odds changes |
| **Modules fed** | 12 (multi-source odds), 44 (anomaly -- bookmaker vs pari mutuel) |
| **Priority** | MEDIUM |
| **Accessibility** | SCRAPING (reverse-engineered API) |
| **Status** | PARTIALLY HAVE |

#### F5. ZEturf
| Field | Value |
|-------|-------|
| **Source name** | ZEturf |
| **URL** | https://www.zeturf.fr |
| **Data provided** | Cotes pari mutuel alternatif, rapports, pronostics |
| **Modules fed** | 12, 44 |
| **Priority** | MEDIUM |
| **Accessibility** | SCRAPING |
| **Status** | TO ACQUIRE |

#### F6. Winamax Turf
| Field | Value |
|-------|-------|
| **Source name** | Winamax Hippique |
| **URL** | https://www.winamax.fr/paris-hippiques |
| **Data provided** | Cotes Winamax, offres speciales |
| **Modules fed** | 12, 44 |
| **Priority** | LOW |
| **Accessibility** | SCRAPING |
| **Status** | TO ACQUIRE |

#### F7. Betclic Turf
| Field | Value |
|-------|-------|
| **Source name** | Betclic Hippique |
| **URL** | https://www.betclic.fr/turf |
| **Data provided** | Cotes Betclic hippique |
| **Modules fed** | 12, 44 |
| **Priority** | LOW |
| **Accessibility** | SCRAPING |
| **Status** | TO ACQUIRE |

#### F8. Parions Sport (FDJ)
| Field | Value |
|-------|-------|
| **Source name** | Parions Sport FDJ |
| **URL** | https://www.enligne.parionssport.fdj.fr |
| **Data provided** | Cotes FDJ hippique |
| **Modules fed** | 12, 44 |
| **Priority** | LOW |
| **Accessibility** | SCRAPING |
| **Status** | TO ACQUIRE |

#### F9. Oddschecker (Aggregateur)
| Field | Value |
|-------|-------|
| **Source name** | Oddschecker France |
| **URL** | https://www.oddschecker.com/horse-racing |
| **Data provided** | Comparaison cotes multi-bookmakers |
| **Modules fed** | 12 (consensus odds), 44 (anomaly -- market divergence) |
| **Priority** | MEDIUM |
| **Accessibility** | SCRAPING (anti-bot) |
| **Status** | TO ACQUIRE |

#### F10. Odds Portal
| Field | Value |
|-------|-------|
| **Source name** | Odds Portal |
| **URL** | https://www.oddsportal.com/horse-racing |
| **Data provided** | Historique cotes multi-bookmakers, mouvements |
| **Modules fed** | 12, 44, 47 |
| **Priority** | MEDIUM |
| **Accessibility** | SCRAPING |
| **Status** | TO ACQUIRE |

---

### G. COMMUNITY / PRONOSTIC SITES

#### G1. Zone-Turf
| Field | Value |
|-------|-------|
| **Source name** | Zone-Turf |
| **URL** | https://zone-turf.fr |
| **Script** | 31_zone_turf.py |
| **Data provided** | Pronostics communautaires, consensus %, stats chevaux |
| **Modules fed** | 44 (anomaly -- crowd vs market), 45 (retour forme), 50 (ZURI), 40 (meta model) |
| **Priority** | MEDIUM |
| **Accessibility** | SCRAPING |
| **Status** | PARTIALLY HAVE |

#### G2. Turfomania
| Field | Value |
|-------|-------|
| **Source name** | Turfomania |
| **URL** | https://turfomania.fr |
| **Script** | 32_turfomania.py |
| **Data provided** | Indice confiance Turfomania, Turf Machine IA, fiches techniques |
| **Modules fed** | 40 (meta model -- IA alternative), 44 (anomaly), 50 (ZURI) |
| **Priority** | MEDIUM |
| **Accessibility** | SCRAPING |
| **Status** | PARTIALLY HAVE |

#### G3. Turf-FR
| Field | Value |
|-------|-------|
| **Source name** | Turf-FR |
| **URL** | https://turf-fr.com |
| **Script** | 33_turf_fr.py |
| **Data provided** | Pronostics presse, % adversaires battus, stats chevaux |
| **Modules fed** | 40 (meta model -- press consensus), 44, 50 |
| **Priority** | MEDIUM |
| **Accessibility** | SCRAPING |
| **Status** | PARTIALLY HAVE |

#### G4. Geny.com (Groupe PMU)
| Field | Value |
|-------|-------|
| **Source name** | Geny.com |
| **URL** | https://geny.com |
| **Script** | 26_geny_scraper.py |
| **Data provided** | Pronostics Geny, stats jockeys detaillees, commentaires experts |
| **Modules fed** | 09, 13 (synergy -- jockey stats), 40 (meta model), 44 (anomaly) |
| **Priority** | HIGH |
| **Accessibility** | SCRAPING |
| **Status** | PARTIALLY HAVE |

#### G5. Canalturf
| Field | Value |
|-------|-------|
| **Source name** | Canalturf |
| **URL** | https://canalturf.com |
| **Script** | 24_canalturf_scraper.py |
| **Data provided** | Fiches chevaux detaillees: pedigree, stats PMU, historique, rapports |
| **Modules fed** | 14 (pedigree), 09, 33 (survival), 18 (field strength) |
| **Priority** | MEDIUM |
| **Accessibility** | SCRAPING |
| **Status** | PARTIALLY HAVE |

#### G6. Turfostats
| Field | Value |
|-------|-------|
| **Source name** | Turfostats |
| **URL** | https://turfostats.com |
| **Script** | 25_turfostats_scraper.py |
| **Data provided** | Keyrace index, style de course, affinite distance |
| **Modules fed** | 15 (track bias), 16 (pace profile), 18 (field strength) |
| **Priority** | MEDIUM |
| **Accessibility** | SCRAPING |
| **Status** | PARTIALLY HAVE |

#### G7. Boturfers
| Field | Value |
|-------|-------|
| **Source name** | Boturfers |
| **URL** | https://www.boturfers.fr |
| **Script** | 19_boturfers_stats.py |
| **Data provided** | Stats par hippodrome (nb courses/an, rapport moyen, discipline dominante) |
| **Modules fed** | 15 (track bias), 52 (race simulation) |
| **Priority** | LOW |
| **Accessibility** | SCRAPING |
| **Status** | ALREADY HAVE |

#### G8. Pronostics Gratuits (multiple)
| Field | Value |
|-------|-------|
| **Source name** | Sites pronostics gratuits (tierce-magazine, zeturf-pronostic, paris-turf) |
| **Data provided** | Pronostics consensus, bases solides, outsiders cites |
| **Modules fed** | 40 (meta model), 50 (ZURI) |
| **Priority** | LOW |
| **Accessibility** | SCRAPING |
| **Status** | TO ACQUIRE |

#### G9. Forumpmu / TurfOO
| Field | Value |
|-------|-------|
| **Source name** | Forums communautaires (Forumpmu, TurfOO) |
| **Data provided** | Avis pronostiqueurs amateurs, tuyaux, discussions |
| **Modules fed** | 44 (anomaly -- NLP sentiment), 50 (ZURI -- crowd whispers) |
| **Priority** | LOW |
| **Accessibility** | SCRAPING |
| **Status** | TO ACQUIRE |

#### G10. Turf BZH
| Field | Value |
|-------|-------|
| **Source name** | Turf BZH Export CSV |
| **URL** | https://www.turf.bzh/export-journee.php |
| **Data provided** | Export complet journee (58 colonnes): partants, rapports SG/SP, musique, poids |
| **Modules fed** | 01 (ingestion), 03 (dataset), 12 (rapports historiques), 44 (anomaly), 47 (ROI) |
| **Priority** | HIGH |
| **Accessibility** | SCRAPING (communautaire, faible protection) |
| **Status** | PARTIALLY HAVE |

---

### H. EXPERT / PRESS SOURCES

#### H1. Paris-Turf
| Field | Value |
|-------|-------|
| **Source name** | Paris-Turf |
| **URL** | https://www.paris-turf.com |
| **Data provided** | Pronostics experts, analyse course par course, cotes conseillees |
| **Modules fed** | 40 (meta model -- expert overlay), 44, 50 |
| **Priority** | MEDIUM |
| **Accessibility** | PAID API / SCRAPING |
| **Status** | TO ACQUIRE |

#### H2. Week-End (journal hippique)
| Field | Value |
|-------|-------|
| **Source name** | Week-End Hippique |
| **URL** | https://www.journal-weekend.fr |
| **Data provided** | Pronostics journal reference, avis entraineurs |
| **Modules fed** | 40, 44, 50 |
| **Priority** | LOW |
| **Accessibility** | PAID / SCRAPING |
| **Status** | TO ACQUIRE |

#### H3. Tierce Magazine
| Field | Value |
|-------|-------|
| **Source name** | Tierce Magazine |
| **URL** | https://www.tierce-magazine.com |
| **Data provided** | Pronostics, bases, outsiders, tocards |
| **Modules fed** | 40, 50 |
| **Priority** | LOW |
| **Accessibility** | SCRAPING |
| **Status** | TO ACQUIRE |

#### H4. Equidia (Videos + Commentaires)
| Field | Value |
|-------|-------|
| **Source name** | Equidia TV |
| **URL** | https://www.equidia.fr |
| **Data provided** | Replays, commentaires avant-course, analyses experts, avis entraineurs |
| **Modules fed** | 44 (NLP -- expert commentary), 45 (retour forme -- trainer comments), 50 |
| **Priority** | MEDIUM |
| **Accessibility** | SCRAPING / MANUAL |
| **Status** | TO ACQUIRE |

#### H5. PMU Commentaire Apres-Course
| Field | Value |
|-------|-------|
| **Source name** | PMU Commentaires |
| **Data provided** | commentaire_apres_course field in partants data |
| **Modules fed** | 44 (NLP), 45 (retour forme -- race incident analysis) |
| **Priority** | MEDIUM |
| **Accessibility** | FREE API (already in partants data) |
| **Status** | ALREADY HAVE |

#### H6. PMU Avis Entraineur
| Field | Value |
|-------|-------|
| **Source name** | PMU Avis Entraineur |
| **Data provided** | avis_entraineur field in partants data |
| **Modules fed** | 44 (NLP sentiment), 45 (retour forme -- trainer confidence), 50 |
| **Priority** | MEDIUM |
| **Accessibility** | FREE API (already in partants data) |
| **Status** | ALREADY HAVE |

---

### I. INTERNATIONAL RACING DATABASES

#### I1. rpscrape (Racing Post Scraper)
| Field | Value |
|-------|-------|
| **Source name** | rpscrape / Racing Post |
| **URL** | https://github.com/joenano/rpscrape |
| **Script** | 37_rpscrape_racing_post.py |
| **Data provided** | RPR (Racing Post Rating), Top Speed, going, draw, class, prize, OR |
| **Coverage** | Galop FR flat+jumps only (no trot) |
| **Modules fed** | 09, 17 (sectional -- Top Speed), 18 (field strength -- RPR), 52 (race simulation) |
| **Priority** | HIGH |
| **Accessibility** | SCRAPING (anti-bot, needs account) |
| **Status** | PARTIALLY HAVE |

#### I2. Timeform
| Field | Value |
|-------|-------|
| **Source name** | Timeform |
| **URL** | https://www.timeform.com |
| **Data provided** | Timeform ratings (independent), speed figures, commentary |
| **Modules fed** | 09, 17, 18, 40 (meta model) |
| **Priority** | MEDIUM |
| **Accessibility** | PAID API (subscription) |
| **Status** | TO ACQUIRE |

#### I3. Sporting Life
| Field | Value |
|-------|-------|
| **Source name** | Sporting Life |
| **URL** | https://www.sportinglife.com/racing |
| **Data provided** | Tips, race cards, results UK/IRE/FR |
| **Modules fed** | 40 (meta model), 09 |
| **Priority** | LOW |
| **Accessibility** | SCRAPING |
| **Status** | TO ACQUIRE |

#### I4. At The Races
| Field | Value |
|-------|-------|
| **Source name** | At The Races |
| **URL** | https://www.attheraces.com |
| **Data provided** | UK/IRE race data, tips, form |
| **Modules fed** | 40, 09 |
| **Priority** | LOW |
| **Accessibility** | SCRAPING |
| **Status** | TO ACQUIRE |

#### I5. TrotStats International
| Field | Value |
|-------|-------|
| **Source name** | International trot databases (Travsport, etc.) |
| **URL** | Various (travsport.se, ustrotting.com, etc.) |
| **Data provided** | International trot results, times, pedigree |
| **Modules fed** | 09, 14, 17 |
| **Priority** | LOW |
| **Accessibility** | SCRAPING |
| **Status** | TO ACQUIRE |

#### I6. Aspiturf
| Field | Value |
|-------|-------|
| **Source name** | Aspiturf |
| **URL** | https://aspiturf.com |
| **Data provided** | Base complete depuis 2014, CSV par course |
| **Modules fed** | 01, 03 (cross-validation PMU), 04 (quality monitor) |
| **Priority** | MEDIUM |
| **Accessibility** | SCRAPING (inscription gratuite + session) |
| **Status** | TO ACQUIRE |

---

### J. COMMERCIAL / SALES DATA

#### J1. Arqana -- Ventes de Chevaux
| Field | Value |
|-------|-------|
| **Source name** | Arqana |
| **URL** | https://arqana.com |
| **Script** | 29_arqana_ventes.py |
| **Data provided** | Prix d'achat, vendeur, acheteur, lot, catalogue, yearling/breeding |
| **Modules fed** | 09 (commercial value feature), 14 (breeding value -- auction price proxy), 31 (GNN -- owner/buyer graph) |
| **Priority** | MEDIUM |
| **Accessibility** | SCRAPING |
| **Status** | PARTIALLY HAVE |

#### J2. Tattersalls (UK/IRE Sales)
| Field | Value |
|-------|-------|
| **Source name** | Tattersalls |
| **URL** | https://www.tattersalls.com |
| **Data provided** | Sale prices UK/IRE horses (some racing in FR) |
| **Modules fed** | 09, 14, 31 |
| **Priority** | LOW |
| **Accessibility** | SCRAPING |
| **Status** | TO ACQUIRE |

#### J3. Goffs (IRE Sales)
| Field | Value |
|-------|-------|
| **Source name** | Goffs |
| **URL** | https://www.goffs.com |
| **Data provided** | Irish bloodstock sales |
| **Modules fed** | 09, 14 |
| **Priority** | LOW |
| **Accessibility** | SCRAPING |
| **Status** | TO ACQUIRE |

---

### K. KAGGLE / OPEN DATASETS

#### K1. Kaggle Nanaelie PMU Dataset
| Field | Value |
|-------|-------|
| **Source name** | Kaggle Nanaelie |
| **URL** | https://www.kaggle.com/datasets/nanaelie/historical-pmu-horse-racing-dataset |
| **Script** | 15_download_external_datasets.py |
| **Data provided** | Dataset PMU historique bulk (2004-2026) |
| **Modules fed** | 01 (validation/backup), 03 (historical builder -- gap filling) |
| **Priority** | MEDIUM |
| **Accessibility** | FREE API (Kaggle CLI) |
| **Status** | ALREADY HAVE |

#### K2. Kaggle Nanaelie 2004-2013
| Field | Value |
|-------|-------|
| **Source name** | Nanaelie Open PMU |
| **Script** | 16_collecte_nanaelie_2004_2013.py |
| **Data provided** | Arrivees top 5 historiques 2004-2013 |
| **Volume** | 3,295 courses |
| **Modules fed** | 03, 10 (extended rolling windows) |
| **Priority** | MEDIUM |
| **Accessibility** | FREE API |
| **Status** | ALREADY HAVE |

#### K3. Other Kaggle Horse Racing Datasets
| Field | Value |
|-------|-------|
| **Source name** | Kaggle misc horse racing |
| **Data provided** | Various -- UK flat, US thoroughbred, Australian racing |
| **Modules fed** | 09 (international feature comparison), 40 (meta model -- transfer learning) |
| **Priority** | LOW |
| **Accessibility** | FREE API |
| **Status** | TO ACQUIRE |

---

### L. VIDEO / TRACKING / BIOMECHANICS (FUTURE)

#### L1. Equidia Replays (Video Analysis)
| Field | Value |
|-------|-------|
| **Source name** | Equidia Race Replays |
| **URL** | https://www.equidia.fr/replay |
| **Data provided** | Video replays of races (potential for CV-based pace analysis) |
| **Modules fed** | 16 (pace profile -- visual pace), 52 (race simulation -- running line) |
| **Priority** | LOW (requires CV pipeline) |
| **Accessibility** | SCRAPING / PAID |
| **Status** | TO ACQUIRE |

#### L2. GPS Tracking (France Galop / LeTrot)
| Field | Value |
|-------|-------|
| **Source name** | GPS Tracking Data |
| **Data provided** | Real-time position, speed, stride data |
| **Modules fed** | 16, 17 (sectional -- real splits), 52 (race simulation) |
| **Priority** | HIGH (if available) |
| **Accessibility** | NOT AVAILABLE publicly |
| **Status** | TO ACQUIRE (when released) |

#### L3. Stride Analysis / Biomechanics
| Field | Value |
|-------|-------|
| **Source name** | Biomechanical Data |
| **Data provided** | Stride length, frequency, ground contact time |
| **Modules fed** | 09, 16, 52 |
| **Priority** | HIGH (if available) |
| **Accessibility** | NOT AVAILABLE publicly |
| **Status** | TO ACQUIRE (when released) |

#### L4. Trot Gait Analysis
| Field | Value |
|-------|-------|
| **Source name** | Trot Gait / DQ Analysis |
| **Data provided** | Allure analysis, disqualification risk patterns |
| **Modules fed** | 09, 44 (anomaly -- DQ prediction) |
| **Priority** | MEDIUM |
| **Accessibility** | NOT AVAILABLE |
| **Status** | TO ACQUIRE |

---

## 3. PIPELINE MODULE DEPENDENCY MATRIX

### Phase 01 -- Infrastructure (Modules 1-8)

| Module | Name | Primary Sources | Secondary Sources |
|--------|------|----------------|-------------------|
| 01 | data_ingestion_manager | PMU API (A1-A12), Turf BZH (G10), SIRE (C1), Kaggle (K1-K2) | All scrapers (24-37) |
| 02 | data_schema_validator | Generated from ingested data | -- |
| 03 | historical_dataset_builder | PMU 02+02b merged, Nanaelie (K2), LeTrot (C8) | Aspiturf (I6), Turf BZH (G10) |
| 04 | data_quality_monitor | All sources (cross-validation) | -- |
| 05 | missing_values_handler | All feature matrices | -- |
| 06 | outlier_cleaner | Cotes (A8), poids (A10), temps (A11) | -- |
| 07 | data_normalizer | All numeric features | -- |
| 08 | cache_manager | All sources | -- |

### Phase 02 -- Feature Engineering (Modules 9-18)

| Module | Name | Primary Sources | Secondary Sources |
|--------|------|----------------|-------------------|
| 09 | advanced_feature_generator | PMU partants (A1), SIRE (C1) | rpscrape (I1), Arqana (J1) |
| 10 | rolling_stats_generator | PMU historique 2013-2026 (A1), Perfs detaillees (B1) | Nanaelie 2004-2013 (K2) |
| 11 | temporal_feature_builder | PMU calendrier (A2), reunions enrichies (B5) | -- |
| 12 | odds_feature_builder | PMU cotes (A8), rapports (A4-A5), citations (B3), combinaisons (B4) | Smarkets (F1), Betfair (F2-F3), Unibet (F4), Turf BZH (G10), Oddschecker (F9) |
| 13 | jockey_trainer_synergy_builder | PMU historique jockeys/entraineurs (A7), Geny (G4) | IFCE Stats (C2), France Galop (C4), LeTrot Stats (C5) |
| 14 | pedigree_feature_builder | PedigreeQuery (D1), SIRE (C1), PMU pedigree (A12) | France Sire (D5), Etalons.info (D6), Racing Post (D2), Canalturf (G5) |
| 15 | track_bias_detector | PMU historique, LeTrot records (C6), Boturfers (G7), IFCE Stats (C2) | Equidia penetrometre (E5), meteo (E1-E4) |
| 16 | pace_profile_builder | PMU sectionals (A11), perfs detaillees (B1) | LeTrot records (C6), Turfostats (G6) |
| 17 | sectional_feature_builder | PMU sectionals (A11), rpscrape Top Speed (I1) | Timeform (I2) |
| 18 | field_strength_builder | PMU cotes (A8), historiques (A6-A7) | rpscrape RPR (I1), Turfostats (G6), France Galop ratings (C3) |

### Phase 03 -- Feature Selection (Modules 19-20)

| Module | Name | Primary Sources |
|--------|------|----------------|
| 19 | selection_auto_features | Output of Phase 02 (feature matrix) |
| 20 | feature_subset_optimizer | Labels + feature matrix |

### Phase 04 -- ML Core (Modules 21-25)

| Module | Name | Primary Sources |
|--------|------|----------------|
| 21 | logistic_regression | Feature matrix (Phase 02-03 output) |
| 22 | random_forest | Feature matrix |
| 23 | xgboost | Feature matrix |
| 24 | lightgbm | Feature matrix |
| 25 | catboost | Feature matrix |

> All ML core models benefit from more features. Key differentiators: more odds sources (F1-F10), more pedigree depth (D1-D6), more performance data (B1, I1).

### Phase 05 -- Deep Learning (Modules 26-30)

| Module | Name | Primary Sources | Critical Data Needs |
|--------|------|----------------|---------------------|
| 26 | mlp | Feature matrix | Same as Phase 04 |
| 27 | lstm | PMU historique (sequential), perfs detaillees (B1) | SEQUENCES: ordered past N races per horse |
| 28 | gru | Same as LSTM | SEQUENCES |
| 29 | tabnet | Feature matrix | Same as Phase 04 |
| 30 | tft | Historique sequentiel + meteo sequentielle (E1-E3) + static features | SEQUENCES + static context |

### Phase 06 -- Advanced Models (Modules 31-34)

| Module | Name | Primary Sources | Critical Data Needs |
|--------|------|----------------|---------------------|
| 31 | gnn | PMU (all entity relationships), SIRE (C1), IFCE (C2), Arqana (J1) | GRAPH: jockey-cheval-entraineur-hippodrome-pere-mere |
| 32 | bayesian_nn | Feature matrix + uncertainty estimates | Feature distributions |
| 33 | survival_model | PMU historique, SIRE date_mort/naissance (C1) | Time-to-event: career duration, inter-race intervals |
| 34 | quantile_regressor | Feature matrix + labels | Performance distributions |

### Phase 07 -- AutoML (Modules 35-37)

| Module | Name | Primary Sources |
|--------|------|----------------|
| 35 | autogluon | Complete feature matrix |
| 36 | tpot | Complete feature matrix |
| 37 | h2o | Complete feature matrix |

### Phase 08 -- Fusion (Modules 38-40)

| Module | Name | Primary Sources | Secondary Sources |
|--------|------|----------------|-------------------|
| 38 | stacking | Predictions from Phase 04-07 | -- |
| 39 | blending | Predictions from Phase 04-07 | -- |
| 40 | meta_model | All model predictions | External predictions: Geny (G4), Turfomania (G2), Turf-FR (G3), Zone-Turf (G1), Paris-Turf (H1), Timeform (I2) |

### Phase 09 -- Calibration (Modules 41-43)

| Module | Name | Primary Sources |
|--------|------|----------------|
| 41 | calibration_inter_blocs | Predicted probas vs actual results (PMU A3) |
| 42 | platt_scaling | Predicted probas vs actual |
| 43 | isotonic_calibration | Predicted probas vs actual |

### Phase 10 -- Outsiders (Modules 44-46)

| Module | Name | Primary Sources | Secondary Sources |
|--------|------|----------------|-------------------|
| 44 | anomalie_detector | PMU cotes (A8), Smarkets (F1), Betfair (F2), Unibet (F4), rapports (A4-A5), citations (B3), Turf BZH (G10) | Pronostics: Geny (G4), Zone-Turf (G1), Turfomania (G2) |
| 45 | retour_forme_hidden | Perfs detaillees (B1), PMU historique, avis entraineur (H6), commentaire apres-course (H5) | Equipment changes (A9), jockey changes |
| 46 | gan_turf | Complete dataset (all sources) | -- |

### Phase 11 -- Betting (Modules 47-50)

| Module | Name | Primary Sources | Secondary Sources |
|--------|------|----------------|-------------------|
| 47 | roi_predictor | Turf BZH rapports (G10), PMU rapports (A4), historique rapports | Betfair BSP (F2-F3) |
| 48 | value_hunter_rl | Multi-source cotes (A8, F1-F10) vs model probas | -- |
| 49 | meta_selector | Performance logs, monitoring metrics | -- |
| 50 | ZURI_OUTSIDER_ENGINE | All Phase 10 outputs, pronostics (G1-G9), expert sources (H1-H4) | Citations (B3), cotes divergences |

### Phase 12 -- Simulation (Modules 51-52)

| Module | Name | Primary Sources | Secondary Sources |
|--------|------|----------------|-------------------|
| 51 | monte_carlo | Feature distributions, historique | Betfair BSP (F3) for true odds calibration |
| 52 | race_simulation | LeTrot records (C6), sectionals (A11), meteo (E1-E5), perfs detaillees (B1) | GPS/tracking (L2, future) |

### Phase 13 -- Bet Sizing (Modules 53-57)

| Module | Name | Primary Sources |
|--------|------|----------------|
| 53 | bet_sizing_engine | Calibrated probas (Phase 09) + live market cotes |
| 54 | kelly_strategy | Same |
| 55 | fractional_kelly | Same |
| 56 | ticket_optimizer | Rapports probables (B4), combinaisons marche (B4) |
| 57 | tickets_combines | Same + multi-race correlations |

### Phase 14 -- Adaptation (Modules 58-60)

| Module | Name | Primary Sources |
|--------|------|----------------|
| 58 | auto_recalibration | PMU resultats recents (A3) |
| 59 | model_decay_detector | Performance logs |
| 60 | concept_drift_detector | Feature distributions over time |

### Phase 15 -- Monitoring (Modules 61-63)

| Module | Name | Primary Sources |
|--------|------|----------------|
| 61 | telemetrie | All module outputs |
| 62 | dashboard | All metrics |
| 63 | alert_manager | Thresholds + metrics |

### Phase 16 -- Orchestration (Modules 64-68)

| Module | Name | Primary Sources |
|--------|------|----------------|
| 64 | orchestrateur | PMU calendrier (A2), all module configs |
| 65 | workflow_dependency_manager | Module dependency graph |
| 66 | job_scheduler | Schedule configs |
| 67 | multi_model_controller | Model registry |
| 68 | failover_manager | Health metrics |

---

## 4. ACQUISITION PRIORITIES

### TIER 1 -- Critical (HIGH impact, do first)

| # | Source | Impact | Effort | Modules Impacted | Status |
|---|--------|--------|--------|------------------|--------|
| 1 | PMU Performances Detaillees (B1) | +15 features, LSTM/GRU/TFT critical | 20-30h scraping | 09, 10, 27-28, 30, 45 | PARTIALLY (31%) |
| 2 | Betfair Historical BSP (F3) | True market odds, value detection | ~150EUR/yr + setup | 12, 44, 47-48, 51 | TO ACQUIRE |
| 3 | Smarkets Exchange (F1) | Sharp exchange odds FR | Script ready, run needed | 12, 44, 47-48, 50 | PARTIALLY |
| 4 | PMU Citations & Enjeux (B3) | Crowd wisdom, money flow | ~20h scraping | 12, 44, 47-48, 50 | PARTIALLY |
| 5 | PMU Combinaisons Marche (B4) | Rapport probable, bet sizing | ~20h scraping | 12, 47-48, 53-57 | PARTIALLY |
| 6 | rpscrape Racing Post (I1) | RPR, Top Speed ratings | 8-12h scraping | 09, 17, 18, 52 | PARTIALLY |
| 7 | Turf BZH Historical (G10) | Rapports definitifs historiques | ~4h scraping | 01, 03, 12, 44, 47 | PARTIALLY |

### TIER 2 -- Important (MEDIUM impact)

| # | Source | Impact | Effort | Modules Impacted | Status |
|---|--------|--------|--------|------------------|--------|
| 8 | PedigreeQuery completion (D1) | Deep pedigree 58% missing | ~20h scraping | 14, 31, 33 | PARTIALLY (42%) |
| 9 | Unibet cotes (F4) | Bookmaker vs mutuel comparison | Script ready | 12, 44 | PARTIALLY |
| 10 | Geny.com stats (G4) | Expert pronostics + jockey stats | ~10h scraping | 09, 13, 40, 44 | PARTIALLY |
| 11 | Open-Meteo backfill (E3) | Fill 88% meteo gap | ~2h API calls | 09, 15 | PARTIALLY |
| 12 | IFCE Stats & Cartes (C2) | Official industry stats | ~1h API | 15, 18, 31 | PARTIALLY |
| 13 | Zone-Turf consensus (G1) | Community predictions | ~6h scraping | 44, 45, 50 | PARTIALLY |
| 14 | France Sire (D5) | Stallion breeding indices | ~8h scraping | 14, 31 | TO ACQUIRE |
| 15 | Aspiturf (I6) | Cross-validation dataset | ~6h scraping | 01, 03, 04 | TO ACQUIRE |

### TIER 3 -- Nice to Have (LOW impact or hard to get)

| # | Source | Impact | Effort | Status |
|---|--------|--------|--------|--------|
| 16 | Oddschecker (F9) | Multi-bookmaker comparison | Anti-bot heavy | TO ACQUIRE |
| 17 | Betfair live API (F2) | Real-time exchange odds | API key required | TO ACQUIRE |
| 18 | Timeform ratings (I2) | Independent speed figures | Paid subscription | TO ACQUIRE |
| 19 | Paris-Turf experts (H1) | Expert layer for meta model | Paid / scraping | TO ACQUIRE |
| 20 | France Galop ratings (C3) | Official handicap ratings | Heavy anti-bot JS | PARTIAL (via PMU) |
| 21 | Arqana completion (J1) | Sale price features | ~4h scraping | PARTIALLY |
| 22 | Equidia replays (L1) | Video-based pace analysis | CV pipeline needed | TO ACQUIRE |
| 23 | Tattersalls/Goffs (J2-J3) | International sale prices | Low FR coverage | TO ACQUIRE |
| 24 | International trot (I5) | Cross-border trot data | Multiple sources | TO ACQUIRE |
| 25 | Forum NLP (G9) | Crowd sentiment | NLP pipeline needed | TO ACQUIRE |

### Summary by Status

| Status | Count | % |
|--------|-------|---|
| ALREADY HAVE | 22 | 34% |
| PARTIALLY HAVE | 21 | 32% |
| TO ACQUIRE | 22 | 34% |
| **Total catalogued** | **65 distinct sources** | 100% |

> Note: The 65 distinct sources map to 210+ individual data endpoints when counting
> sub-APIs (e.g., PMU has 12+ endpoints), per-hippodrome scraping targets,
> historical vs live variants, and multiple betting exchange markets.

### Estimated Total Acquisition Effort

| Phase | Sources | Time | Cost |
|-------|---------|------|------|
| Immediate (already scripted) | B1-B5, F1, G10, E3 | ~80h scraping | FREE |
| Short term (scripts to write) | D1 completion, I1, G4-G6, D5 | ~40h dev + scraping | FREE |
| Medium term (paid/complex) | F2-F3, I2, H1 | ~10h setup | ~300EUR/yr |
| Long term (R&D) | L1-L4, G9 NLP, CV pipeline | ~100h R&D | TBD |

---

*Document generated 2026-03-15. Review quarterly as new sources become available.*
