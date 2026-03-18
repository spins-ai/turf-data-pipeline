# Sources de donnees

Liste exhaustive des 40+ sources de donnees du pipeline hippique, groupees par categorie.

---

## A. PMU -- API Officielles (Sources principales)

| # | Script | Source | URL / Endpoint | Type | Records | Champs principaux | Statut |
|---|--------|--------|----------------|------|---------|-------------------|--------|
| 1 | 01_calendrier_reunions.py | PMU Calendrier | `offline.turfinfo.api.pmu.fr/rest/client/7/programme/{date}` | API JSON | 41,477 reunions | date, hippodrome, discipline, nb_courses, pays | Termine (686 MB) |
| 2 | 02_liste_courses.py | PMU Courses + Partants | `offline.turfinfo.api.pmu.fr/.../participants` | API JSON | 257,806 courses / 2,930,290 partants | Tous les champs partants (66), conditions, cotes, musique, equipements, poids | Termine (14 GB) |
| 3 | 04_resultats.py | PMU Resultats | `offline.turfinfo.api.pmu.fr/.../resultats` | API JSON | ~217K courses | position_arrivee, ecarts, temps, rapports | En cours (~2.1 GB) |
| 4 | 05_historique_chevaux.py | PMU Historique Chevaux | `offline.turfinfo.api.pmu.fr/.../historiquePerformances` | API JSON | 80,656 chevaux | nb_courses, victoires, gains, disciplines par cheval | Termine (324 MB) |
| 5 | 06_historique_jockeys.py | PMU Historique Jockeys | `offline.turfinfo.api.pmu.fr/.../historique` | API JSON | 12,319 jockeys + ~5,000 entraineurs | victoires, places, gains, chevaux montes | Termine (14 MB) |
| 6 | 07_cotes_marche.py | PMU Cotes Marche | `offline.turfinfo.api.pmu.fr/.../cotes` | API JSON | ~2.7M partants | cote_finale, cote_reference, proba_implicite, surcote | Termine (286 MB) |
| 7 | 08_pedigree.py | PMU Pedigree Parents | `offline.turfinfo.api.pmu.fr/.../pedigree` | API JSON | Tous partants | pere, mere, pere_mere (1 generation) | Termine (20 MB) |
| 8 | 09_equipements.py | PMU Equipements | `offline.turfinfo.api.pmu.fr/.../equipements` | API JSON | 573,111 partants | oeilleres, deferre, changements equipement | Termine (319 MB) |
| 9 | 10_poids_handicaps.py | PMU Poids / Handicaps | `offline.turfinfo.api.pmu.fr/.../poids` | API JSON | ~2.7M partants | poids_porte, poids_base, surcharge/decharge, handicap_valeur | Termine (141 MB) |
| 10 | 11_sectionals.py | PMU Sectionals | `offline.turfinfo.api.pmu.fr/.../sectionals` | API JSON | Courses trackees | reduction_km, vitesse, ecarts, temps sections | Termine (133 MB) |

---

## B. PMU -- Endpoints derives / internes

| # | Script | Source | URL / Endpoint | Type | Records | Champs principaux | Statut |
|---|--------|--------|----------------|------|---------|-------------------|--------|
| 11 | 21_rapports_definitifs.py | PMU Rapports Nationaux | `offline.turfinfo.api.pmu.fr/.../rapports-definitifs` | API JSON | 124,287 courses | rapports simple/couple/tierce/quarte/quinte, mise base 200 | En cours (240K JSONL) |
| 12 | 22_performances_detaillees.py | PMU Performances Detaillees | `online.turfinfo.api.pmu.fr/.../performances-detaillees/pretty` | API JSON | 917,805 partants | 5 dernieres performances (date, hippo, distance, position, ecart, temps, terrain, cote) | En cours (12 GB) |
| 13 | 27_citations_enjeux.py | PMU Citations / Enjeux | `offline.turfinfo.api.pmu.fr/.../citations` | API JSON | ~144K/300K (48%) | enjeux par cheval, citations experts | En cours (5.9 GB JSONL) |
| 14 | 28_combinaisons_marche.py | PMU Combinaisons | `offline.turfinfo.api.pmu.fr/.../combinaisons` | API JSON | 5,700,000 | masse d'enjeux par combinaison | Termine (2.3 GB JSONL) |
| 15 | 38_rapports_internet.py | PMU Rapports Internet | `offline.turfinfo.api.pmu.fr/.../rapports-definitifs?specialisation=INTERNET` | API JSON | 3,200,000 | rapports e-paris, mise base 100 | Termine (1.9 GB JSONL) |
| 16 | 39_reunions_enrichies.py | PMU Reunions Enrichies | `offline.turfinfo.api.pmu.fr/.../R{r}` | API JSON | ~230K | meteo, incidents, conditions reunion, masse enjeux | En cours (2 GB JSONL) |
| 17 | 40_enrichissement_partants.py | PMU Enrichissement | PMU API (champs supplementaires) | API JSON | ~2.5M | gainsParticipant, dernierRapport, idCheval, nomPereMere, handicapValeur, eleveur, race, robe | Termine (655 MB) |
| 18 | 23_pronostics_equidia.py | PMU/Equidia Pronostics | PMU pronostics endpoint | API JSON | ~110K | pronostics officiels | En cours (431 MB) |

---

## C. Donnees institutionnelles / Open Data

| # | Script | Source | URL / Endpoint | Type | Records | Champs principaux | Statut |
|---|--------|--------|----------------|------|---------|-------------------|--------|
| 19 | 17_process_sire.py | SIRE / IFCE | `data.gouv.fr/datasets/fichier-des-equides` | CSV Open Data | 4,000,000 equides | race, sexe, robe, date_naissance, pays_naissance, nom | Termine (1.1 GB) |
| 20 | 20_ifce_stats.py | IFCE Stats & Cartes | `statscartes.ifce.fr/dashboard/47` | JSON API | ~5,000 | stats par hippodrome, entraineur, jockey | Termine (252 KB) |

---

## D. Pedigree / Elevage

| # | Script | Source | URL / Endpoint | Type | Records | Champs principaux | Statut |
|---|--------|--------|----------------|------|---------|-------------------|--------|
| 21 | 12_pedigree_scraper.py | PedigreeQuery.com (galop) | `pedigreequery.com` | Scraping HTML | ~18K/58K chevaux | pedigree 4 generations (sires, dams, grandsires) | Consolider cache |
| 22 | 14_pedigree_scraper.py | PedigreeQuery.com (complet) | `pedigreequery.com` | Scraping HTML | ~89K/250K (35%) | pedigree 4 generations, coat, country | En cours |
| 23 | 36_pedigree_query.py | PedigreeQuery.com (v2) | `pedigreequery.com` | Scraping HTML | 24,484 chevaux | pedigree 4 generations complet | Bloque (Cloudflare) |
| 24 | 29_arqana_ventes.py | Arqana Ventes | `arqana.com` | Scraping HTML | Variable | prix_vente, acheteur, vendeur, lot | A lancer |

---

## E. Meteo / Meteorologie

| # | Script | Source | URL / Endpoint | Type | Records | Champs principaux | Statut |
|---|--------|--------|----------------|------|---------|-------------------|--------|
| 25 | 13_meteo_historique.py | Meteostat | API Meteostat | API JSON | 31,778 courses | temperature, humidite, vent, precipitations | Termine (71 MB) |
| 26 | 35_meteo_france_api.py | Meteo France | `api.meteo.fr` | API JSON | Variable | donnees meteo France (payant) | A relancer |
| 27 | 00_enrichissement_meteo.py | NASA / Open-Meteo | `open-meteo.com` | API JSON | ~12,754 cache | temperature, precipitations, vent mondial | En cours |
| 28 | fetch_openmeteo_missing.py | Open-Meteo (comblement) | `open-meteo.com` | API JSON | Complement | comblement trous meteo mondiales | En cours |

---

## F. Betting Exchange / Bookmakers

| # | Script | Source | URL / Endpoint | Type | Records | Champs principaux | Statut |
|---|--------|--------|----------------|------|---------|-------------------|--------|
| 29 | 30_smarkets_exchange.py | Smarkets Exchange | `api.smarkets.com` | API JSON | ~1,000 | cotes exchange (back/lay), volume | Termine (640 KB) |
| 30 | 34_unibet_cotes.py | Unibet | `unibet.fr` | Scraping | Variable | cotes bookmaker, marches | A lancer |

---

## G. Sites communautaires / Pronostics

| # | Script | Source | URL / Endpoint | Type | Records | Champs principaux | Statut |
|---|--------|--------|----------------|------|---------|-------------------|--------|
| 31 | 24_canalturf_scraper.py | CanalTurf | `canalturf.com` | Scraping HTML | ~9,159 profils | stats cheval, pronostics, historique | Termine (41 MB) |
| 32 | 25_turfostats_scraper.py | TurfoStats | `turfostats.com` | Scraping HTML | ~8,332 entries | stats avancees, indices performance | Termine (27 MB) |
| 33 | 26_geny_scraper.py | Geny Courses | `geny.com` | Scraping HTML | Variable | pronostics, stats, historique | Termine (44 MB) |
| 34 | 31_zone_turf.py | Zone Turf | `zone-turf.fr` | Scraping HTML | Variable | pronostics, base musique | A lancer |
| 35 | 32_turfomania.py | Turfomania | `turfomania.com` | Scraping HTML | Variable | pronostics, stats | A lancer |
| 36 | 33_turf_fr.py | Turf.fr | `turf.fr` | Scraping HTML | Variable | stats, historique | A lancer |
| 37 | 19_boturfers_stats.py | Boturfers | `boturfers.fr` | Scraping HTML | ~250 hippodromes | stats hippodrome, rapport moyen, disciplines | Termine (632 KB) |

---

## H. Courses internationales

| # | Script | Source | URL / Endpoint | Type | Records | Champs principaux | Statut |
|---|--------|--------|----------------|------|---------|-------------------|--------|
| 38 | 37_rpscrape_racing_post.py | Racing Post (UK/IRE) | `racingpost.com` via rpscrape | Scraping | ~10K cache | RPR, Top Speed, going, draw, class, OR | Crashe (patch JSONL fait, a relancer) |

---

## I. Le Trot

| # | Script | Source | URL / Endpoint | Type | Records | Champs principaux | Statut |
|---|--------|--------|----------------|------|---------|-------------------|--------|
| 39 | 02b_scraper_letrot.py | Le Trot (HTML brut) | `letrot.com` | Scraping HTML | ~36K courses | courses trot hors PMU | Termine (15 GB) |
| 40 | 02b_liste_courses_2013.py | Le Trot (historique) | `letrot.com` | Scraping HTML | Variable | donnees 2004-2013 | Termine (3.2 GB) |
| 41 | 18_letrot_records.py | Le Trot Records de piste | `letrot.com/stats` | Scraping HTML | ~236 hippodromes | records par hippodrome/distance/specialite | Termine (152 KB) |

---

## J. Datasets ouverts / Kaggle

| # | Script | Source | URL / Endpoint | Type | Records | Champs principaux | Statut |
|---|--------|--------|----------------|------|---------|-------------------|--------|
| 42 | 16_collecte_nanaelie_2004_2013.py | Kaggle nanaelie PMU | `kaggle.com/datasets/nanaelie/historical-pmu-horse-racing-dataset` | CSV/Parquet | 3,295 courses (2004-2013) | arrivees top 5 historiques | Termine |
| 43 | 15_download_external_datasets.py | Datasets externes divers | Kaggle + autres | CSV | Variable | datasets complementaires | A lancer |

---

## K. Base interne hippodromes

| # | Script | Source | Type | Records | Champs principaux | Statut |
|---|--------|--------|------|---------|-------------------|--------|
| 44 | hippodromes_db.py | Base hippodromes maison | Dictionnaire Python | 673 hippodromes | nom, GPS (lat/lon), altitude, type_piste, corde, region, pays | Termine |

---

## Endpoints PMU utilises (resume)

| # | Endpoint | Donnees | Script(s) |
|---|----------|---------|-----------|
| 1 | `offline.turfinfo.api.pmu.fr/.../participants` | Partants + resultats | 02 |
| 2 | `online.turfinfo.api.pmu.fr/.../rapports-definitifs` | Dividendes nationaux | 21 |
| 3 | `offline.turfinfo.api.pmu.fr/.../rapports-definitifs?specialisation=INTERNET` | E-paris | 38 |
| 4 | `offline.turfinfo.api.pmu.fr/.../performances-detaillees/pretty` | 9 dernieres courses | 22 |
| 5 | `offline.turfinfo.api.pmu.fr/.../citations` | Enjeux par cheval | 27 |
| 6 | `offline.turfinfo.api.pmu.fr/.../combinaisons` | Masse d'enjeux | 28 |
| 7 | `offline.turfinfo.api.pmu.fr/.../R{r}` | Meteo, incidents, conditions reunion | 39 |

---

## Couverture des 10 signaux cles (hedge fund)

| # | Signal | Couverture | Sources |
|---|--------|------------|---------|
| 1 | Cotes bookmakers | PARTIEL | PMU (07), Unibet (34), Smarkets (30) |
| 2 | Resultats historiques | OUI | PMU (02, 02b, 04), Nanaelie (16), Le Trot |
| 3 | Pedigree | PARTIEL | PedigreeQuery (14), PMU (08), SIRE/IFCE (17) |
| 4 | Meteo | OUI | Meteostat (13), Open-Meteo (35) |
| 5 | Terrain / piste | OUI | PMU (penetrometre), Equidia |
| 6 | Vitesse sectionnelle | PARTIEL | PMU sectionals (11), Perf detaillees (22) |
| 7 | Biomecanique | NON | Pas de source publique |
| 8 | Tracking GPS | NON | Pas de source publique en France |
| 9 | Historique jockey | OUI | PMU (06), stats rolling |
| 10 | Historique entraineur | OUI | PMU (06), stats rolling |

**Score : 6/10 complets, 3/10 partiels, 1/10 manquant.**
