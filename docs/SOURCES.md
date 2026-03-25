# Sources de donnees

Liste exhaustive des 115 sources de donnees du pipeline hippique, groupees par categorie.
Derniere mise a jour : 2026-03-25.

---

## A. PMU -- API Officielles (Sources principales)

| # | Source | URL | Records | Size | Last Update | Status |
|---|--------|-----|---------|------|-------------|--------|
| 01 | PMU Calendrier | `offline.turfinfo.api.pmu.fr/.../programme/{date}` | 41,477 reunions | 615 MB | 2026-03-15 | ✅ Active |
| 02 | PMU Courses + Partants | `offline.turfinfo.api.pmu.fr/.../participants` | 2,930,290 partants | 31 GB | 2026-03-22 | ✅ Active |
| 04 | PMU Resultats | `offline.turfinfo.api.pmu.fr/.../resultats` | 1,386,967 rapports | 4.3 GB | 2026-03-22 | ✅ Active |
| 05 | PMU Historique Chevaux | `offline.turfinfo.api.pmu.fr/.../historiquePerformances` | 80,656 chevaux | 325 MB | 2026-03-13 | ✅ Active |
| 06 | PMU Historique Jockeys | `offline.turfinfo.api.pmu.fr/.../historique` | 12,319 jockeys + 11,840 entraineurs | 15 MB | 2026-03-13 | ✅ Active |
| 07 | PMU Cotes Marche | `offline.turfinfo.api.pmu.fr/.../cotes` | 573,111 partants | 287 MB | 2026-03-13 | ✅ Active |
| 08 | PMU Pedigree Parents | `offline.turfinfo.api.pmu.fr/.../pedigree` | Tous partants | 20 MB | 2026-03-13 | ✅ Active |
| 09 | PMU Equipements | `offline.turfinfo.api.pmu.fr/.../equipements` | 573,111 partants | 320 MB | 2026-03-13 | ✅ Active |
| 10 | PMU Poids / Handicaps | `offline.turfinfo.api.pmu.fr/.../poids` | 271,276 partants | 142 MB | 2026-03-13 | ✅ Active |
| 11 | PMU Sectionals | `offline.turfinfo.api.pmu.fr/.../sectionals` | 243,410 courses | 134 MB | 2026-03-13 | ✅ Active |

---

## B. PMU -- Endpoints derives / internes

| # | Source | URL | Records | Size | Last Update | Status |
|---|--------|-----|---------|------|-------------|--------|
| 21 | PMU Rapports Nationaux | `offline.turfinfo.api.pmu.fr/.../rapports-definitifs` | 201,023 courses | 381 MB | 2026-03-22 | ✅ Active |
| 22 | PMU Performances Detaillees | `online.turfinfo.api.pmu.fr/.../performances-detaillees/pretty` | 244,349 partants | 1.3 GB | 2026-03-22 | ✅ Active |
| 23 | PMU/Equidia Pronostics | PMU pronostics endpoint | 204,598 pronostics | 422 MB | 2026-03-22 | ✅ Active |
| 27 | PMU Citations / Enjeux | `offline.turfinfo.api.pmu.fr/.../citations` | 1,500,599 entries | 5.3 GB | 2026-03-22 | ✅ Active |
| 28 | PMU Combinaisons | `offline.turfinfo.api.pmu.fr/.../combinaisons` | 5,413,768 combinaisons | 2.7 GB | 2026-03-22 | ✅ Active |
| 38 | PMU Rapports Internet | `offline.turfinfo.api.pmu.fr/.../rapports-definitifs?specialisation=INTERNET` | 2,799,619 rapports | 1.6 GB | 2026-03-22 | ✅ Active |
| 39 | PMU Reunions Enrichies | `offline.turfinfo.api.pmu.fr/.../R{r}` | 233,719 reunions | 1.4 GB | 2026-03-22 | ✅ Active |
| 40 | PMU Enrichissement Partants | PMU API (champs supplementaires) | 2,678,013 partants | 699 MB | 2026-03-20 | ✅ Active |
| 101 | PMU API v2 (unifie) | `online.turfinfo.api.pmu.fr/rest/client/1` | 1,193,327 participants / 101,638 courses | 1.1 GB | 2026-03-22 | 🔄 In Progress |

---

## C. Donnees institutionnelles / Open Data

| # | Source | URL | Records | Size | Last Update | Status |
|---|--------|-----|---------|------|-------------|--------|
| 17 | SIRE / IFCE | `data.gouv.fr/datasets/fichier-des-equides` | 1,476,670 equides | 1.1 GB | 2026-03-14 | ✅ Active |
| 20 | IFCE Stats & Cartes | `statscartes.ifce.fr` | 16 entries | 212 KB | 2026-03-21 | ✅ Active |
| 80 | France Galop | `france-galop.com` | 44 records | 93 KB | 2026-03-22 | ⚠️ Partial |

---

## D. Pedigree / Elevage

| # | Source | URL | Records | Size | Last Update | Status |
|---|--------|-----|---------|------|-------------|--------|
| 12 | PedigreeQuery (galop) | `pedigreequery.com` | 544 pedigrees | 218 KB | 2026-03-22 | ⚠️ Partial |
| 14 | PedigreeQuery (complet) | `pedigreequery.com` | 29,158 chevaux | 15 MB | 2026-03-22 | ⚠️ Partial |
| 36 | PedigreeQuery (v2) | `pedigreequery.com` | 6,629 chevaux | 3.6 MB | 2026-03-18 | ❌ Blocked |
| 71 | AllBreedPedigree | `allbreedpedigree.com` | 0 | 0 | -- | ❌ Blocked |
| 87 | Bloodstock (BloodHorse/TDN) | `bloodhorse.com` / `thoroughbreddailynews.com` | 0 | 0 | -- | ❌ Blocked |
| 88 | Weatherbys (UK Stud Book) | `weatherbys.co.uk` | 0 | 0 | -- | ❌ Blocked |

---

## E. Meteo / Meteorologie

| # | Source | URL | Records | Size | Last Update | Status |
|---|--------|-----|---------|------|-------------|--------|
| 13 | Meteostat | API Meteostat | 4,551 entries | 4.2 MB | 2026-03-22 | ✅ Active |
| 35 | Meteo France | `api.meteo.fr` | 40 hippodromes | 11 MB | 2026-03-14 | ⚠️ Partial |
| 96 | NOAA Weather | `noaa.gov` | 0 | 0 | -- | ❌ Blocked |
| 97 | Meteostat (v2 bulk) | `meteostat.p.rapidapi.com` | 0 | 0 | -- | ❌ Blocked |

---

## F. Betting Exchange / Bookmakers

| # | Source | URL | Records | Size | Last Update | Status |
|---|--------|-----|---------|------|-------------|--------|
| 30 | Smarkets Exchange | `api.smarkets.com` | 660 events | 313 KB | 2026-03-22 | ✅ Active |
| 51 | ZeTurf | `zeturf.fr` | 413,621 records | 95 MB | 2026-03-22 | ✅ Active |
| 60 | OddsChecker | `oddschecker.com` | 0 | 0 | -- | ❌ Blocked |
| 68 | Betfair Exchange | `betfair.com` | 0 | 0 | -- | ❌ Blocked |
| 69 | OddsPortal | `oddsportal.com` | 0 | 0 | -- | ❌ Blocked |
| 70 | BetExplorer | `betexplorer.com` | 0 | 0 | -- | ❌ Blocked |

---

## G. Sites communautaires / Pronostics (France)

| # | Source | URL | Records | Size | Last Update | Status |
|---|--------|-----|---------|------|-------------|--------|
| 24 | CanalTurf | `canalturf.com` | 9,503 profils | 4.5 MB | 2026-03-22 | ✅ Active |
| 25 | TurfoStats | `turfostats.com` | 6,121 courses | 359 KB | 2026-03-22 | ✅ Active |
| 26 | Geny Courses | `geny.com` | 2,265 entries | 18 MB | 2026-03-22 | ✅ Active |
| 52 | Turfomania | `turfomania.fr` | 6,177 records | 2.3 MB | 2026-03-22 | ✅ Active |
| 53 | Paris Turf | `paris-turf.com` | 32,154 courses | 20 MB | 2026-03-22 | ✅ Active |
| 54 | TurfInfo | `turfinfo.fr` | 0 (cache only) | 0 | 2026-03-21 | ❌ Blocked |
| 55 | Equidia | `equidia.fr` | 1,131 records | 9.2 MB | 2026-03-22 | ⚠️ Partial |
| 81 | Pronosoft | `pronosoft.com` | 0 | 0 | 2026-03-22 | ❌ Blocked |
| 82 | Turf-FR | `turf-fr.com` | 3,062 records | 1.9 MB | 2026-03-22 | ✅ Active |
| 84 | Turfoo | `turfoo.fr` | 5,071 records | 1.7 MB | 2026-03-22 | ✅ Active |
| 19 | Boturfers | `boturfers.fr` | 272 hippodromes | 31 KB | 2026-03-22 | ✅ Active |

---

## H. Courses internationales (UK/IRE)

| # | Source | URL | Records | Size | Last Update | Status |
|---|--------|-----|---------|------|-------------|--------|
| 37 | Racing Post (historique FR) | `racingpost.com` via rpscrape | 3,610,366 records | 1.3 GB | 2026-03-22 | ✅ Active |
| 56 | Timeform | `timeform.com` | 60,343 records | 11 MB | 2026-03-22 | ✅ Active |
| 57 | Sporting Life | `sportinglife.com` | 25,086 records | 5.4 MB | 2026-03-22 | ✅ Active |
| 58 | At The Races | `attheraces.com` | 0 | 0 | -- | ❌ Blocked |
| 59 | Racing TV | `racingtv.com` | 0 | 0 | -- | ❌ Blocked |
| 78 | GoingStick Data | `britishhorseracing.com` / `racingpost.com` | 0 | 0 | 2026-03-22 | ❌ Blocked |
| 79 | Trainer Stats (multi-source) | `racingpost.com` / `sportinglife.com` | 0 | 0 | -- | ❌ Blocked |
| 86 | SmartForm | `smartform.co.uk` | 0 | 0 | -- | ❌ Blocked |
| 91 | EquiRatings | `equiratings.com` | 0 | 0 | -- | ❌ Blocked |
| 93 | Raceform | `raceform.co.uk` | 0 | 0 | -- | ❌ Blocked |
| 98 | TurfTrax | `turftrax.com` | 0 | 0 | -- | ❌ Blocked |
| 99 | Clerk of Course (BHA) | `britishhorseracing.com` | 0 | 0 | -- | ❌ Blocked |
| 102 | Racing Post (v2) | `racingpost.com` | 24 records | 9.5 KB | 2026-03-22 | ⚠️ Partial |
| 85 | Racing and Sports | `racingandsports.com` | 0 | 0 | -- | ❌ Blocked |

---

## I. Courses internationales (Reste du monde)

| # | Source | URL | Records | Size | Last Update | Status |
|---|--------|-----|---------|------|-------------|--------|
| 61 | Equibase (US) | `equibase.com` | 256 records | 47 KB | 2026-03-22 | ⚠️ Partial |
| 62 | Horse Racing Nation (US) | `horseracingnation.com` | 1,514 records | 541 KB | 2026-03-22 | ⚠️ Partial |
| 63 | Daily Racing Form (US) | `drf.com` | 0 | 0 | -- | ❌ Blocked |
| 64 | Punters (AU) | `punters.com.au` | 0 | 0 | -- | ❌ Blocked |
| 65 | Racenet (AU) | `racenet.com.au` | 0 | 0 | -- | ❌ Blocked |
| 66 | HKJC (Hong Kong) | `racing.hkjc.com` | 1,570 records | 664 KB | 2026-03-22 | ✅ Active |
| 67 | JRA (Japan) | `jra.go.jp` | 41 records | 15 KB | 2026-03-22 | ⚠️ Partial |
| 89 | Singapore Pools | `singaporepools.com.sg` | 809 records | 53 KB | 2026-03-22 | ⚠️ Partial |
| 90 | Korea Racing (KRA) | `kra.co.kr` | 0 | 0 | -- | ❌ Blocked |
| 92 | OptixEQ (US) | `optixeq.com` | 0 | 0 | -- | ❌ Blocked |

---

## J. Le Trot

| # | Source | URL | Records | Size | Last Update | Status |
|---|--------|-----|---------|------|-------------|--------|
| 02b | Le Trot (HTML brut) | `letrot.com` | 26,462 courses | 41 MB | 2026-03-22 | ✅ Active |
| 02b_2013 | Le Trot (historique 2004-2013) | `letrot.com` | 89,527 partants | 1.4 GB | 2026-03-19 | ✅ Active |
| 18 | Le Trot Records de piste | `letrot.com/stats` | ~236 hippodromes | 149 KB | 2026-03-15 | ✅ Active |
| 76 | USTA Trotting (US) | `ustrotting.com` | 3,933 records | 727 KB | 2026-03-22 | ✅ Active |
| 83 | Le Trot (v2 corrige) | `letrot.com` | 1,401,487 records | 600 MB | 2026-03-22 | ✅ Active |
| 94 | Harness Australia | `harness.org.au` | 0 | 0 | -- | ❌ Blocked |
| 95 | Standardbred Canada | `standardbredcanada.ca` | 0 | 0 | -- | ❌ Blocked |

---

## K. Ventes aux encheres

| # | Source | URL | Records | Size | Last Update | Status |
|---|--------|-----|---------|------|-------------|--------|
| 72 | Tattersalls (UK) | `tattersalls.com` | 0 | 0 | -- | ❌ Blocked |
| 73 | Goffs (IRE) | `goffs.com` | 0 | 0 | -- | ❌ Blocked |
| 74 | Arqana (FR) | `arqana.com` | 0 | 0 | -- | ❌ Blocked |
| 75 | Keeneland (US) | `keeneland.com` | 2 records | 3.7 KB | 2026-03-22 | ⚠️ Partial |
| 100 | Magic Millions (AU) | `magicmillions.com.au` | 0 (cache only) | 0 | -- | ❌ Blocked |

---

## L. Datasets ouverts / Kaggle

| # | Source | URL | Records | Size | Last Update | Status |
|---|--------|-----|---------|------|-------------|--------|
| 16 | Kaggle nanaelie PMU | `kaggle.com/datasets/nanaelie` | 3,316 courses | 1.1 MB | 2026-03-22 | ✅ Active |
| 77 | Kaggle Datasets (divers) | `kaggle.com` | 0 | 0 | -- | ❌ Blocked |

---

## M. Pipeline interne (transformations)

| # | Source | Type | Records | Size | Last Update | Status |
|---|--------|------|---------|------|-------------|--------|
| 02_raw | PMU raw backup | JSON/JSONL | 2,678,013 partants | 14 GB | 2026-03-21 | ✅ Active |
| 02_merged | Merged intermediate | JSON/JSONL | 2,930,290 partants | 4.7 GB | 2026-03-22 | ✅ Active |
| 41 | Sequences performances | JSONL | 2,930,290 entries | 2.5 GB | 2026-03-22 | ✅ Active |
| 42 | Croisement RP-PMU | JSONL | 2,891,593 entries | 277 MB | 2026-03-22 | ✅ Active |
| 43 | Croisement Meteo-Courses | JSONL | 2,930,290 entries | 1.2 GB | 2026-03-22 | ✅ Active |
| 44 | Croisement Pedigree-Partants | JSONL | 2,930,290 entries | 1.6 GB | 2026-03-22 | ✅ Active |
| 45 | Graphe GNN (nodes+edges) | JSONL | 2,914,985 partants + edges | 302 MB | 2026-03-22 | ✅ Active |
| 46 | Track Bias / Speed Class | JSONL | 2,930,290 entries | 1.3 GB | 2026-03-22 | ✅ Active |
| 48 | Conditions Texte (NLP) | JSONL | 257,806 courses | 122 MB | 2026-03-22 | ✅ Active |
| 49 | Ecart Cotes Market | JSONL | 2,930,290 entries | 990 MB | 2026-03-22 | ✅ Active |
| dedup | Deduplication | JSONL | 3,433,444 entries | 370 MB | 2026-03-22 | ✅ Active |
| nettoyage | Nettoyage | JSONL | 2,930,290 partants | 4.7 GB | 2026-03-22 | ✅ Active |
| comblage | Comblage | JSONL | 2,930,290 partants | 4.8 GB | 2026-03-22 | ✅ Active |
| features | Feature Matrix | JSONL | 2,930,290 partants | 234 GB | 2026-03-22 | ✅ Active |
| exports | Master Export | JSON/CSV/JSONL | 2,891,593 partants | 90 GB | 2026-03-22 | ✅ Active |
| labels | Training Labels | JSONL/CSV/Parquet | 4,800,517 labels | 2.6 GB | 2026-03-22 | ✅ Active |

---

## N. Scrapers 146-158 (nouvelles sources gratuites)

| # | Source | URL | Records | Size | Last Update | Status |
|---|--------|-----|---------|------|-------------|--------|
| 146 | PMU Web Detail (Playwright) | `pmu.fr/turf/programme/` | -- | -- | 2026-03-24 | ✅ Built |
| 147 | Sporting Life Results | `sportinglife.com/racing/results` | -- | -- | 2026-03-24 | ✅ Built |
| 148 | LeTurf Consensus | `leturf.fr` | -- | -- | 2026-03-24 | ✅ Built |
| 149 | Racing API Free Tier | `theracingapi.com` (free) | -- | -- | 2026-03-24 | ✅ Built |
| 150 | data.gov.uk Racing | `data.gov.uk` | -- | -- | 2026-03-24 | ✅ Built |
| 151 | IFHA World Rankings | `ifhaonline.org` | -- | -- | 2026-03-24 | ✅ Built |
| 152 | Zone-Turf Stats | `zone-turf.fr/statistiques` | -- | -- | 2026-03-24 | ✅ Built |
| 153 | Timeform Free Section | `timeform.com` (free) | -- | -- | 2026-03-24 | ✅ Built |
| 154 | Racing Post Free Racecards | `racingpost.com` (free) | -- | -- | 2026-03-24 | ✅ Built |
| 155 | data.gouv.fr Racing | `data.gouv.fr` | -- | -- | 2026-03-24 | ✅ Built |
| 156 | At The Races Free | `attheraces.com` (free) | -- | -- | 2026-03-24 | ✅ Built |
| 157 | CanalTurf Statistiques | `canalturf.com/statistiques` | -- | -- | 2026-03-24 | ✅ Built |
| 158 | Turf-FR Stats | `turf-fr.com/stats` | -- | -- | 2026-03-24 | ✅ Built |

---

## O. Feature Builders

| Builder | Script | Features | Records | Size | Status |
|---------|--------|----------|---------|------|--------|
| Elo Ratings | `feature_builders/elo_rating_builder.py` | elo_cheval, elo_jockey, elo_entraineur, elo_combined, elo_cheval_delta, nb_races_elo | 2,869,936 | 494 MB | ✅ Active |
| Recovery | `feature_builders/recovery_features.py` | jours_repos, repos_optimal, perf_apres_repos_court/moyen/long, repos_vs_moyenne, nb_courses_30j/60j/90j | -- | -- | ✅ Built |
| Fatigue | `feature_builders/fatigue_features.py` | fatigue_30j/60j/90j, fatigue_distance_ponderee, intensite_recente, sequence_courses, tendance_fatigue | -- | -- | ✅ Built |
| Class Change | `feature_builders/class_change_features.py` | class_change features | 2,477,960 | 14 GB | ✅ Active |
| Field Strength | output/field_strength/ | field_strength per course | 293,243 | 259 MB | ✅ Active |
| Combo Features | output/features/combo_features.jsonl | combined feature interactions | 2,930,290 | 19 GB | ✅ Active |
| Equipement Features | output/features/equipement_features.jsonl | equipment-based features | 2,930,290 | 17 GB | ✅ Active |
| Interaction Features | output/features/interaction_features.jsonl | cross-entity interactions | 2,930,290 | 20 GB | ✅ Active |
| Meteo Features | output/features/meteo_features.jsonl | weather-based features | 2,930,290 | 17 GB | ✅ Active |
| Musique Features | output/features/musique_features.jsonl | past performance encoding | 2,930,290 | 18 GB | ✅ Active |
| Poids Features | output/features/poids_features.jsonl | weight/handicap features | 2,930,290 | 18 GB | ✅ Active |
| Profil Cheval | output/features/profil_cheval_features.jsonl | horse profile features | 2,930,290 | 21 GB | ✅ Active |
| Temps Features | output/features/temps_features.jsonl | time-based features | 2,930,290 | 19 GB | ✅ Active |
| Entity Features | output/features/precomputed_entity_features.jsonl | precomputed entity stats | 2,930,290 | 20 GB | ✅ Active |
| Partant Features | output/features/precomputed_partant_features.jsonl | precomputed partant stats | 2,930,290 | 18 GB | ✅ Active |

---

## Resume par statut

| Statut | Nombre | Details |
|--------|--------|---------|
| ✅ Active | 59 | Sources operationnelles avec donnees |
| ⚠️ Partial | 10 | Donnees partielles ou limitees |
| ❌ Blocked | 35 | Bloque (Cloudflare, auth, anti-bot, payant) |
| 🔄 In Progress | 1 | Collecte en cours (101 PMU API v2) |
| **Total** | **105 sources + pipeline interne + 73 feature builders** | |

---

## Couverture des 10 signaux cles (hedge fund)

| # | Signal | Couverture | Sources |
|---|--------|------------|---------|
| 1 | Cotes bookmakers | OUI | PMU (07), Smarkets (30), ZeTurf (51), Paris Turf (53) |
| 2 | Resultats historiques | OUI | PMU (02, 04), Le Trot (02b, 83), Nanaelie (16), Racing Post (37) |
| 3 | Pedigree | PARTIEL | PedigreeQuery (14), PMU (08), SIRE/IFCE (17) |
| 4 | Meteo | OUI | Meteostat (13), Open-Meteo, Meteo France (35) |
| 5 | Terrain / piste | OUI | PMU (penetrometre), reunions enrichies (39) |
| 6 | Vitesse sectionnelle | PARTIEL | PMU sectionals (11), Perf detaillees (22), Timeform (56) |
| 7 | Biomecanique | NON | Pas de source publique |
| 8 | Tracking GPS | NON | Pas de source publique en France |
| 9 | Historique jockey | OUI | PMU (06), stats rolling, Sporting Life (57), Elo jockey |
| 10 | Historique entraineur | OUI | PMU (06), stats rolling, Elo entraineur |

**Score : 6/10 complets, 3/10 partiels, 1/10 manquant.**

---

## Volume total de donnees

| Categorie | Volume |
|-----------|--------|
| Sources brutes (output/) | ~460+ GB |
| Feature matrix (73 builders) | 253 GB |
| Master export (exports/) | 89 GB |
| Elo ratings | 627 MB |
| Labels | 1.4 GB |
| Partants (records principaux) | 2,930,290 |
| Courses (records principaux) | ~257,806 |
| Training labels | 4,800,517 |
| Feature builders | 73 (producing ~510 cataloged features) |
| data_master/partants_master.jsonl | 24.4 GB |
