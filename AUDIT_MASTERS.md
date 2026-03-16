# AUDIT DATA MASTERS — 15 mars 2026

## Résumé

| Master | Records | Champs | Taille | Couverture |
|--------|---------|--------|--------|------------|
| pedigree_master | 1,413,913 | 56 | 465 MB | Tous chevaux PMU |
| meteo_master | 479,377 | 25 | 743 MB | 2013-2026 |
| rapports_master | 217,569 | 309 | 343 MB | 2013-2021 |
| marche_master | 151,258 | 23 | 57 MB | 2013-2019 |
| equipements_master | 573,111 | 26 | 220 MB | 2013-2026 |
| horse_stats_master | 80,656 | 20 | 130 MB | Tous chevaux |
| performances_master | ❌ | - | - | À faire lundi (RAM) |

## Joinabilité entre masters

✅ **100% du marché** a des équipements correspondants
✅ **100% des rapports** ont la météo correspondante
⚠️ Seulement **8%** des équipements ont du marché (marché = 2013-2019 seulement)
⚠️ Seulement **45%** de la météo a des rapports (rapports s'arrêtent en 2021)

## Points forts

- **Pedigree** : 100% nom/sexe/race, 94% robe, 90% année naissance
- **Météo** : 100% vent/temp/humidité/type_piste, 3 sources croisées par record
- **Rapports** : 309 champs ! 98% discipline/distance/hippodrome
- **Équipements** : 100% œillères, 99% poids — très complet
- **Marché** : 100% partant_uid, 99% cotes

## Problèmes identifiés

### 🔴 CRITIQUE
1. **Rapports s'arrêtent en 2021** — les scripts 04/21/38 sont encore en train de collecter 2021-2026
2. **Marché s'arrête en 2019** — les scripts 28 (combinaisons) et 07 (cotes) continuent
3. **Pedigree père/mère** seulement 20% rempli — le script 14 (28%) va améliorer ça

### 🟠 IMPORTANT
4. **Pedigree** : info sur le père du cheval (sire_*) seulement 14% — manque de données amont
5. **Météo** : is_cold/is_hot à 0% — flags booléens jamais calculés (facile à ajouter)
6. **Équipements** : déferré seulement 1% — donnée rare mais normale (peu de chevaux déferrés)

### 🟡 NICE TO HAVE
7. Racing Post master vide — parser les 8600 fichiers cache lundi
8. Performances master à créer lundi sur le gros PC

## Actions pour lundi

1. Relancer les merges APRÈS fin des scripts de collecte (rapports/marché auront plus de données)
2. Créer performances_master sur le PC 64GB
3. Parser Racing Post cache
4. Calculer les flags météo manquants (is_cold, is_hot, is_windy)
5. Re-merger pedigree quand script 14 finit
6. Commencer la matrice de features (tous les masters seront prêts)
