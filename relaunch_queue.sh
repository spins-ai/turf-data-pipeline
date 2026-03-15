#!/bin/bash
# Script de relance automatique — lance les scripts un par un
# quand les scripts en cours seront finis et la RAM libérée
cd "/Users/quentinherve/models hybride"

echo "$(date) | Attente fin des scripts en cours..."

# Attendre que 22_performances finisse (le plus gourmand en RAM)
while ps -p 4799 > /dev/null 2>&1; do
    sleep 300  # check toutes les 5 min
done
echo "$(date) | 22_performances terminé, RAM libérée"

# 1. Renormalisation Geny (rapide, ~10 min)
echo "$(date) | Lancement renormalisation Geny..."
python3 26_geny.py --renormalize 2>&1 | tee logs/26_geny_renorm.log
echo "$(date) | Geny terminé"

# 2. Pedigree scraper (reprend au checkpoint)
echo "$(date) | Lancement 14_pedigree_scraper (reprise)..."
python3 14_pedigree_scraper.py 2>&1 | tee logs/14_pedigree_relaunch.log
echo "$(date) | 14_pedigree terminé"

# 3. Pedigree query (reprend au cache)
echo "$(date) | Lancement 36_pedigree_query (reprise)..."
python3 36_pedigree_query.py 2>&1 | tee logs/36_pedigree_query_relaunch.log
echo "$(date) | 36_pedigree_query terminé"

# 4. Racing Post (reprend)
echo "$(date) | Lancement 37_rpscrape (reprise)..."
python3 37_rpscrape_racing_post.py 2>&1 | tee logs/37_racing_post_relaunch.log
echo "$(date) | 37_racing_post terminé"

# 5. Feature builder (le grand moment !)
echo "$(date) | Lancement master_feature_builder (350+ features)..."
cd feature_builders
python3 master_feature_builder.py 2>&1 | tee ../logs/master_feature_builder.log
cd ..
echo "$(date) | Feature builder terminé"

# 6. Re-fusionner TOUS les pedigrees (les 4 sources + nouvelles données des relances)
echo "$(date) | Fusion finale de tous les pedigrees..."
python3 merge_all_pedigree.py 2>&1 | tee logs/merge_pedigree_final.log
echo "$(date) | Pedigree complet fusionné"

echo ""
echo "$(date) | === TOUT EST TERMINÉ ==="
echo "Vérifier:"
echo "  - output/features/ pour la nouvelle matrice 350+ features"
echo "  - output/pedigree_complete/ pour le pedigree fusionné"
