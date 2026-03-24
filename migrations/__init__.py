"""
migrations/ — Scripts de migration de schema
=============================================
Quand le schema des fichiers master change (ajout/renommage/suppression de champs),
les scripts de migration permettent de mettre a jour les donnees existantes.

Convention de nommage:
    YYYYMMDD_description.py
    Exemple: 20260324_rename_hippo_to_hippodrome.py

Chaque script de migration doit:
1. Lire l'ancien fichier master
2. Transformer les records
3. Ecrire le nouveau fichier master
4. Logger les modifications
"""
