# Place this constant near the top of your script, 
# typically with other constants and imports, to make it accessible.

# Exemple de définition pour corriger l'erreur NameError: name 'MAX_ROWS' is not defined
MAX_ROWS = 100000  # Remplacez 100000 par la limite réelle de votre jeu de données

# ... le reste de votre code app_immo.py ...

# Lignes 366-370 (après correction) :

# ...
# st.divider()
# st.subheader("Indicateurs Clés de Performance (KPI)")
# ...

# Les lignes suivantes sont pour contexte et montrent l'endroit où MAX_ROWS était manquant.

    # Dans votre fonction ou bloc de code où les KPIs sont définis:
    # kpi1, kpi2, kpi3, kpi4 = st.columns(4)

    # ... autres KPIs

    # kpi4.metric(
    #    "Volume de Ventes",
    #    f"{nb_transactions:,}" if nb_transactions > 0 else "N/A",
    #    help=f"Nombre total de transactions analysées (max {MAX_ROWS:,} lignes dans le jeu de données original)."
    # )
# ...
