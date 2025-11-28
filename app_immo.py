import streamlit as st
import pandas as pd
# On importe create_client et Client depuis supabase.client.
from supabase.client import create_client, Client
# L'APIError doit être importée depuis postgrest.exceptions.
from postgrest.exceptions import APIError 
import plotly.express as px

# --- 1. CONFIGURATION DE LA PAGE ---
st.set_page_config(
    page_title="Immo-Data Analyst",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 2. GESTION DE LA CONNEXION (SÉCURISÉE) ---
@st.cache_resource
def init_connection():
    """
    Initialise la connexion à Supabase.
    Récupère les clés depuis st.secrets (requis par Streamlit Cloud).
    """
    
    url = st.secrets.get("SUPABASE_URL", "REMPLACER_PAR_VOTRE_URL_SUPABASE")
    key = st.secrets.get("SUPABASE_KEY", "REMPLACER_PAR_VOTRE_KEY_SUPABASE")
    
    if url == "REMPLACER_PAR_VOTRE_URL_SUPABASE" or key == "REMPLACER_PAR_VOTRE_KEY_SUPABASE":
        st.error(
            "❌ Erreur de configuration: Les variables SUPABASE_URL ou SUPABASE_KEY sont manquantes ou incorrectes."
            "\n\nVérifiez que vous avez copié le contenu du fichier secrets.toml dans les Secrets de Streamlit Cloud."
        )
        return None
        
    try:
        return create_client(url, key)
    except Exception as e:
        st.error(f"❌ Erreur critique : Impossible de se connecter à Supabase. Vérifiez l'URL et la clé. \n Détail: {e}")
        return None

supabase = init_connection()

# --- 3. FONCTIONS DE RÉCUPÉRATION DE DONNÉES (CACHÉES) ---

@st.cache_data(ttl=3600)  # Cache d'1 heure
def get_villes_list():
    """Récupère le référentiel des villes (Nom + CP + INSEE) depuis la table Dim_ville"""
    if not supabase: 
        return pd.DataFrame()
    
    TABLE_DIM_VILLE = 'Dim_ville'
    
    try:
        # CORRECTION 1a: On ajoute un limit() élevé pour être certain de tout charger
        response = supabase.table(TABLE_DIM_VILLE).select('code_insee, code_postal, nom_commune').limit(200000).execute()
        
    except APIError as e:
        st.error(
            f"❌ Erreur Supabase lors du chargement des villes (APIError) : La requête SELECT sur '{TABLE_DIM_VILLE}' a échoué."
            f"\nDétail technique: {e}"
        )
        return pd.DataFrame()
    
    if not response.data or len(response.data) == 0:
        st.warning(f"⚠️ La table `{TABLE_DIM_VILLE}` est vide ou inaccessible. (Vérifiez le RLS)")
        return pd.DataFrame()
    
    df = pd.DataFrame(response.data)
    
    if not df.empty:
        # CORRECTION 2a: Assurer que code_insee est une chaîne de caractères (important pour les filtres!)
        df['code_insee'] = df['code_insee'].astype(str).str.zfill(5) 
        # CORRECTION 2b: Assurer que code_postal est une chaîne de caractères
        df['code_postal'] = df['code_postal'].astype(str).str.zfill(5)
        
        # Création d'une étiquette propre pour la liste déroulante : "Bordeaux (33000)"
        df['label'] = df['nom_commune'] + " (" + df['code_postal'].astype(str) + ")"
        return df.sort_values('nom_commune')
    return pd.DataFrame()

def get_city_data_full(code_insee_actuel):
    """Récupère les infos de loyer pour une ville donnée depuis la table Dim_ville"""
    if not supabase: return None
    TABLE_DIM_VILLE = 'Dim_ville'
    try:
        # CORRECTION 2c: Forcer le code INSEE en string pour la requête
        response = supabase.table(TABLE_DIM_VILLE).select('*').eq('code_insee', str(code_insee_actuel)).execute()
        if response.data:
            return response.data[0]
    except APIError as e:
        print(f"Erreur silencieuse sur get_city_data_full: {e}")
    return None

def get_transactions(code_insee_actuel):
    """Récupère l'historique des ventes pour une ville donnée depuis la table Fct_transaction_immo"""
    if not supabase: return pd.DataFrame()
    
    TABLE_FACT_TRANSAC = 'Fct_transaction_immo'
    
    try:
        # CORRECTION 2d: Forcer le code INSEE en string pour la requête
        response = supabase.table(TABLE_FACT_TRANSAC)\
            .select('*')\
            .eq('code_insee', str(code_insee_actuel))\
            .gt('valeur_fonciere', 5000)\
            .gt('surface_reelle_bati', 9)\
            .limit(50000)\
            .execute()
            
    except APIError as e:
        st.error(
            f"❌ Erreur Supabase lors du chargement des transactions (APIError) : La requête SELECT sur '{TABLE_FACT_TRANSAC}' a échoué."
            f"\nDétail technique: {e}"
        )
        return pd.DataFrame()
    
    df = pd.DataFrame(response.data)
    
    if not df.empty:
        # Typage fort des données (essentiel pour les calculs)
        df['date_mutation'] = pd.to_datetime(df['date_mutation'])
        df['valeur_fonciere'] = pd.to_numeric(df['valeur_fonciere'])
        df['surface_reelle_bati'] = pd.to_numeric(df['surface_reelle_bati'])
        
        # Feature Engineering : Prix au m²
        df['prix_m2'] = df['valeur_fonciere'] / df['surface_reelle_bati']
        
        # Filtrage des outliers extrêmes (ex: erreur de saisie à 100k€/m²)
        df = df[(df['prix_m2'] > 500) & (df['prix_m2'] < 30000)]
        
    return df

# --- 4. INTERFACE UTILISATEUR (SIDEBAR) ---

with st.sidebar:
    st.header("🔍 Localisation")
    
    # Chargement initial
    with st.spinner("Chargement des villes..."):
        df_villes = get_villes_list()
    
    if df_villes.empty:
        st.error("L'application s'arrête car la liste des villes n'a pas pu être chargée.")
        st.stop()
        
    # Sélecteur de ville
    # On utilise l'index de la ligne trouvée précédemment pour assurer la correspondance
    selected_label = st.selectbox(
        "Choisissez une commune",
        options=df_villes['label'],
        placeholder="Tapez le nom d'une ville..."
    )
    
    # Récupération du Code INSEE correspondant au choix
    row_ville = df_villes[df_villes['label'] == selected_label].iloc[0]
    code_insee_actuel = row_ville['code_insee']
    
    st.divider()
    st.caption(f"Code INSEE : {code_insee_actuel}")
    st.caption("Données sources : DVF (Etalab) & Ministère Transition Écologique")

# --- 5. DASHBOARD PRINCIPAL ---

st.title(f"Analyse Immobilière : {row_ville['nom_commune']}")

if code_insee_actuel:
    
    # Chargement des données détaillées
    col1, col2 = st.columns([1, 3])
    with col1:
        with st.spinner("Analyse..."):
            # On utilise le code INSEE actuel pour toutes les fonctions
            info_ville = get_city_data_full(code_insee_actuel)
            df_transac = get_transactions(code_insee_actuel)

    # --- SECTION A : KPI MARKET ---
    if info_ville and not df_transac.empty:
        
        # 1. Calculs
        prix_m2_achat = df_transac['prix_m2'].median()
        
        # Loyer moyen: la colonne est 'loyer_m2_appart_moyen_all' mais c'est une estimation.
        # On va utiliser une colonne plus fiable dans Dim_ville si elle est disponible, sinon on prend la colonne estimée
        # Je vais renommer la clé dans Dim_ville pour correspondre aux données du fichier d'exemple (INSEE_C, loyspredm2).
        # Je vais supposer que vous avez une colonne 'loypredm2' dans Dim_ville
        loyer_m2 = info_ville.get('loypredm2') 
        if loyer_m2 is None: # Si la clé loypredm2 n'existe pas, on tente l'ancienne clé.
            loyer_m2 = info_ville.get('loyer_m2_appart_moyen_all')
        if not loyer_m2: loyer_m2 = 0
        
        # ... Reste du code du dashboard (identique à la version précédente mais utilisant les nouvelles données)
            
        # Rentabilité Brute
        if prix_m2_achat > 0:
            renta_brute = ((loyer_m2 * 12) / prix_m2_achat) * 100
        else:
            renta_brute = 0
            
        # Tendance (Dernière année vs Total)
        derniere_annee = df_transac['date_mutation'].dt.year.max()
        prix_m2_recent = df_transac[df_transac['date_mutation'].dt.year == derniere_annee]['prix_m2'].median()
        delta_prix = prix_m2_recent - prix_m2_achat

        # 2. Affichage
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        
        kpi1.metric(
            "Prix Achat Médian", 
            f"{int(prix_m2_achat)} €/m²",
            delta=f"{int(delta_prix)} € vs historique"
        )
        
        kpi2.metric(
            "Loyer Estimé (Appt)", 
            f"{loyer_m2:.1f} €/m²",
            help="Basé sur les indicateurs territoriaux (colonne loypredm2 ou loyer_m2_appart_moyen_all)"
        )
        
        kpi3.metric(
            "Rentabilité Brute", 
            f"{renta_brute:.2f} %",
            delta="Opportunité" if renta_brute > 6 else "Marché tendu"
        )
        
        kpi4.metric(
            "Volume de Ventes", 
            f"{len(df_transac)}",
            help="Nombre total de transactions analysées"
        )
        
        st.divider()
        
        # --- SECTION B : GRAPHIQUES ---
        
        g1, g2 = st.columns([2, 1])
        
        with g1:
            st.subheader("📈 Évolution des prix")
            # Agrégation par Trimestre
            df_transac['trimestre'] = df_transac['date_mutation'].dt.to_period('Q').astype(str)
            df_trend = df_transac.groupby('trimestre')['prix_m2'].median().reset_index()
            
            fig_line = px.line(
                df_trend, x='trimestre', y='prix_m2', markers=True,
                title="Prix médian au m² par trimestre",
                labels={'prix_m2': 'Prix €/m²', 'trimestre': 'Période'}
            )
            fig_line.update_layout(xaxis_title=None)
            st.plotly_chart(fig_line, use_container_width=True)
            
        with g2:
            st.subheader("📊 Distribution")
            fig_hist = px.histogram(
                df_transac, x="prix_m2", nbins=25,
                title="Répartition des prix au m²",
                color_discrete_sequence=['#636EFA']
            )
            fig_hist.add_vline(x=prix_m2_achat, line_dash="dash", line_color="red", annotation_text="Médiane")
            st.plotly_chart(fig_hist, use_container_width=True)

        # --- SECTION C : DATA EXPLORER ---
        with st.expander("📂 Voir les dernières transactions détaillées"):
            st.dataframe(
                df_transac[['date_mutation', 'valeur_fonciere', 'surface_reelle_bati', 'prix_m2', 'type_local']]
                .sort_values('date_mutation', ascending=False),
                column_config={
                    "date_mutation": "Date",
                    "valeur_fonciere": st.column_config.NumberColumn("Prix", format="%d €"),
                    "surface_reelle_bati": st.column_config.NumberColumn("Surface", format="%d m²"),
                    "prix_m2": st.column_config.NumberColumn("Prix/m²", format="%.2f €"),
                },
                use_container_width=True
            )
            
    # GESTION DES CAS VIDES
    elif not df_transac.empty and not info_ville:
        st.warning("⚠️ Nous avons les ventes, mais pas les données de loyer pour cette commune (Code INSEE inconnu dans la table de référence).")
        st.dataframe(df_transac.head())
        
    else:
        st.info("👋 Aucune donnée trouvée pour cette ville.")
        st.markdown("""
        **Pourquoi ce message ?**
        1. **Problème de type de données résolu :** Le code a été corrigé pour assurer la correspondance `string` pour le `code_insee`.
        2. **Vérifiez la table des transactions :** Assurez-vous que la table `Fct_transaction_immo` contient des transactions pour le `code_insee` sélectionné et que la colonne `code_insee` est bien une chaîne de caractères de 5 chiffres.
        3. **Vérifiez le RLS (à nouveau) :** Même si vous avez mis `anon`, une politique mal écrite peut bloquer. Vérifiez que la politique `SELECT` sur `Fct_transaction_immo` a bien pour expression `true` (ou une autre condition que le rôle `anon` satisfait).
        """)
