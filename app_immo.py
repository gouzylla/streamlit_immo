import streamlit as st
import pandas as pd
from supabase.client import create_client, Client
from postgrest.exceptions import APIError 
import plotly.express as px
import sys # Pour les logs de debug

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
    """
    
    url = st.secrets.get("SUPABASE_URL", "REMPLACER_PAR_VOTRE_URL_SUPABASE")
    key = st.secrets.get("SUPABASE_KEY", "REMPLACER_PAR_VOTRE_KEY_SUPABASE")
    
    if url == "REMPLACER_PAR_VOTRE_URL_SUPABASE" or key == "REMPLACER_PAR_VOTRE_KEY_SUPABASE":
        st.error("❌ Erreur de configuration: Les variables SUPABASE_URL ou SUPABASE_KEY sont manquantes.")
        return None
        
    try:
        return create_client(url, key)
    except Exception as e:
        st.error(f"❌ Erreur critique : Impossible de se connecter à Supabase. Détail: {e}")
        return None

supabase = init_connection()

# --- 3. FONCTIONS DE RÉCUPÉRATION DE DONNÉES (CACHÉES) ---

@st.cache_data(ttl=3600)  # Cache d'1 heure
def get_villes_list():
    """
    Récupère le référentiel des villes.
    Si le problème persiste, videz le cache et vérifiez si le RLS est activé
    sur Dim_ville (le rôle anon doit avoir SELECT).
    """
    if not supabase: 
        return pd.DataFrame()
    
    TABLE_DIM_VILLE = 'Dim_ville'
    
    try:
        # Tente de charger un nombre très élevé pour bypasser toute limite implicite
        # REMARQUE : Si cette ligne cause une erreur de mémoire, la limite doit être abaissée.
        response = supabase.table(TABLE_DIM_VILLE).select('code_insee, code_postal, nom_commune').limit(500000).execute()
        
    except APIError as e:
        st.error(f"❌ Erreur Supabase lors du chargement des villes (APIError). Détail: {e}")
        return pd.DataFrame()
    
    if not response.data or len(response.data) == 0:
        st.warning(f"⚠️ La table `{TABLE_DIM_VILLE}` est vide ou inaccessible. (Vérifiez le RLS)")
        return pd.DataFrame()
    
    df = pd.DataFrame(response.data)
    
    if not df.empty:
        # Assurer que code_insee est une chaîne de caractères de 5 chiffres pour la cohérence
        df['code_insee'] = df['code_insee'].astype(str).str.zfill(5) 
        df['code_postal'] = df['code_postal'].astype(str).str.zfill(5)
        
        # Pour le debugging: Afficher le nombre de villes chargées et le type de code_insee
        print(f"DEBUG: {len(df)} villes chargées. Type de code_insee: {df['code_insee'].dtype}", file=sys.stderr)
        
        # Création d'une étiquette propre pour la liste déroulante
        df['label'] = df['nom_commune'] + " (" + df['code_postal'].astype(str) + ")"
        return df.sort_values('nom_commune')
    return pd.DataFrame()

def get_city_data_full(code_insee_actuel):
    """
    Récupère les infos de loyer pour une ville donnée depuis la table Dim_ville.
    """
    if not supabase: return None
    TABLE_DIM_VILLE = 'Dim_ville'
    
    # Assurer que l'identifiant de recherche est bien une chaîne de caractères
    insee_str = str(code_insee_actuel).zfill(5)
    
    print(f"DEBUG: get_city_data_full cherche INSEE='{insee_str}'", file=sys.stderr)
    
    try:
        # On utilise une liste de noms de colonnes probables pour le code INSEE dans Dim_ville
        # Si votre colonne est nommée différemment, changez 'code_insee' ici
        response = supabase.table(TABLE_DIM_VILLE).select('*').eq('code_insee', insee_str).execute()
        
        if response.data:
            return response.data[0]
        
    except APIError as e:
        print(f"Erreur get_city_data_full: {e}", file=sys.stderr)
        
    return None

def get_transactions(code_insee_actuel):
    """
    Récupère l'historique des ventes pour une ville donnée depuis Fct_transaction_immo.
    """
    if not supabase: return pd.DataFrame()
    
    TABLE_FACT_TRANSAC = 'Fct_transaction_immo'
    
    # Assurer que l'identifiant de recherche est bien une chaîne de caractères
    insee_str = str(code_insee_actuel).zfill(5)
    
    print(f"DEBUG: get_transactions cherche INSEE='{insee_str}'", file=sys.stderr)
    
    try:
        # Si votre colonne de jointure est 'code_commune' au lieu de 'code_insee', changez-la ici
        response = supabase.table(TABLE_FACT_TRANSAC)\
            .select('*')\
            .eq('code_insee', insee_str)\
            .gt('valeur_fonciere', 5000)\
            .gt('surface_reelle_bati', 9)\
            .limit(50000)\
            .execute()
            
    except APIError as e:
        st.error(
            f"❌ Erreur Supabase lors du chargement des transactions (APIError). Vérifiez le RLS et le nom des colonnes/tables."
            f"\nDétail technique: {e}"
        )
        return pd.DataFrame()
    
    df = pd.DataFrame(response.data)
    
    print(f"DEBUG: {len(df)} transactions trouvées pour INSEE='{insee_str}'", file=sys.stderr)
    
    if not df.empty:
        # Typage fort des données (essentiel pour les calculs)
        df['date_mutation'] = pd.to_datetime(df['date_mutation'], errors='coerce')
        df['valeur_fonciere'] = pd.to_numeric(df['valeur_fonciere'], errors='coerce')
        df['surface_reelle_bati'] = pd.to_numeric(df['surface_reelle_bati'], errors='coerce')
        
        # Filtrage des lignes avec des valeurs non valides après coercion
        df.dropna(subset=['date_mutation', 'valeur_fonciere', 'surface_reelle_bati'], inplace=True)
        
        # Feature Engineering : Prix au m²
        df['prix_m2'] = df['valeur_fonciere'] / df['surface_reelle_bati']
        
        # Filtrage des outliers extrêmes 
        df = df[(df['prix_m2'] > 500) & (df['prix_m2'] < 30000)]
        
    return df

# --- 4. INTERFACE UTILISATEUR (SIDEBAR) ---

with st.sidebar:
    st.header("🔍 Localisation")
    
    # Chargement initial
    # st.experimental_rerun() permet de vider le cache, mais c'est risqué. On s'en tient à cache_data.
    with st.spinner("Chargement des villes..."):
        df_villes = get_villes_list()
    
    if df_villes.empty:
        st.error("L'application s'arrête car la liste des villes n'a pas pu être chargée.")
        st.stop()
        
    # Sélecteur de ville
    selected_label = st.selectbox(
        "Choisissez une commune",
        options=df_villes['label'],
        placeholder="Tapez le nom d'une ville..."
    )
    
    # Récupération du Code INSEE correspondant au choix
    # On garantit que la colonne code_insee est bien lue comme string
    row_ville = df_villes[df_villes['label'] == selected_label].iloc[0]
    code_insee_actuel = row_ville['code_insee'] # Est déjà un string de 5 chiffres grâce à get_villes_list
    
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
            info_ville = get_city_data_full(code_insee_actuel)
            df_transac = get_transactions(code_insee_actuel)

    # --- SECTION A : KPI MARKET ---
    if info_ville and not df_transac.empty:
        
        # 1. Calculs
        prix_m2_achat = df_transac['prix_m2'].median()
        
        # CORRECTION DE LA CLÉ DE LOYER
        # Le nom de la colonne de loyer est 'loypredm2' si vous avez utilisé le fichier de l'ANIL
        loyer_m2 = info_ville.get('loypredm2') 
        # Si la clé n'est pas 'loypredm2', on essaie l'ancienne clé ou une autre clé probable
        if loyer_m2 is None: 
            loyer_m2 = info_ville.get('loyer_m2_appart_moyen_all') 
        if loyer_m2 is None: 
            loyer_m2 = 0 
        
        # Rentabilité Brute
        if prix_m2_achat > 0 and loyer_m2 > 0:
            renta_brute = ((loyer_m2 * 12) / prix_m2_achat) * 100
        else:
            renta_brute = 0
            
        # Tendance (Dernière année vs Total)
        derniere_annee = df_transac['date_mutation'].dt.year.max()
        
        if pd.notna(derniere_annee):
            prix_m2_recent = df_transac[df_transac['date_mutation'].dt.year == derniere_annee]['prix_m2'].median()
            delta_prix = prix_m2_recent - prix_m2_achat
        else:
            # Cas où aucune date de mutation valide n'a été trouvée
            derniere_annee = "N/A"
            prix_m2_recent = prix_m2_achat
            delta_prix = 0

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
            help="Basé sur l'indicateur de loyer ('loypredm2' ou 'loyer_m2_appart_moyen_all') de Dim_ville"
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
    elif not info_ville:
        st.error("❌ ERREUR DE RÉFÉRENTIEL : Les données de loyer (Dim_ville) sont introuvables pour ce code INSEE.")
        if not df_transac.empty:
            st.info("💡 Cependant, des transactions ont été trouvées pour cette ville.")
            st.dataframe(df_transac.head())
        
    else:
        st.info("👋 Aucune transaction (Fct_transaction_immo) trouvée pour cette ville (ou toutes les transactions ont été filtrées).")
        st.markdown(f"""
        **Vérifications recommandées :**
        - **1. Console Debug :** Ouvrez votre console (F12) et vérifiez la ligne **`DEBUG: {len(df_transac)} transactions trouvées pour INSEE='{code_insee_actuel}'`**. Si ce nombre est 0, c'est que la requête Supabase ne trouve rien.
        - **2. Nom de Colonne :** Dans Supabase, vérifiez que la colonne utilisée pour la jointure dans la table `Fct_transaction_immo` s'appelle bien **`code_insee`**. Si elle s'appelle `code_commune` ou autre chose, changez-la dans la fonction `get_transactions`.
        - **3. RLS sur Fct_transaction_immo :** Si les logs indiquent 0, le RLS est toujours la cause la plus probable. Vérifiez à nouveau que le rôle `anon` peut **SELECT** sans aucune condition bloquante.
        """)
