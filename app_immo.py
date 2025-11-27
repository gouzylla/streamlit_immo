import streamlit as st
import pandas as pd
from supabase import create_client, Client
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
    Tente de récupérer les clés dans st.secrets (Prod) ou utilise des valeurs par défaut (Dev).
    """
    try:
        # Cas 1 : Production (Streamlit Cloud) ou fichier .streamlit/secrets.toml local
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
    except (FileNotFoundError, KeyError):
        # Cas 2 : Fallback (Développement local rapide sans fichier secrets)
        # ATTENTION : Remplacez ces valeurs par vos clés UNIQUEMENT en local.
        # Ne committez jamais ce fichier avec les vraies clés sur GitHub public.
        url = "REMPLACER_PAR_VOTRE_URL_SUPABASE"
        key = "REMPLACER_PAR_VOTRE_KEY_SUPABASE"
        
    try:
        return create_client(url, key)
    except Exception as e:
        st.error(f"❌ Erreur critique : Impossible de se connecter à Supabase. \n{e}")
        return None

supabase = init_connection()

# --- 3. FONCTIONS DE RÉCUPÉRATION DE DONNÉES (CACHÉES) ---

@st.cache_data(ttl=3600)  # Cache d'1 heure pour la liste des villes (ça ne change pas souvent)
def get_villes_list():
    """Récupère le référentiel des villes (Nom + CP + INSEE)"""
    if not supabase: return pd.DataFrame()
    
    # On ne récupère que les colonnes nécessaires pour le menu pour être léger
    response = supabase.table('villes').select('code_insee, code_postal, nom_commune').execute()
    
    df = pd.DataFrame(response.data)
    if not df.empty:
        # Création d'une étiquette propre pour la liste déroulante : "Bordeaux (33000)"
        df['label'] = df['nom_commune'] + " (" + df['code_postal'].astype(str) + ")"
        return df.sort_values('nom_commune')
    return pd.DataFrame()

def get_city_data_full(code_insee):
    """Récupère les infos de loyer pour une ville donnée"""
    if not supabase: return None
    response = supabase.table('villes').select('*').eq('code_insee', code_insee).execute()
    if response.data:
        return response.data[0] # Retourne un dictionnaire (la première ligne trouvée)
    return None

def get_transactions(code_insee):
    """Récupère l'historique des ventes pour une ville donnée"""
    if not supabase: return pd.DataFrame()
    
    # On récupère les ventes. Filtres basiques pour éviter le bruit (ventes à 1€, erreurs...)
    response = supabase.table('transactions')\
        .select('*')\
        .eq('code_insee', code_insee)\
        .gt('valeur_fonciere', 5000)\
        .gt('surface_reelle_bati', 9)\
        .execute()
    
    df = pd.DataFrame(response.data)
    
    if not df.empty:
        # Typage fort des données (essentiel pour les calculs)
        df['date_mutation'] = pd.to_datetime(df['date_mutation'])
        df['valeur_fonciere'] = pd.to_numeric(df['valeur_fonciere'])
        df['surface_reelle_bati'] = pd.to_numeric(df['surface_reelle_bati'])
        
        # Feature Engineering : Prix au m²
        df['prix_m2'] = df['valeur_fonciere'] / df['surface_reelle_bati']
        
        # Filtrage des outliers extrêmes (ex: erreur de saisie à 100k€/m²)
        # On garde ce qui est raisonnable (entre 500€ et 30k€ le m²)
        df = df[(df['prix_m2'] > 500) & (df['prix_m2'] < 30000)]
        
    return df

# --- 4. INTERFACE UTILISATEUR (SIDEBAR) ---

with st.sidebar:
    st.header("🔍 Localisation")
    
    # Chargement initial
    with st.spinner("Chargement des villes..."):
        df_villes = get_villes_list()
    
    if df_villes.empty:
        st.warning("Aucune ville chargée. Vérifiez votre table 'villes' dans Supabase.")
        st.stop()
        
    # Sélecteur de ville
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
            info_ville = get_city_data_full(code_insee_actuel)
            df_transac = get_transactions(code_insee_actuel)

    # --- SECTION A : KPI MARKET ---
    if info_ville and not df_transac.empty:
        
        # 1. Calculs
        # Prix achat médian (plus fiable que la moyenne)
        prix_m2_achat = df_transac['prix_m2'].median()
        
        # Loyer moyen (gestion des cas où la donnée est vide)
        loyer_m2 = info_ville.get('loyer_m2_appart_moyen_all')
        if not loyer_m2: loyer_m2 = 0
        
        # Rentabilité Brute : (Loyer x 12) / Prix Achat
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
            help="Basé sur les indicateurs territoriaux"
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
        **Pourquoi ?**
        - Soit il n'y a pas eu de ventes récentes (> 2019).
        - Soit les données n'ont pas encore été importées dans Supabase pour ce département.
        """)
