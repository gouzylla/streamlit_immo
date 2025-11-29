import streamlit as st
import pandas as pd
from supabase.client import create_client, Client
from postgrest.exceptions import APIError 
import plotly.express as px
import sys 
import requests # Pour les appels API Gemini
import json
import time

# --- 1. CONFIGURATION DE LA PAGE ---
st.set_page_config(
    page_title="Immo-Data Analyst",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CONFIGURATION API GEMINI ---
MODEL_NAME = "gemini-2.5-flash-preview-09-2025"
API_KEY = "" # Laissez vide comme requis par l'environnement
BASE_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL_NAME}:generateContent?key={API_KEY}"
MAX_RETRIES = 5

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

# Variable globale pour stocker l'ID de jointure utilisé (Code Postal)
if 'join_id' not in st.session_state:
    st.session_state.join_id = 'code_postal'


@st.cache_data(ttl=3600)  # Cache d'1 heure
def get_villes_list():
    """
    Récupère l'intégralité du référentiel des villes via pagination (boucle) 
    pour surmonter la limite de 1000 lignes de l'API Supabase.
    """
    if not supabase: 
        return pd.DataFrame()
    
    TABLE_DIM_VILLE = 'Dim_ville'
    
    # Configuration de la pagination
    PAGE_SIZE = 1000  # Nombre de lignes récupérées par requête
    all_data = []
    offset = 0
    total_data_loaded = 0
    
    while True:
        try:
            # Utilisation de range pour la pagination (offset + limit)
            # range(a, b) dans Supabase est inclusif des deux côtés, donc [a, b]. 
            # Pour récupérer PAGE_SIZE=1000 lignes, on fait range(offset, offset + 999)
            response = supabase.table(TABLE_DIM_VILLE)\
                .select('code_insee, code_postal, nom_commune')\
                .order('nom_commune', desc=False)\
                .range(offset, offset + PAGE_SIZE - 1)\
                .execute()
            
            current_page_data = response.data
            
            if not current_page_data:
                # Si la requête est vide, c'est la fin des données
                break
                
            all_data.extend(current_page_data)
            total_data_loaded += len(current_page_data)
            
            # Vérification de la condition d'arrêt : si on a moins que la taille de la page, c'est la fin
            if len(current_page_data) < PAGE_SIZE:
                break
                
            # Préparation de l'offset pour la prochaine page
            offset += PAGE_SIZE
            
        except APIError as e:
            st.error(f"❌ Erreur Supabase lors du chargement des villes (APIError) à l'offset {offset}. Détail: {e}")
            break # Arrêter en cas d'erreur
        except Exception as e:
            st.error(f"❌ Erreur inattendue lors du chargement des villes à l'offset {offset}. Détail: {e}")
            break

    if not all_data:
        st.warning(f"⚠️ La table `{TABLE_DIM_VILLE}` est vide ou inaccessible. (Vérifiez le RLS)")
        return pd.DataFrame()
    
    df = pd.DataFrame(all_data)
    
    if not df.empty:
        # Assurer que code_postal est une chaîne de caractères de 5 chiffres pour la cohérence
        df[st.session_state.join_id] = df[st.session_state.join_id].astype(str).str.zfill(5)
        df['code_insee'] = df['code_insee'].astype(str).str.zfill(5)
        
        # Création d'une étiquette propre pour la liste déroulante
        # Dédoublonnage sur le 'label' pour éviter d'avoir 10 fois la même commune dans le selectbox
        df['label'] = df['nom_commune'] + " (" + df[st.session_state.join_id].astype(str) + ")"
        df = df.drop_duplicates(subset=['label'])
        
        # Pour le debugging
        print(f"DEBUG: {len(df)} villes (uniques) chargées via pagination. Clé de jointure: {st.session_state.join_id}", file=sys.stderr)
        
        return df.sort_values('nom_commune')
    return pd.DataFrame()

def get_city_data_full(join_key_value):
    """
    Récupère les infos de loyer pour une ville donnée depuis Dim_ville.
    """
    if not supabase: return None
    TABLE_DIM_VILLE = 'Dim_ville'
    
    # Assurer que l'identifiant de recherche (Code Postal) est bien une chaîne de caractères
    join_key_value_str = str(join_key_value).zfill(5)
    
    print(f"DEBUG: get_city_data_full cherche {st.session_state.join_id}='{join_key_value_str}'", file=sys.stderr)
    
    try:
        # Utilisation de select('*') pour récupérer toutes les colonnes de loyer 
        response = supabase.table(TABLE_DIM_VILLE).select('*').eq(st.session_state.join_id, join_key_value_str).execute()
        
        if response.data:
            # On prend la première ligne 
            return response.data[0] 
        
    except APIError as e:
        print(f"Erreur get_city_data_full: {e}", file=sys.stderr)
        
    return None

def get_transactions(join_key_value):
    """
    Récupère l'historique des ventes pour une ville donnée depuis Fct_transaction_immo.
    """
    if not supabase: return pd.DataFrame()
    
    TABLE_FACT_TRANSAC = 'Fct_transaction_immo'
    
    # Assurer que l'identifiant de recherche (Code Postal) est bien une chaîne de caractères
    join_key_value_str = str(join_key_value).zfill(5)
    
    print(f"DEBUG: get_transactions cherche {st.session_state.join_id}='{join_key_value_str}'", file=sys.stderr)
    
    try:
        # Utilisation de st.session_state.join_id ('code_postal') pour la recherche
        # Limite à 50 000 transactions pour éviter un chargement trop long.
        response = supabase.table(TABLE_FACT_TRANSAC)\
            .select('*')\
            .eq(st.session_state.join_id, join_key_value_str)\
            .gt('valeur_fonciere', 5000)\
            .gt('surface_reelle_bati', 9)\
            .limit(50000)\
            .execute()
            
    except APIError as e:
        st.error(
            f"❌ Erreur Supabase lors du chargement des transactions (APIError). Vérifiez le RLS sur Fct_transaction_immo et le nom des colonnes/tables."
            f"\nDétail technique: {e}"
        )
        return pd.DataFrame()
    
    df = pd.DataFrame(response.data)
    
    print(f"DEBUG: {len(df)} transactions trouvées pour {st.session_state.join_id}='{join_key_value_str}'", file=sys.stderr)
    
    if not df.empty:
        # Typage fort des données
        df['date_mutation'] = pd.to_datetime(df['date_mutation'], errors='coerce')
        df['valeur_fonciere'] = pd.to_numeric(df['valeur_fonciere'], errors='coerce')
        df['surface_reelle_bati'] = pd.to_numeric(df['surface_reelle_bati'], errors='coerce')
        
        df.dropna(subset=['date_mutation', 'valeur_fonciere', 'surface_reelle_bati'], inplace=True)
        
        # Feature Engineering : Prix au m²
        df['prix_m2'] = df['valeur_fonciere'] / df['surface_reelle_bati']
        
        # Filtrage des outliers extrêmes 
        df = df[(df['prix_m2'] > 500) & (df['prix_m2'] < 30000)]
        
    return df

# --- 4. UTILS POUR LA CONVERSION DE LOYER ---

def convert_loyer_to_float(raw_value):
    """
    Convertit une valeur de loyer potentiellement au format texte (avec virgule) en float.
    Retourne 0.0 si la valeur est None ou non numérique.
    """
    if raw_value is None:
        return 0.0
    
    try:
        # 1. Conversion en chaîne pour assurer la méthode .replace()
        value_str = str(raw_value)
        # 2. Remplacement de la virgule par le point (pour gérer le format français)
        cleaned_value = value_str.replace(',', '.')
        # 3. Conversion en float
        return float(cleaned_value)
    except ValueError as e:
        # En cas d'échec (ex: chaîne vide, texte), on renvoie 0.0
        print(f"ATTENTION: Échec de la conversion de la valeur de loyer '{raw_value}'. Détail: {e}", file=sys.stderr)
        return 0.0
        
# --- 5. FONCTION D'ANALYSE IA ---

@st.cache_data(ttl=600) # Cache 10 minutes pour l'analyse IA
def get_ai_market_analysis(city_name, prix_m2_achat, loyer_m2, renta_brute, typ_pred, nb_transactions, delta_prix):
    """
    Génère une analyse de marché basée sur les indicateurs clés via l'API Gemini.
    """
    
    # 1. Définition du rôle et du format de l'analyse (System Instruction)
    system_prompt = (
        "Vous êtes un analyste financier immobilier spécialisé dans l'investissement locatif en France. "
        "Fournissez une analyse concise (maximum 250 mots) et professionnelle du marché pour un investisseur. "
        "L'analyse doit être structurée en deux sections claires : **Points Forts** et **Points Faibles**. "
        "Basez-vous *uniquement* sur les données fournies ci-dessous. Interprétez la fiabilité de l'estimation de loyer (TYPPRED)."
    )
    
    # 2. Construction de la requête utilisateur avec les données
    user_query = f"""
    Analysez le marché pour la ville de {city_name} en vous basant sur ces métriques :
    - Prix Achat Médian: {prix_m2_achat} €/m²
    - Loyer Estimé: {loyer_m2} €/m²
    - Rentabilité Brute (estimée): {renta_brute:.2f} %
    - Fiabilité de l'estimation de loyer (TYPPRED): {typ_pred} (Rappel: 'commune' > 'epci' > 'maille')
    - Volume de Transactions (analysées): {nb_transactions}
    - Tendance prix vs historique: {delta_prix} €/m²
    """
    
    payload = {
        "contents": [{"parts": [{"text": user_query}]}],
        "systemInstruction": {"parts": [{"text": system_prompt}]}
    }

    headers = {'Content-Type': 'application/json'}
    
    for attempt in range(MAX_RETRIES):
        try:
            # 3. Appel API avec gestion de l'authentification (si API_KEY est fourni par l'environnement)
            response = requests.post(BASE_URL, headers=headers, data=json.dumps(payload), timeout=30)
            response.raise_for_status()  # Lève une exception pour les codes d'erreur HTTP
            
            result = response.json()
            
            # Extraction du texte généré
            text = result.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '')
            if text:
                return text
            
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                # Gestion de l'exponentiel backoff
                sleep_time = 2 ** attempt
                print(f"Erreur requête API: {e}. Tentative {attempt + 1}/{MAX_RETRIES}. Réessayer dans {sleep_time}s...", file=sys.stderr)
                time.sleep(sleep_time)
            else:
                st.error("❌ Échec de l'analyse IA : Le service de génération de texte n'est pas disponible.")
                return "Analyse IA indisponible (erreur de connexion ou de l'API)."
        except Exception as e:
            st.error(f"❌ Erreur inattendue lors de l'appel à l'API Gemini: {e}")
            return "Analyse IA indisponible (erreur interne)."

    return "Analyse IA non générée."


# --- 6. INTERFACE UTILISATEUR (SIDEBAR) ---

with st.sidebar:
    st.header("🔍 Localisation")
    
    # Ajout d'un spinner pour le chargement potentiellement plus long
    with st.spinner("Chargement des villes par pagination (cela peut prendre quelques secondes)..."):
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
    
    # Récupération de la clé de jointure (Code Postal) correspondant au choix
    # Utiliser un masque booléen pour trouver la ligne
    row_ville = df_villes[df_villes['label'] == selected_label].iloc[0]
    
    # On récupère la valeur du Code Postal
    join_key_value = row_ville[st.session_state.join_id]
    
    st.divider()
    st.caption(f"Clé de Jointure ({st.session_state.join_id.replace('_', ' ').title()}) : {join_key_value}")
    st.caption(f"Code INSEE réel : {row_ville['code_insee']}")
    st.caption("Données sources : DVF (Etalab) & ANIL (Carte des Loyers)")

# --- 7. DASHBOARD PRINCIPAL ---

st.title(f"Analyse Immobilière : {row_ville['nom_commune']}")

if join_key_value:
    
    # Chargement des données détaillées en utilisant la nouvelle clé de jointure
    with st.spinner("Chargement des données de marché et transactions..."):
        info_ville = get_city_data_full(join_key_value)
        df_transac = get_transactions(join_key_value)

    # --- CALCUL DES KPIS (NÉCESSAIRE POUR L'IA) ---
    prix_m2_achat = df_transac['prix_m2'].median() if not df_transac.empty else 0.0
    prix_m2_achat = float(prix_m2_achat) if pd.notna(prix_m2_achat) else 0.0
    
    loyer_keys = ['loypredm2', 'loyer_m2_appart_moyen_all'] 
    raw_loyer_m2 = None
    
    typ_pred = "N/A"
    
    if info_ville:
        typ_pred = info_ville.get('TYPPRED', 'N/A')
        for key in loyer_keys:
            raw_loyer_m2 = info_ville.get(key)
            if raw_loyer_m2 is not None:
                break 

    loyer_m2 = convert_loyer_to_float(raw_loyer_m2)
    
    renta_brute = 0.0
    if prix_m2_achat > 0 and loyer_m2 > 0:
        renta_brute = ((loyer_m2 * 12) / prix_m2_achat) * 100
    
    derniere_annee = df_transac['date_mutation'].dt.year.max() if not df_transac.empty else "N/A"
    
    delta_prix = 0
    if pd.notna(derniere_annee) and derniere_annee != "N/A" and not df_transac.empty:
        prix_m2_historique = df_transac['prix_m2'].median()
        prix_m2_recent = df_transac[df_transac['date_mutation'].dt.year == derniere_annee]['prix_m2'].median()
        prix_m2_recent = float(prix_m2_recent) if pd.notna(prix_m2_recent) else prix_m2_achat
        delta_prix = int(prix_m2_recent - prix_m2_historique)
    
    nb_transactions = len(df_transac)
    
    # --- SECTION A : KPI MARKET (Réutilisation du code précédent) ---
    if info_ville or not df_transac.empty: # Afficher même si seul df_transac est présent pour les KPI
        
        st.subheader("Indicateurs Clés de Marché")
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        
        kpi1.metric(
            "Prix Achat Médian", 
            f"{int(prix_m2_achat)} €/m²" if prix_m2_achat > 0 else "N/A",
            delta=f"{delta_prix} € vs historique"
        )
        
        kpi2.metric(
            "Loyer Estimé (Appt)", 
            f"{loyer_m2:.1f} €/m²" if loyer_m2 > 0 else "N/A",
            help=f"Basé sur une prédiction de type : {typ_pred}"
        )
        
        kpi3.metric(
            "Rentabilité Brute", 
            f"{renta_brute:.2f} %" if renta_brute > 0 else "N/A",
            delta="Opportunité" if renta_brute > 6 else "Marché tendu"
        )
        
        kpi4.metric(
            "Volume de Ventes", 
            f"{nb_transactions}",
            help="Nombre total de transactions analysées (limite max: 50 000)"
        )
        
        # --- SECTION B : ANALYSE IA (NOUVEAU) ---
        st.divider()
        st.subheader("🤖 Analyse du Marché pour l'Investisseur (Générée par IA)")
        
        if prix_m2_achat > 0 and loyer_m2 > 0:
            with st.spinner("Génération de l'analyse des Points Forts/Faibles..."):
                analysis_text = get_ai_market_analysis(
                    row_ville['nom_commune'], 
                    prix_m2_achat, 
                    loyer_m2, 
                    renta_brute, 
                    typ_pred, 
                    nb_transactions, 
                    delta_prix
                )
                st.markdown(analysis_text)
        else:
            st.info("💡 L'analyse IA sera disponible dès que les métriques principales (Prix Achat Médian et Loyer Estimé) seront disponibles.")

        st.divider()

        # --- SECTION C : GRAPHIQUES (Affiches seulement si transactions > 0) ---
        if not df_transac.empty:
            
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
                if prix_m2_achat > 0:
                    fig_hist.add_vline(x=prix_m2_achat, line_dash="dash", line_color="red", annotation_text="Médiane")
                st.plotly_chart(fig_hist, use_container_width=True)

            # --- SECTION D : DATA EXPLORER ---
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
        else:
            # S'il y a des info_ville mais pas de transaction
            st.info("👋 Aucune transaction (Fct_transaction_immo) trouvée pour ce Code Postal (ou toutes les transactions ont été filtrées).")
        
    # GESTION DES CAS VIDES
    else: # si info_ville n'a rien retourné
        st.error(f"❌ ERREUR DE RÉFÉRENTIEL : Les données de loyer (Dim_ville) sont introuvables pour le Code Postal : {join_key_value}. (Vérifiez si la colonne `code_postal` est bien remplie dans Dim_ville)")
        if not df_transac.empty:
            st.info("💡 Cependant, des transactions ont été trouvées pour cette ville. Le problème est que le loyer ne peut pas être estimé sans les données de Dim_ville.")
            st.dataframe(df_transac.head())
