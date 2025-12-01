import streamlit as st
import pandas as pd
from supabase.client import create_client, Client
from postgrest.exceptions import APIError 
import plotly.express as px
import sys 

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

# Clé de jointure sur Code Postal
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
            
            # Vérification de la condition d'arrêt
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
        # Assurer que code_postal (la clé de jointure) est une chaîne de caractères de 5 chiffres
        df[st.session_state.join_id] = df[st.session_state.join_id].astype(str).str.zfill(5)
        # Assurer que code_insee est une chaîne de caractères de 5 chiffres
        df['code_insee'] = df['code_insee'].astype(str).str.zfill(5)
        
        # Création d'une étiquette propre pour la liste déroulante
        df['label'] = df['nom_commune'] + " (" + df[st.session_state.join_id].astype(str) + ")"
        df = df.drop_duplicates(subset=['label'])
        
        # Pour le debugging
        print(f"DEBUG: {len(df)} villes (uniques) chargées via pagination. Clé de jointure: {st.session_state.join_id}", file=sys.stderr)
        
        return df.sort_values('nom_commune')
    return pd.DataFrame()

def get_city_data_full(join_key_value):
    """
    Récupère les infos détaillées de loyer pour une ville donnée depuis Dim_ville.
    Utilise le Code Postal comme clé de recherche.
    """
    if not supabase: return None
    TABLE_DIM_VILLE = 'Dim_ville'
    
    # Colonnes de loyer réelles dans la base de données de l'utilisateur
    # Note: Si vous ajoutez d'autres colonnes INSEE, ajoutez-les ici aussi.
    select_columns = (
        'code_insee, code_postal, nom_commune, '
        'loyer_m2_maison_moyen, loyer_m2_appart_t1_t2, loyer_m2_appart_t3_plus'
    )
    
    # Assurer que l'identifiant de recherche (Code Postal) est bien une chaîne de caractères
    join_key_value_str = str(join_key_value).zfill(5)
    
    print(f"DEBUG: get_city_data_full cherche {st.session_state.join_id}='{join_key_value_str}'", file=sys.stderr)
    
    try:
        # Recherche par Code Postal
        response = supabase.table(TABLE_DIM_VILLE).select(select_columns).eq(st.session_state.join_id, join_key_value_str).execute()
        
        if response.data:
            # On prend la première ligne 
            return response.data[0] 
        
    except APIError as e:
        # Ajout d'une erreur si la structure de table est encore incorrecte (colonnes manquantes)
        if 'column "loyer_m2' in str(e):
             st.error("❌ ERREUR STRUCTURE DE TABLE : Une ou plusieurs colonnes de loyer sont introuvables. Vérifiez l'orthographe exacte.")
        print(f"Erreur get_city_data_full: {e}", file=sys.stderr)
        
    return None

def get_transactions(join_key_value):
    """
    Récupère l'historique des ventes pour une ville donnée depuis Fct_transaction_immo.
    Utilise le Code Postal comme clé de recherche.
    """
    if not supabase: return pd.DataFrame()
    
    TABLE_FACT_TRANSAC = 'Fct_transaction_immo'
    
    # Assurer que l'identifiant de recherche (Code Postal) est bien une chaîne de caractères
    join_key_value_str = str(join_key_value).zfill(5)
    
    print(f"DEBUG: get_transactions cherche {st.session_state.join_id}='{join_key_value_str}'", file=sys.stderr)
    
    try:
        # Recherche par Code Postal
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
        
# --- 5. INTERFACE UTILISATEUR (SIDEBAR) ---

with st.sidebar:
    st.header("🔍 Localisation")
    
    # Ajout d'un spinner pour le chargement potentiellement plus long
    with st.spinner("Chargement des villes par pagination..."):
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
    row_ville = df_villes[df_villes['label'] == selected_label].iloc[0]
    
    # On récupère la valeur du Code Postal (clé de jointure)
    join_key_value = row_ville[st.session_state.join_id] # Code Postal
    
    st.divider()
    st.caption(f"Clé de Jointure utilisée (Code Postal) : {join_key_value}")
    st.caption(f"Code INSEE de référence : {row_ville['code_insee']}")
    st.caption("Données sources : DVF (Etalab) & ANIL (Carte des Loyers)")

# --- 6. DASHBOARD PRINCIPAL ---

st.title(f"Analyse Immobilière : {row_ville['nom_commune']}")

if join_key_value:
    
    # Chargement des données détaillées en utilisant le Code Postal
    with st.spinner("Chargement des données de marché et transactions..."):
        info_ville = get_city_data_full(join_key_value)
        df_transac = get_transactions(join_key_value)

    # --- CALCUL DES KPIS & DONNÉES DE LOYER DÉTAILLÉES ---
    
    # Données d'achat (Transactions)
    prix_m2_achat = df_transac['prix_m2'].median() if not df_transac.empty else 0.0
    prix_m2_achat = float(prix_m2_achat) if pd.notna(prix_m2_achat) else 0.0
    
    derniere_annee = df_transac['date_mutation'].dt.year.max() if not df_transac.empty else "N/A"
    
    delta_prix = 0
    if pd.notna(derniere_annee) and derniere_annee != "N/A" and not df_transac.empty:
        # On calcule le delta par rapport à la médiane historique de toutes les transactions chargées
        prix_m2_historique = df_transac['prix_m2'].median()
        prix_m2_recent = df_transac[df_transac['date_mutation'].dt.year == derniere_annee]['prix_m2'].median()
        prix_m2_recent = float(prix_m2_recent) if pd.notna(prix_m2_recent) else prix_m2_achat
        delta_prix = int(prix_m2_recent - prix_m2_historique)
    
    nb_transactions = len(df_transac)
    
    # Données de Loyer (Dim_ville) - UTILISATION DES NOUVEAUX NOMS DE COLONNES
    
    loyer_m2_t1t2 = convert_loyer_to_float(info_ville.get('loyer_m2_appart_t1_t2')) if info_ville else 0.0
    loyer_m2_t3plus = convert_loyer_to_float(info_ville.get('loyer_m2_appart_t3_plus')) if info_ville else 0.0
    loyer_m2_maison = convert_loyer_to_float(info_ville.get('loyer_m2_maison_moyen')) if info_ville else 0.0

    # Estimation du loyer moyen Appartement global
    loyers_appart = [l for l in [loyer_m2_t1t2, loyer_m2_t3plus] if l > 0]
    loyer_m2_all = sum(loyers_appart) / len(loyers_appart) if loyers_appart else 0.0
    
    loyer_m2_data = {
        "Appartement T1-T2": loyer_m2_t1t2,
        "Appartement T3 et +": loyer_m2_t3plus,
        "Maison": loyer_m2_maison,
        "Appartement (Global Estimé)": loyer_m2_all, 
    }
    
    # Calcul de la rentabilité brute
    renta_brute = 0.0
    if prix_m2_achat > 0 and loyer_m2_all > 0:
        renta_brute = ((loyer_m2_all * 12) / prix_m2_achat) * 100
    
    # --- SECTION A : KPI MARKET ---
    if info_ville or not df_transac.empty: 
        
        st.subheader("Indicateurs Clés de Marché")
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        
        kpi1.metric(
            "Prix Achat Médian", 
            f"{int(prix_m2_achat)} €/m²" if prix_m2_achat > 0 else "N/A",
            delta=f"{delta_prix} € vs historique"
        )
        
        # Le KPI du loyer utilise le loyer 'Global Estimé' comme référence
        kpi2.metric(
            "Loyer Moyen Estimé (Appt)", 
            f"{loyer_m2_all:.1f} €/m²" if loyer_m2_all > 0 else "N/A",
        )
        
        kpi3.metric(
            "Rentabilité Brute (Base Appt)", 
            f"{renta_brute:.2f} %" if renta_brute > 0 else "N/A",
            delta="Opportunité" if renta_brute > 6 else "Marché tendu"
        )
        
        kpi4.metric(
            "Volume de Ventes", 
            f"{nb_transactions}",
            help="Nombre total de transactions analysées (limite max: 50 000)"
        )
        
        st.divider()

        # --- SECTION B : ANALYSE DES LOYERS DÉTAILLÉS (TABLEAU SEUL) ---
        st.subheader("📊 Loyers Estimés par Typologie")
        
        # Préparation des données pour le tableau
        df_loyer = pd.DataFrame(
            [
                ("Appartement T1-T2", loyer_m2_data.get("Appartement T1-T2", 0.0)),
                ("Appartement T3 et +", loyer_m2_data.get("Appartement T3 et +", 0.0)),
                ("Maison", loyer_m2_data.get("Maison", 0.0))
            ], 
            columns=['Typologie', 'Loyer_m2']
        ).sort_values('Loyer_m2', ascending=False)
        
        df_loyer_filtered = df_loyer[df_loyer['Loyer_m2'] > 0] # Filtrer les valeurs absentes (si 0.0)

        if not df_loyer_filtered.empty:
            
            # Affichage du tableau des données
            st.dataframe(
                df_loyer_filtered,
                column_config={
                    "Typologie": "Type de Bien",
                    "Loyer_m2": st.column_config.NumberColumn("Loyer Estimé (€/m²)", format="%.2f €")
                },
                hide_index=True,
                use_container_width=True
            )
            
        else:
            # Message d'alerte si les loyers sont N/A
            st.warning("⚠️ Les données de loyer (Loyers Moyens et détaillés) sont absentes dans la table `Dim_ville` pour cette ville. La ligne a été trouvée, mais les colonnes de loyer sont vides/nulles.")
            
            # Aide au débogage : Affichage de la ligne de Dim_ville trouvée
            if info_ville:
                st.info(f"💡 DEBUG: La ligne de `Dim_ville` trouvée pour {join_key_value} contient ces données brutes :")
                st.json(info_ville)

        st.divider()

        # --- SECTION C : GRAPHIQUES HISTORIQUES ---
        if not df_transac.empty:
            
            g1, g2 = st.columns([2, 1])
            
            with g1:
                st.subheader("📈 Évolution des prix d'achat")
                # Agrégation par Trimestre
                df_transac['trimestre'] = df_transac['date_mutation'].dt.to_period('Q').astype(str)
                df_trend = df_transac.groupby('trimestre')['prix_m2'].median().reset_index()
                
                fig_line = px.line(
                    df_trend, x='trimestre', y='prix_m2', markers=True,
                    title="Prix médian au m² par trimestre (Transactions DVF)",
                    labels={'prix_m2': 'Prix €/m²', 'trimestre': 'Période'}
                )
                fig_line.update_layout(xaxis_title=None)
                st.plotly_chart(fig_line, use_container_width=True)
                
            with g2:
                st.subheader("📊 Distribution des prix")
                fig_hist = px.histogram(
                    df_transac, x="prix_m2", nbins=25,
                    title="Répartition des prix d'achat au m²",
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
        st.error(f"❌ ERREUR DE RÉFÉRENTIEL : La ligne de données de loyer (Dim_ville) est introuvable pour le Code Postal : {join_key_value}. (Vérifiez la table `Dim_ville`.)")
        if not df_transac.empty:
            st.info("💡 Des transactions ont cependant été trouvées. Le problème semble être que les données de loyer manquent dans `Dim_ville` pour cet identifiant.")
