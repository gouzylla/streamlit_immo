import streamlit as st
import pandas as pd
from supabase.client import create_client, Client
from postgrest.exceptions import APIError 
import plotly.express as px
import sys 
import os

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
    # Utilisation de st.secrets pour la configuration dans l'environnement Streamlit
    url = st.secrets.get("SUPABASE_URL", os.environ.get("SUPABASE_URL"))
    key = st.secrets.get("SUPABASE_KEY", os.environ.get("SUPABASE_KEY"))
    
    if not url or not key:
        st.error("❌ Erreur de configuration: Les variables SUPABASE_URL ou SUPABASE_KEY sont manquantes.")
        return None
        
    try:
        # Tente de créer la connexion Supabase
        return create_client(url, key)
    except Exception as e:
        st.error(f"❌ Erreur critique : Impossible de se connecter à Supabase. Détail: {e}")
        return None

supabase = init_connection()

# --- 3. GESTION DE L'ÉTAT ET CLÉ DE JOINTURE ---
if 'join_id' not in st.session_state:
    st.session_state.join_id = 'code_postal'


# --- 4. FONCTIONS DE RÉCUPÉRATION DE DONNÉES (CACHÉES) ---

@st.cache_data(ttl=3600)  # Cache d'1 heure
def get_valid_postal_codes():
    """
    Récupère la liste des codes postaux uniques présents dans la table des transactions.
    Ceci assure que l'on ne propose que des villes pour lesquelles nous avons des données de vente.
    """
    if not supabase: return []
    
    try:
        # Récupère les codes postaux uniques de la table de faits
        # Note: 'code_postal' est de type bigint dans Fct_transaction_immo
        response = supabase.table('Fct_transaction_immo')\
            .select('code_postal', head=True)\
            .order('code_postal', desc=False)\
            .limit(1000000)\
            .execute()
            
        df_cp = pd.DataFrame(response.data)
        if not df_cp.empty:
            # Convertir en chaîne formatée (str.zfill(5)) pour la jointure
            return df_cp['code_postal'].astype(str).str.zfill(5).unique().tolist()
        return []
    except Exception as e:
        print(f"Erreur lors de la récupération des CP valides: {e}", file=sys.stderr)
        return []

@st.cache_data(ttl=3600)  # Cache d'1 heure
def get_villes_list():
    """
    Récupère le référentiel des villes et le filtre pour ne garder que celles
    présentes dans les transactions.
    """
    if not supabase: 
        return pd.DataFrame()
    
    TABLE_DIM_VILLE = 'Dim_ville'
    
    # 1. Récupérer les codes postaux actifs
    valid_cps = get_valid_postal_codes()
    
    # 2. Récupérer toutes les villes (avec pagination)
    PAGE_SIZE = 1000
    all_data = []
    offset = 0
    
    while True:
        try:
            # On ne sélectionne que les colonnes nécessaires au sélecteur
            response = supabase.table(TABLE_DIM_VILLE)\
                .select('code_insee, code_postal, nom_commune')\
                .order('nom_commune', desc=False)\
                .range(offset, offset + PAGE_SIZE - 1)\
                .execute()
            
            current_page_data = response.data
            if not current_page_data: break
            
            all_data.extend(current_page_data)
            if len(current_page_data) < PAGE_SIZE: break
            offset += PAGE_SIZE
            
        except APIError as e:
            st.error(f"❌ Erreur Supabase (villes) : {e}")
            break

    if not all_data: 
        print("DEBUG: Aucune donnée récupérée pour Dim_ville.", file=sys.stderr)
        return pd.DataFrame()
    
    df = pd.DataFrame(all_data)
    
    if not df.empty:
        # Standardisation des clés de jointure
        # code_postal est bigint, on le convertit en string zfill(5) pour correspondre à valid_cps
        df[st.session_state.join_id] = df[st.session_state.join_id].astype(str).str.zfill(5)
        df['code_insee'] = df['code_insee'].astype(str).str.zfill(5)
        
        # FILTRAGE : On ne garde que les villes dont le CP est dans les transactions
        if valid_cps:
            df = df[df['code_postal'].isin(valid_cps)]
        
        # Création label pour le sélecteur
        df['label'] = df['nom_commune'] + " (" + df[st.session_state.join_id].astype(str) + ")"
        df = df.drop_duplicates(subset=['label'])
        
        return df.sort_values('nom_commune')
    return pd.DataFrame()

def get_city_data_full(join_key_value):
    """
    Récupère les infos détaillées. 
    Ajusté pour utiliser le nom de colonne correct `taux_chomage_calcule_pct`.
    """
    if not supabase: return None
    TABLE_DIM_VILLE = 'Dim_ville'
    
    # Liste des colonnes selon le schéma fourni
    extended_columns = [
        'pop_totale', 'part_pop_15_29_ans_pct', 
        'revenu_dispo_median_uc', 'salaire_net_mensuel_moyen', 
        'taux_chomage_calcule_pct' # FIX: Nom de colonne corrigé ici
    ]
    # Colonnes de loyer
    loyer_columns = [
        'loyer_m2_maison_moyen', 'loyer_m2_appart_t1_t2', 'loyer_m2_appart_t3_plus', 'loyer_m2_appart_moyen_all'
    ]
    # Colonnes d'ID
    base_columns = ['code_insee', 'code_postal', 'nom_commune']
    
    select_query = ",".join(base_columns + loyer_columns + extended_columns)
    
    # Assurer que la clé de jointure est formatée correctement (string zfill(5))
    join_key_value_str = str(join_key_value).zfill(5)
    
    try:
        # Tentative 1 : Tout récupérer
        response = supabase.table(TABLE_DIM_VILLE).select(select_query).eq(st.session_state.join_id, join_key_value_str).execute()
        
        if response.data: return response.data[0]
        
    except APIError as e:
        # Erreur 42703 (colonne inexistante) est la plus courante ici
        if e.code == '42703': 
            print(f"⚠️ Avertissement: Colonnes manquantes dans Dim_ville. Tentative de récupération simplifiée (*).", file=sys.stderr)
            try:
                # Tentative 2 : SELECT * (prend tout ce qui existe)
                response = supabase.table(TABLE_DIM_VILLE).select('*').eq(st.session_state.join_id, join_key_value_str).execute()
                if response.data: return response.data[0]
            except Exception as e2:
                st.error(f"❌ Erreur critique récupération ville : {e2}")
        else:
            print(f"Erreur get_city_data_full: {e}", file=sys.stderr)
            st.error(f"❌ Erreur API Supabase : {e.message}")
            
    return None

def get_transactions(join_key_value):
    """
    Récupère l'historique des ventes pour une ville donnée depuis Fct_transaction_immo.
    """
    if not supabase: return pd.DataFrame()
    
    TABLE_FACT_TRANSAC = 'Fct_transaction_immo'
    
    join_key_value_str = str(join_key_value).zfill(5)
    
    try:
        # Note: on récupère code_postal (bigint), on le compare à join_key_value_str (text)
        # La comparaison .eq() gère généralement les types, mais pour être sûr, on pourrait 
        # forcer l'argument en bigint si nécessaire. Ici, on laisse Supabase gérer la conversion.
        response = supabase.table(TABLE_FACT_TRANSAC)\
            .select('date_mutation, valeur_fonciere, surface_reelle_bati, type_local')\
            .eq(st.session_state.join_id, join_key_value_str)\
            .gt('valeur_fonciere', 5000)\
            .gt('surface_reelle_bati', 9)\
            .limit(50000)\
            .execute()
            
    except APIError as e:
        st.error(
            f"❌ Erreur Supabase lors du chargement des transactions."
            f"\nDétail technique: {e.message}"
        )
        return pd.DataFrame()
    
    df = pd.DataFrame(response.data)
    
    if not df.empty:
        # Typage et nettoyage des données
        df['date_mutation'] = pd.to_datetime(df['date_mutation'], errors='coerce')
        df['valeur_fonciere'] = pd.to_numeric(df['valeur_fonciere'], errors='coerce')
        df['surface_reelle_bati'] = pd.to_numeric(df['surface_reelle_bati'], errors='coerce')
        
        df.dropna(subset=['date_mutation', 'valeur_fonciere', 'surface_reelle_bati'], inplace=True)
        
        # Feature Engineering : Prix au m²
        df['prix_m2'] = df['valeur_fonciere'] / df['surface_reelle_bati']
        
        # Filtrage des outliers extrêmes 
        df = df[(df['prix_m2'] > 500) & (df['prix_m2'] < 30000)]
        
    return df

# --- 5. UTILS POUR LA CONVERSION DE DONNÉES ---

def convert_to_float(raw_value):
    """Convertit une valeur potentiellement texte/None en float."""
    if raw_value is None:
        return 0.0
    try:
        # Gère le cas où les données textuelles (comme les revenus) sont importées avec des virgules.
        value_str = str(raw_value).replace(',', '.')
        return float(value_str)
    except ValueError:
        return 0.0

def convert_to_int(raw_value):
    """Convertit une valeur potentiellement texte/None en int."""
    if raw_value is None:
        return 0
    try:
        return int(convert_to_float(raw_value)) # Utilise la conversion float puis int
    except Exception:
        return 0
        
# --- 6. INTERFACE UTILISATEUR (SIDEBAR) ---

with st.sidebar:
    st.header("🔍 Localisation")
    
    # Ajout d'un spinner pour le chargement potentiellement plus long
    with st.spinner("Chargement des villes actives (celles qui ont des transactions)..."):
        # Cette fonction est maintenant essentielle pour filtrer les codes postaux
        df_villes = get_villes_list()
    
    if df_villes.empty:
        st.error("Aucune ville disponible (Vérifiez la connexion ou si Fct_transaction_immo contient des données).")
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

# --- 7. DASHBOARD PRINCIPAL ---

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
        # Calcul du delta par rapport à la moyenne historique
        prix_m2_historique = df_transac['prix_m2'].mean()
        delta_prix = int(prix_m2_achat - prix_m2_historique)
    
    nb_transactions = len(df_transac)
    
    # Données de Loyer (Dim_ville)
    loyer_m2_all = convert_to_float(info_ville.get('loyer_m2_appart_moyen_all')) if info_ville else 0.0
    
    loyer_m2_data = {
        "Appartement T1-T2": convert_to_float(info_ville.get('loyer_m2_appart_t1_t2')) if info_ville else 0.0,
        "Appartement T3 et +": convert_to_float(info_ville.get('loyer_m2_appart_t3_plus')) if info_ville else 0.0,
        "Maison": convert_to_float(info_ville.get('loyer_m2_maison_moyen')) if info_ville else 0.0,
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
            "Prix Achat Médian (m²)", 
            f"{int(prix_m2_achat):,} €" if prix_m2_achat > 0 else "N/A",
            delta=f"{delta_prix} € vs moyenne" if delta_prix != 0 else None
        )
        
        kpi2.metric(
            "Loyer Moyen Estimé (Appt)", 
            f"{loyer_m2_all:.1f} €/m²" if loyer_m2_all > 0 else "N/A",
        )
        
        kpi3.metric(
            "Rentabilité Brute Estimée", 
            f"{renta_brute:.2f} %" if renta_brute > 0 else "N/A",
            delta="Potentiel d'investissement" if renta_brute > 6 else "Marché classique"
        )
        
        kpi4.metric(
            "Volume de Ventes", 
            f"{nb_transactions:,}" if nb_transactions > 0 else "N/A",
            help="Nombre total de transactions analysées (limite max: 50 000)"
        )
        
        st.divider()

        # --- SECTION B : ANALYSE SOCIOCULTURELLE (INSEE) ---
        st.subheader("🌍 Profil Socio-démographique et Économique (Source INSEE)")
        
        # Récupération et conversion des données INSEE (utilisant .get() pour la robustesse)
        try:
            pop_totale = convert_to_int(info_ville.get('pop_totale')) if info_ville else 0
            part_jeunes = convert_to_float(info_ville.get('part_pop_15_29_ans_pct')) if info_ville else 0.0
            revenu_median = convert_to_int(info_ville.get('revenu_dispo_median_uc')) if info_ville else 0
            salaire_moyen = convert_to_int(info_ville.get('salaire_net_mensuel_moyen')) if info_ville else 0
            # FIX: Utilisation du nom de colonne corrigé
            taux_chomage = convert_to_float(info_ville.get('taux_chomage_calcule_pct')) if info_ville else 0.0
            
        except Exception as e:
            st.error(f"Erreur lors du traitement des données INSEE : {e}")
            pop_totale = revenu_median = salaire_moyen = 0
            part_jeunes = taux_chomage = 0.0

        # Affichage des métriques INSEE
        col_demo1, col_demo2, col_demo3, col_demo4 = st.columns(4)
        col_demo1.metric("Population totale", f"{pop_totale:,.0f} hab." if pop_totale > 0 else 'N/A')
        col_demo2.metric("Part 15-29 ans", f"{part_jeunes:.1f} %" if part_jeunes > 0 else 'N/A', help="Indique le dynamisme potentiel (étudiants, jeunes actifs).")
        col_demo3.metric("Salaire net mensuel (moyen)", f"{salaire_moyen:,} €" if salaire_moyen > 0 else 'N/A')
        col_demo4.metric("Taux de Chômage", f"{taux_chomage:.1f} %" if taux_chomage > 0 else 'N/A')
        
        st.divider()

        # --- SECTION C : LOYERS DÉTAILLÉS ---
        st.subheader("🏡 Loyers Estimés par Typologie (Source ANIL)")
        
        # Préparation du DataFrame pour le tableau des loyers
        df_loyer = pd.DataFrame(
            [
                ("Appartement T1-T2", loyer_m2_data.get("Appartement T1-T2", 0.0)),
                ("Appartement T3 et +", loyer_m2_data.get("Appartement T3 et +", 0.0)),
                ("Maison", loyer_m2_data.get("Maison", 0.0))
            ], 
            columns=['Typologie', 'Loyer_m2']
        ).sort_values('Loyer_m2', ascending=False)
        
        df_loyer_filtered = df_loyer[df_loyer['Loyer_m2'] > 0] 

        if not df_loyer_filtered.empty:
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
            st.warning("⚠️ Les données de loyer détaillées sont absentes dans la table `Dim_ville` pour cette ville.")
            
        st.divider()

        # --- SECTION D : GRAPHIQUES HISTORIQUES ---
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
                df_hist = df_transac[df_transac['prix_m2'] < 10000] # Limite visuelle pour la clarté
                fig_hist = px.histogram(
                    df_hist, x="prix_m2", nbins=25,
                    title="Répartition des prix d'achat au m²",
                    color_discrete_sequence=['#636EFA']
                )
                if prix_m2_achat > 0:
                    fig_hist.add_vline(x=prix_m2_achat, line_dash="dash", line_color="red", annotation_text="Médiane", annotation_position="top left")
                st.plotly_chart(fig_hist, use_container_width=True)

            # --- SECTION E : DATA EXPLORER ---
            with st.expander("📂 Voir les dernières transactions détaillées"):
                st.dataframe(
                    df_transac[['date_mutation', 'valeur_fonciere', 'surface_reelle_bati', 'prix_m2', 'type_local']]
                    .sort_values('date_mutation', ascending=False),
                    column_config={
                        "date_mutation": "Date",
                        "valeur_fonciere": st.column_config.NumberColumn("Prix", format="%.0f €"),
                        "surface_reelle_bati": st.column_config.NumberColumn("Surface", format="%.0f m²"),
                        "prix_m2": st.column_config.NumberColumn("Prix/m²", format="%.2f €"),
                        "type_local": "Type de Bien"
                    },
                    use_container_width=True
                )
        else:
            # S'il y a des info_ville mais pas de transaction
            st.info("👋 Aucune transaction (Fct_transaction_immo) trouvée pour ce Code Postal (ou toutes les transactions ont été filtrées).")
        
    # GESTION DES CAS VIDES
    else: # si info_ville n'a rien retourné
        st.error(f"❌ ERREUR DE RÉFÉRENTIEL : La ligne de données de la ville est introuvable pour le Code Postal : {join_key_value}. Vérifiez la table `Dim_ville` dans Supabase.")
