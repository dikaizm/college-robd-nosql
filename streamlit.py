import streamlit as st
import pandas as pd
from pymongo import MongoClient
from neo4j import GraphDatabase
from datetime import datetime

# --- Konfigurasi dan Koneksi Database ---
username = 'root'  # Replace with your MongoDB username
password = '1234abcd'  # Replace with your MongoDB password

# Ganti dengan URI dan kredensial Anda yang sebenarnya jika berbeda
MONGO_URI = f"mongodb://{username}:{password}@localhost:27017/"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "yourStrongPassword"
    
MONGO_DB_NAME = "TIKET"
MONGO_ORDERS_COLLECTION = "orders"
MONGO_FLIGHT_PRICES_COLLECTION = "flight_prices"

# Fungsi untuk koneksi ke MongoDB
@st.cache_resource # Cache resource untuk koneksi database
def get_mongo_client():
    """Membuat dan mengembalikan klien MongoDB."""
    try:
        client = MongoClient(MONGO_URI)
        # Ping server untuk memastikan koneksi berhasil
        client.admin.command('ping')
        st.sidebar.success("Berhasil terhubung ke MongoDB!")
        return client
    except Exception as e:
        st.sidebar.error(f"Koneksi MongoDB gagal: {e}")
        return None

# Fungsi untuk koneksi ke Neo4j
@st.cache_resource # Cache resource untuk koneksi database
def get_neo4j_driver():
    """Membuat dan mengembalikan driver Neo4j."""
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        st.sidebar.success("Berhasil terhubung ke Neo4j!")
        return driver
    except Exception as e:
        st.sidebar.error(f"Koneksi Neo4j gagal: {e}")
        return None

mongo_client = get_mongo_client()
neo4j_driver = get_neo4j_driver()

# --- Fungsi Pengambilan Data ---

@st.cache_data(ttl=600) # Cache data selama 10 menit
def get_top_profitable_routes_mongo(n_routes=10):
    """Mengambil N rute paling menguntungkan dari MongoDB."""
    if not mongo_client:
        return pd.DataFrame()
    try:
        db = mongo_client[MONGO_DB_NAME]
        pipeline = [
            {
                "$match": {
                    "depart_date": {
                        "$gte": datetime(2023, 3, 10),
                        "$lte": datetime(2023, 4, 9, 23, 59, 59) # Pastikan rentang mencakup keseluruhan hari
                    },
                    "flight_id": { "$type": "int" }
                }
            },
            {
                "$lookup": {
                    "from": MONGO_FLIGHT_PRICES_COLLECTION,
                    "localField": "flight_id",
                    "foreignField": "id", # Pastikan field ini benar di koleksi flight_prices
                    "as": "flight_info"
                }
            },
            {"$unwind": "$flight_info"},
            {
                "$addFields": {
                    "price_diff": {"$subtract": ["$total_price", "$flight_info.best_price"]}
                }
            },
            {
                "$group": {
                    "_id": {
                        "origin": "$origin",
                        "destination": "$destination"
                    },
                    "total_order": {"$sum": 1},
                    "total_revenue": {"$sum": "$total_price"},
                    "avg_diff": {"$avg": "$price_diff"}
                }
            },
            {"$sort": {"avg_diff": -1}},
            {"$limit": n_routes},
            {
                "$project": {
                    "_id": 0,
                    "origin": "$_id.origin",
                    "destination": "$_id.destination",
                    "total_order": 1,
                    "total_revenue": 1,
                    "avg_diff": 1
                }
            }
        ]
        result = list(db[MONGO_ORDERS_COLLECTION].aggregate(pipeline))
        return pd.DataFrame(result)
    except Exception as e:
        st.error(f"Error mengambil data profitabilitas dari MongoDB: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def get_longest_routes_neo4j(n_routes=30000):
    """Mengambil rute terpanjang dari Neo4j."""
    if not neo4j_driver:
        return pd.DataFrame()
    try:
        def get_longest_routes(tx):
            query = """
            MATCH (a:Airport)-[r:CONNECTED_TO]->(b:Airport)
            WHERE r.distance_km > 1000 AND r.flight_time_hr IS NOT NULL
            RETURN 
                a.airport_code AS origin,
                b.airport_code AS destination,
                r.distance_km AS distance_km,
                r.flight_time_hr AS flight_time_hr
            ORDER BY r.distance_km DESC
            LIMIT $n_routes
            """
            return list(tx.run(query, n_routes=n_routes))

        with neo4j_driver.session() as session:
            neo4j_results = session.execute_read(get_longest_routes)
            data = [dict(record) for record in neo4j_results]
            return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Error mengambil data rute terpanjang dari Neo4j: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def get_combined_analysis(n_longest_routes_neo4j=30000, n_profitable_routes_mongo=100):
    """Menggabungkan analisis rute jauh (Neo4j) dan menguntungkan (MongoDB)."""
    if not mongo_client or not neo4j_driver:
        return pd.DataFrame()
    try:
        # Ambil data rute terpanjang dari Neo4j
        neo4j_df = get_longest_routes_neo4j(n_longest_routes_neo4j)
        
        if neo4j_df.empty:
            st.warning("Tidak ada data rute terpanjang dari Neo4j.")
            return pd.DataFrame()

        # Ambil data rute paling menguntungkan dari MongoDB
        db = mongo_client[MONGO_DB_NAME]
        pipeline_profitable = [
            {
                "$match": {
                    "depart_date": {
                        "$gte": datetime(2023, 3, 10),
                        "$lte": datetime(2023, 4, 9)
                    },
                    "flight_id": { "$type": "int" }
                }
            },
            {
                "$lookup": {
                    "from": MONGO_FLIGHT_PRICES_COLLECTION,
                    "localField": "flight_id",
                    "foreignField": "id",
                    "as": "flight_info"
                }
            },
            { "$unwind": "$flight_info" },
            {
                "$addFields": {
                    "price_diff": { "$subtract": ["$total_price", "$flight_info.best_price"] }
                }
            },
            {
                "$group": {
                    "_id": {
                        "origin": "$origin",
                        "destination": "$destination"
                    },
                    "avg_diff": { "$avg": "$price_diff" }
                }
            },
            { "$sort": { "avg_diff": -1 } },
            { "$limit": n_profitable_routes_mongo },
            {
                "$project": {
                    "_id": 0,
                    "origin": "$_id.origin",
                    "destination": "$_id.destination",
                    "avg_diff": 1
                }
            }
        ]
        mongo_results = list(db[MONGO_ORDERS_COLLECTION].aggregate(pipeline_profitable))
        mongo_df = pd.DataFrame(mongo_results)

        if mongo_df.empty:
            st.warning("Tidak ada data rute menguntungkan dari MongoDB untuk periode Ramadhan.")
            return pd.DataFrame()
        
        # Gabungkan kedua DataFrame
        combined_df = pd.merge(
            neo4j_df,
            mongo_df,
            on=["origin", "destination"],
            how="inner"
        )

        # Urutkan berdasarkan kombinasi jarak dan keuntungan
        if not combined_df.empty:
            combined_df = combined_df.sort_values(
                by=["distance_km", "avg_diff"],
                ascending=[False, False]
            )
        
        return combined_df

    except Exception as e:
        st.error(f"Error melakukan analisis gabungan: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def get_airport_connections(airport_code):
    """Mengambil informasi koneksi untuk bandara tertentu."""
    if not neo4j_driver:
        return pd.DataFrame()
    try:
        def get_connections(tx):
            query = """
            MATCH (a:Airport)-[:CONNECTED_TO]-(b:Airport)
            WHERE a.airport_code = $airport_code
            RETURN 
                a.airport_code AS airport,
                a.city AS city,
                COUNT(DISTINCT b) AS total_connections
            ORDER BY total_connections DESC
            LIMIT 10
            """
            return list(tx.run(query, airport_code=airport_code))

        with neo4j_driver.session() as session:
            results = session.execute_read(get_connections)
            data = [dict(record) for record in results]
            return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Error mengambil data koneksi bandara: {e}")
        return pd.DataFrame()

# --- UI Streamlit ---
#st.set_page_config(layout="wide", page_title="Dashboard Analisis Tiket")

st.title("✈️ Dashboard Analisis Data Tiket Pesawat")
st.markdown("Dashboard ini menampilkan analisis data tiket dari MongoDB dan Neo4j.")

# Sidebar untuk filter global jika diperlukan di masa mendatang
st.sidebar.header("Filter Global")
st.sidebar.date_input("Pilih Rentang Tanggal", [datetime(2023,1,1), datetime(2023,12,31)])


tab1, tab2, tab3 = st.tabs(["📊 Profitabilitas Rute (MongoDB)", "🗺️ Rute Terpanjang (Neo4j)", "🔗 Analisis Gabungan"])

with tab1:
    st.header("Top Rute Paling Menguntungkan (Selama Ramadhan)")
    st.markdown("Menampilkan rute dengan rata-rata selisih harga (keuntungan) tertinggi selama periode Ramadhan (10 Maret - 9 April 2023).")
    
    num_profitable_routes = st.slider("Jumlah rute yang ditampilkan:", min_value=5, max_value=50, value=10, key="profitable")
    
    if mongo_client:
        df_profitable = get_top_profitable_routes_mongo(num_profitable_routes)
        if not df_profitable.empty:
            st.dataframe(df_profitable.style.format({"total_revenue": "{:,.0f}", "avg_diff": "{:,.0f}"}))
            
            st.subheader("Visualisasi Rata-rata Keuntungan per Rute")
            # Grafik keuntungan
            # (neo4j_df, mongo_df, on=["origin", "destination"], how="inner")
            chart_data_profit = df_profitable.set_index('origin')['avg_diff']
            if not chart_data_profit.empty:
                 st.bar_chart(chart_data_profit)
            else:
                st.write("Tidak ada data untuk ditampilkan di grafik keuntungan.")

        else:
            st.warning("Tidak ada data profitabilitas rute yang ditemukan atau koneksi MongoDB gagal.")
    else:
        st.error("Koneksi ke MongoDB tidak berhasil. Periksa konfigurasi.")


with tab2:
    st.header("Top Rute Terpanjang")
    st.markdown("Menampilkan rute penerbangan terpanjang.")

    # Section 1: Longest Routes
    st.subheader("1. Rute Terpanjang")
    num_longest_routes = st.slider("Jumlah rute yang ditampilkan:", min_value=5, max_value=50, value=10, key="longest")

    if neo4j_driver:
        df_longest = get_longest_routes_neo4j(num_longest_routes)
        if not df_longest.empty:
            st.dataframe(df_longest.style.format({"distance_km": "{:,.2f} km", "flight_time_hr": "{:,.2f} jam"}))
            
            st.markdown("#### Visualisasi Total Jarak per Rute")
            chart_data_distance = df_longest.set_index('origin')['distance_km']
            if not chart_data_distance.empty:
                st.bar_chart(chart_data_distance)
            else:
                st.write("Tidak ada data untuk ditampilkan di grafik jarak.")
        else:
            st.warning("Tidak ada data rute terpanjang yang ditemukan atau koneksi Neo4j gagal.")
    else:
        st.error("Koneksi ke Neo4j tidak berhasil. Periksa konfigurasi.")

    # Section 2: Airport Connections
    st.subheader("2. Analisis Koneksi Bandara")
    st.markdown("Masukkan kode bandara untuk melihat jumlah koneksi yang tersedia.")
    
    # Get list of unique airports from Neo4j for the dropdown
    if neo4j_driver:
        def get_airports(tx):
            query = """
            MATCH (a:Airport)
            RETURN DISTINCT a.airport_code AS code, a.city AS city
            ORDER BY a.airport_code
            """
            return list(tx.run(query))

        with neo4j_driver.session() as session:
            airports = session.execute_read(get_airports)
            airport_options = {f"{record['code']} - {record['city']}": record['code'] 
                             for record in airports}

        selected_airport = st.selectbox(
            "Pilih Bandara:",
            options=list(airport_options.keys()),
            format_func=lambda x: x
        )

        if selected_airport:
            airport_code = airport_options[selected_airport]
            df_connections = get_airport_connections(airport_code)
            
            if not df_connections.empty:
                st.markdown(f"#### Koneksi untuk Bandara {airport_code}")
                st.dataframe(df_connections.style.format({"total_connections": "{:,}"}))
                
                # Visualisasi koneksi
                st.markdown("#### Visualisasi Jumlah Koneksi")
                chart_data = df_connections.set_index('airport')['total_connections']
                st.bar_chart(chart_data)
            else:
                st.warning(f"Tidak ditemukan data koneksi untuk bandara {airport_code}")
    else:
        st.error("Koneksi ke Neo4j tidak berhasil. Periksa konfigurasi.")

with tab3:
    st.header("Analisis Gabungan: Rute Jauh & Menguntungkan (Selama Ramadhan)")
    st.markdown("Menampilkan rute yang memiliki jarak tempuh jauh (>1000km) dan juga menghasilkan keuntungan tinggi selama periode Ramadhan.")

    col1, col2 = st.columns(2)
    with col1:
        num_neo4j_for_combine = st.slider("Jumlah rute terpanjang dari Neo4j untuk dianalisis:", min_value=1000, max_value=30000, value=30000, key="neo_combine")
    with col2:
        num_mongo_for_combine = st.slider("Jumlah rute menguntungkan dari MongoDB untuk dianalisis:", min_value=10, max_value=200, value=100, key="mongo_combine")

    if mongo_client and neo4j_driver:
        df_combined = get_combined_analysis(num_neo4j_for_combine, num_mongo_for_combine)
        if not df_combined.empty:
            st.dataframe(df_combined.style.format({
                "distance_km": "{:,.2f} km",
                "flight_time_hr": "{:,.2f} jam",
                "avg_diff": "{:,.0f}"
            }))

            st.subheader("Scatter Plot: Jarak vs Keuntungan")
            if 'distance_km' in df_combined.columns and 'avg_diff' in df_combined.columns:
                st.scatter_chart(
                    df_combined,
                    x='distance_km',
                    y='avg_diff',
                    color='origin'
                )
            else:
                st.write("Kolom yang diperlukan untuk scatter plot tidak ditemukan.")
        else:
            st.warning("Tidak ada data gabungan yang ditemukan atau salah satu koneksi database gagal.")
    else:
        st.error("Koneksi ke MongoDB atau Neo4j tidak berhasil. Periksa konfigurasi.")

st.sidebar.markdown("---")
