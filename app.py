import streamlit as st
import pandas as pd
from supabase import create_client, Client
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

#Connecting Database
SUPABASE_URL = st.secrets["SUPABASE_URL"].strip()
SUPABASE_KEY = st.secrets["SUPABASE_KEY"].strip()
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

#Configuration
st.set_page_config(page_title="Industrial AI Command Center", layout="wide", page_icon="🏭")

@st.cache_data(ttl=60)
def load_all_data():
    #Fetching data from dataset
    res_a = supabase.table("energy_prices").select("*").order("timestamp", desc=True).limit(1500).execute()
    res_p = supabase.table("price_predictions").select("*").order("timestamp", desc=True).limit(300).execute()
    
    df_a = pd.DataFrame(res_a.data)
    if not df_a.empty:
        df_a['timestamp'] = pd.to_datetime(df_a['timestamp'])
        df_a['price_eur_mwh'] = pd.to_numeric(df_a['price_eur_mwh'], errors='coerce')
        # Null Value Fixation
        df_a = df_a.sort_values('timestamp')
        df_a['price_eur_mwh'] = df_a['price_eur_mwh'].ffill().fillna(0)
        df_a['Year'] = df_a['timestamp'].dt.year
        df_a['Month'] = df_a['timestamp'].dt.strftime('%B')
        df_a['Day'] = df_a['timestamp'].dt.day_name()
        df_a['Date'] = df_a['timestamp'].dt.date
        df_a['Hour'] = df_a['timestamp'].dt.hour
        
        def categorize(p):
            if p > 120: return '🔴 Expensive'
            if p < 60: return '🟢 Cheap'
            return '🟡 Normal'
        df_a['Status_Zone'] = df_a['price_eur_mwh'].apply(categorize)

    # Prediction
    df_p = pd.DataFrame(res_p.data) if res_p.data else pd.DataFrame()
    if not df_p.empty:
        df_p['timestamp'] = pd.to_datetime(df_p['timestamp'])
        df_p['predicted_price'] = pd.to_numeric(df_p['predicted_price'], errors='coerce').ffill()
        df_p = df_p.sort_values('timestamp')

    return df_a, df_p

try:
    actuals, preds = load_all_data()
    
    #Inserting Slicers
    st.sidebar.header("🎯 Navigation & Filters")
    years = sorted(actuals['Year'].unique())
    selected_year = st.sidebar.multiselect("Select Year", years, default=years)
    months = list(actuals.sort_values('timestamp')['Month'].unique())
    selected_month = st.sidebar.multiselect("Select Month", months, default=months)
    
    # Apply Filtering
    mask = (actuals['Year'].isin(selected_year)) & (actuals['Month'].isin(selected_month))
    filtered_actuals = actuals.loc[mask]
    filtered_actuals = filtered_actuals.copy()
    filtered_actuals['Hours'] = 1

    #UI/UX of the page
    st.title("🏭 Smart Factory: AI Energy Hub")
    
    #KPI Insertion
    if not filtered_actuals.empty:
        latest_p = float(filtered_actuals['price_eur_mwh'].iloc[-1])
        avg_p = float(filtered_actuals['price_eur_mwh'].mean())
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Live Market Price", f"{latest_p:,.0f} €")
        c2.metric("Filtered Avg", f"{avg_p:,.0f} €")
        c3.metric("Peak Price", f"{filtered_actuals['price_eur_mwh'].max():,.0f} €")
        
        # Accuracy %
        merged = pd.merge(actuals, preds, on='timestamp', how='inner')
        if not merged.empty:
            error = (merged['price_eur_mwh'] - merged['predicted_price']).abs().mean()
            rel = max(0, 100 - (error / actuals['price_eur_mwh'].mean() * 100))
            c4.metric("AI Reliability", f"{rel:.1f}%")
        else:
            c4.metric("AI Reliability", "Syncing...")

        #Trend chart
        st.divider()
        st.subheader("📈 Main Energy Trend")
        fig_trend = px.line(filtered_actuals, x='timestamp', y='price_eur_mwh', 
                           template="plotly_dark", color_discrete_sequence=['#00f2ff'])
        if not preds.empty:
            future_preds = preds[preds['timestamp'] > filtered_actuals['timestamp'].max()]
            fig_trend.add_trace(go.Scatter(x=future_preds['timestamp'], y=future_preds['predicted_price'], 
                                         name='Future AI Forecast', line=dict(color='magenta', dash='dash')))
        st.plotly_chart(fig_trend, use_container_width=True)

        #Acutal vs Predicted
        st.divider()
        st.subheader("🎯 Model Performance: Predicted vs. Real (Historical)")
        if not merged.empty:
            fig_acc = go.Figure()
            fig_acc.add_trace(go.Scatter(x=merged['timestamp'], y=merged['price_eur_mwh'], name='Real Price', line=dict(color='#00f2ff')))
            fig_acc.add_trace(go.Scatter(x=merged['timestamp'], y=merged['predicted_price'], name='AI Guess', line=dict(color='magenta', dash='dot')))
            fig_acc.update_layout(template="plotly_dark", xaxis_title="Time", yaxis_title="Price (€)")
            st.plotly_chart(fig_acc, use_container_width=True)
        else:
            st.info("The AI comparison chart will populate once your actual data catches up to your stored predictions.")

        #Core Analysis
        st.divider()
        col_l, col_r = st.columns(2)
        with col_l:
            st.subheader("🌀 Hierarchy (Sunburst)")
            fig_sun = px.sunburst(filtered_actuals, path=['Year', 'Month', 'Status_Zone'], values='Hours',
                                 color='Status_Zone', color_discrete_map={'🔴 Expensive':'#ff4b4b', '🟡 Normal':'#ffa500', '🟢 Cheap':'#00d400'})
            st.plotly_chart(fig_sun, use_container_width=True)
        with col_r:
            st.subheader("🌳 Distribution (Treemap)")
            fig_tree = px.treemap(filtered_actuals, path=['Status_Zone'], values='Hours',
                                 color='Status_Zone', color_discrete_map={'🔴 Expensive':'#ff4b4b', '🟡 Normal':'#ffa500', '🟢 Cheap':'#00d400'})
            st.plotly_chart(fig_tree, use_container_width=True)

        #Heatamp
        st.divider()
        st.subheader("🔥 Price Intensity Heatmap")
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        heatmap_data = filtered_actuals.pivot_table(index='Hour', columns='Day', values='price_eur_mwh', aggfunc='mean')
        heatmap_data = heatmap_data.reindex(columns=[d for d in day_order if d in heatmap_data.columns])
        fig_heat = px.imshow(heatmap_data, color_continuous_scale='RdYlGn_r', aspect="auto")
        st.plotly_chart(fig_heat, use_container_width=True)

        future_only = preds[preds['timestamp'] > actuals['timestamp'].max()]
        if not future_only.empty:
            best_row = future_only.sort_values('predicted_price').iloc[0]
            st.success(f"### 🤖 AI Strategy: Optimal window at **{best_row['timestamp'].strftime('%H:%M on %A')}** ({best_row['predicted_price']:,.0f} €)")
            

except Exception as e:
    st.error(f"Dashboard Error: {e}")