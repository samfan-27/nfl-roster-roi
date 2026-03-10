import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from components.data_utils import load_offense_roster, load_player_history
from utils.fmt import dollars_to_str

def build_epa_composition_chart(row):
    """Builds a simple bar chart showing the breakdown of a player's EPA."""
    categories = ['Passing EPA', 'Rushing EPA', 'Receiving EPA']
    values = [
        float(row.get('passing_epa', 0) or 0),
        float(row.get('rushing_epa', 0) or 0),
        float(row.get('receiving_epa', 0) or 0)
    ]
    
    plot_cats, plot_vals = [], []
    for c, v in zip(categories, values):
        if v != 0:
            plot_cats.append(c)
            plot_vals.append(v)
            
    if not plot_cats:
        return go.Figure().update_layout(title='No EPA components recorded')
        
    colors = ['#2ca02c' if v > 0 else '#d62728' for v in plot_vals]
            
    fig = go.Figure(data=[go.Bar(x=plot_cats, y=plot_vals, marker_color=colors)])
    fig.update_layout(
        title='EPA Composition',
        yaxis_title='Expected Points Added',
        margin=dict(l=20, r=20, t=40, b=20),
        height=300
    )
    return fig

def build_historical_chart(hist_df):
    """Builds a dual-axis chart: APY vs EPA over time."""
    hist_df = hist_df.sort_values('season')
    
    fig = make_subplots(specs=[[{'secondary_y': True}]])
    
    # Bar Chart for APY (Financial Cost)
    fig.add_trace(
        go.Bar(
            x=hist_df['season'].astype(str), 
            y=hist_df['yearly_cap_hit'], 
            name='APY ($M)', 
            marker_color='rgba(31, 119, 180, 0.4)'
        ),
        secondary_y=False,
    )
    
    fig.add_trace(
        go.Scatter(
            x=hist_df['season'].astype(str), 
            y=hist_df['total_epa'], 
            name='Total EPA', 
            mode='lines+markers', 
            line=dict(color='#2ca02c', width=3),
            marker=dict(size=8)
        ),
        secondary_y=True,
    )
    
    fig.update_layout(
        title_text='Historical Performance vs. Contract Value', 
        hovermode='x unified', 
        margin=dict(l=20, r=20, t=40, b=20), 
        height=350,
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
    )
    
    fig.update_yaxes(title_text="APY ($M)", secondary_y=False)
    fig.update_yaxes(title_text="Total EPA", secondary_y=True)
    return fig

def weighted_median(values, weights):
    v = pd.Series(values).astype(float)
    w = pd.Series(weights).astype(float).fillna(0)
    df = pd.concat([v, w], axis=1).dropna()
    df.columns = ['v', 'w']
    if df.empty:
        return float('nan')
    df = df.loc[df['w'] > 0].sort_values('v')
    if df.empty:
        return float(pd.Series(values).median())
    
    cumw = df['w'].cumsum()
    cutoff = df['w'].sum() / 2.0
    idx = cumw.searchsorted(cutoff)
    idx = int(min(idx, len(df)-1))
    return float(df.iloc[idx]['v'])

def weighted_percentile(value, values, weights):
    v = pd.Series(values).astype(float)
    w = pd.Series(weights).astype(float).fillna(0)
    df = pd.concat([v, w], axis=1).dropna()
    df.columns = ['v', 'w']
    if df.empty or df['w'].sum() == 0:
        if df.empty:
            return float('nan')
        less_equal = (df['v'] <= float(value)).sum()
        return float(less_equal) / len(df) * 100.0
        
    le_w = df.loc[df['v'] <= float(value), 'w'].sum()
    pct = float(le_w) / float(df['w'].sum()) * 100.0
    return pct

def render():
    st.title('Player Dossier')
    st.markdown('Micro-level breakdown of individual player contract efficiency.')
    
    season = st.selectbox('Season', [2025, 2024, 2023, 2022, 2021])
    df = load_offense_roster(season)
    
    if df.empty:
        st.warning('No roster data available.')
        return
    
    col1, col2 = st.columns(2)
    with col1:
        teams = sorted(df['team'].dropna().unique().tolist())
        selected_team = st.selectbox('Select Team', ['All'] + teams)
    
    with col2:
        if selected_team != 'All':
            player_pool = df[df['team'] == selected_team]
        else:
            player_pool = df
            
        player_names = sorted(player_pool['player_name'].dropna().unique().tolist())
        selected_player = st.selectbox('Select Player', player_names)

    if selected_player:
        row = df[df['player_name'] == selected_player].iloc[0]
        
        st.divider()
        
        st.subheader(f"{row['player_name']} | {row['position']} - {row['team']}")
        
        is_rookie = row.get("is_rookie_deal", False)
        if is_rookie:
            st.info('🟢 **Contract Status: Rookie Scale Deal (CBA Constrained)**')
        else:
            st.info('🔵 **Contract Status: Veteran / Open Market Deal**')
        
        m1, m2, m3, m4 = st.columns(4)
        m1.metric('APY (Cost)', dollars_to_str(row['yearly_cap_hit']))
        m2.metric('Total EPA', f"{row['total_epa']:.1f}")
        m3.metric('Snaps', f"{int(row['snaps'])}")
        
        cost_per_epa = row.get('cost_per_epa')
        if pd.isna(cost_per_epa) or cost_per_epa is None:
            cpe_display = 'Liability (Negative EPA)'
        else:
            cpe_display = f"${(cost_per_epa * 1_000_000):,.0f}"
            
        m4.metric('Cost per EPA', cpe_display)
        
        st.divider()
        
        col_chart, col_context = st.columns([3, 2])
        
        with col_chart:
            fig = build_epa_composition_chart(row)
            st.plotly_chart(fig, use_container_width=True)
            
        with col_context:
            st.markdown('### Positional Context')
            st.markdown('How this player compares to the rest of the league at their position?')
            
            MIN_SNAPS_FOR_CONTEXT = 200  
            pos_df = df[df['position'] == row['position']]
            pos_peers = pos_df[pos_df['snaps'].fillna(0) >= MIN_SNAPS_FOR_CONTEXT]

            if pos_peers.empty:
                pos_peers = pos_df[pos_df['sample_flag'] != 'low_sample']
            if pos_peers.empty:
                pos_peers = pos_df  
            
            n_peers = len(pos_peers)
            st.write(f'Peers considered: **{n_peers}** *(min snaps = {MIN_SNAPS_FOR_CONTEXT})*')
            
            method = st.radio('Comparison method', ['Unweighted Median', 'Weighted by Snaps'], index=1, horizontal=True)
            
            if method == 'Weighted by Snaps':
                median_epa = weighted_median(pos_peers['total_epa'], pos_peers['snaps'])
                median_cost = weighted_median(pos_peers['yearly_cap_hit'], pos_peers['snaps'])
                player_percentile = weighted_percentile(row['total_epa'], pos_peers['total_epa'], pos_peers['snaps'])
            else:
                median_epa = float(pos_peers['total_epa'].median()) if n_peers > 0 else float('nan')
                median_cost = float(pos_peers['yearly_cap_hit'].median()) if n_peers > 0 else float('nan')
                if n_peers > 0:
                    less_equal = pos_peers['total_epa'].fillna(0).le(row['total_epa']).sum()
                    player_percentile = float(less_equal) / float(n_peers) * 100.0
                else:
                    player_percentile = float('nan')
            
            median_epa_disp = median_epa if pd.notna(median_epa) else 0.0
            median_cost_disp = median_cost if pd.notna(median_cost) else 0.0

            st.write(f'**Positional Median EPA:** {median_epa_disp:.1f}')
            st.write(f'**Positional Median APY:** {dollars_to_str(median_cost_disp)}')
            
            if pd.notna(player_percentile):
                st.write(f'**Player Percentile (EPA):** {player_percentile:.1f}%')
            else:
                st.write('**Player Percentile:** N/A')
            
            st.write("")
            
            try:
                if player_percentile >= 75 and row['yearly_cap_hit'] < median_cost_disp:
                    st.success(' **Elite Value** (Top quartile production, below median cost)')
                elif player_percentile <= 25 and row['yearly_cap_hit'] > median_cost_disp:
                    st.error(' **Roster Liability** (Bottom quartile production, above median cost)')
                else:
                    st.info(' **Market Value** (Production aligns with cost)')
            except Exception:
                st.info(' **Market Value** (insufficient data for classification)')
        
        st.divider()
        
        st.markdown('### Historical Time-Series')
        hist_df = load_player_history(row['gsis_id'])
        
        if not hist_df.empty and len(hist_df) > 1:
            fig_hist = build_historical_chart(hist_df)
            st.plotly_chart(fig_hist, use_container_width=True)
        elif not hist_df.empty and len(hist_df) == 1:
            st.info(' Only one season of data is currently available for this player.')
        else:
            st.warning(' Could not load historical data.')
        