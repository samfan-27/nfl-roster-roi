import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

def build_steal_scatter(df: pd.DataFrame, x_col: str = 'yearly_cap_hit', y_col: str = 'total_epa', log_x: bool = False):
    """
    Build a Plotly scatter for steals.
    """
    if df.empty:
        return px.scatter(title='No data available for these filters')

    df = df.copy()
    
    if x_col == 'yearly_cap_hit':
        df['cap_dollars'] = df['yearly_cap_hit'].astype(float) * 1_000_000
        x = 'cap_dollars'
        x_label = 'APY ($)'
    else:
        x = x_col
        x_label = x_col

    df['cost_per_epa_dollars'] = df['cost_per_epa'].astype(float) * 1_000_000

    df['plot_size'] = df['snaps'].fillna(0).astype(int).clip(lower=10)

    fig = px.scatter(
        df,
        x=x,
        y=y_col,
        color='position',
        size='plot_size', 
        hover_data={
            x: True,
            y_col: ':.2f',
            'player_name': True,
            'team': True,
            'yearly_cap_hit': True,
            'cost_per_epa': False,
            'cost_per_epa_dollars': ':$,.0f',
            'snaps': True,
            'sample_flag': True,
            'plot_size': False
        },
        labels={x: x_label, y_col: 'Total EPA', 'cost_per_epa_dollars': 'Cost per EPA'}
    )
    
    fig.update_traces(
        marker=dict(
            opacity=0.7,
            line=dict(width=0.5, color='white')
        )
    )

    # medians for quadrants
    median_x = df[x].median()
    median_y = df[y_col].median()
    
    fig.add_shape(type='line', x0=median_x, x1=median_x, y0=df[y_col].min(), y1=df[y_col].max(),
                  line=dict(dash='dash', color='gray', width=1))
    fig.add_shape(type='line', x0=df[x].min(), x1=df[x].max(), y0=median_y, y1=median_y,
                  line=dict(dash='dash', color='gray', width=1))

    # Quadrant annotations using relative paper coordinates
    fig.add_annotation(x=0.02, y=0.98, xref='paper', yref='paper', 
                       text='High Value / Bargain', showarrow=False, font=dict(color='green'))
    fig.add_annotation(x=0.98, y=0.02, xref='paper', yref='paper', 
                       text='Overpaid / Liability', showarrow=False, font=dict(color='red'))

    if log_x:
        fig.update_xaxes(type="log")

    fig.update_layout(legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1))
    return fig

def build_efficiency_scatter(df: pd.DataFrame):
    """
    Builds a Volume (Snaps) vs. Efficiency (EPA/Snap) scatter plot.
    Designed to be used on position-specific filtered data.
    """
    if df.empty:
        return px.scatter(title='No data available for these filters')

    df = df.copy()
    
    if 'yearly_cap_hit' in df.columns:
        df['cap_dollars'] = df['yearly_cap_hit'].astype(float) * 1_000_000

    df['snaps'] = df['snaps'].fillna(0).astype(int)
    df['epa_per_snap'] = pd.to_numeric(df['epa_per_snap'], errors='coerce').fillna(0)

    fig = px.scatter(
        df,
        x='snaps',
        y='epa_per_snap',
        color='yearly_cap_hit',
        color_continuous_scale='Viridis', 
        hover_data={
            'snaps': True,
            'epa_per_snap': ':.3f',
            'player_name': True,
            'team': True,
            'yearly_cap_hit': False,
            'cap_dollars': ':$,.0f',
            'total_epa': ':.2f'
        },
        labels={
            'snaps': 'Volume (Total Snaps)', 
            'epa_per_snap': 'Efficiency (EPA per Snap)',
            'yearly_cap_hit': 'APY ($M)',
            'cap_dollars': 'APY'
        }
    )
    
    median_x = df['snaps'].median()
    median_y = df['epa_per_snap'].median()
    
    fig.add_shape(type="line", x0=median_x, x1=median_x, y0=df['epa_per_snap'].min(), y1=df['epa_per_snap'].max(),
                  line=dict(dash="dash", color="gray", width=1))
    fig.add_shape(type="line", x0=df['snaps'].min(), x1=df['snaps'].max(), y0=median_y, y1=median_y,
                  line=dict(dash='dash', color='gray', width=1))

    fig.add_annotation(x=0.98, y=0.98, xref='paper', yref='paper', 
                       text='Elite Workhorses', showarrow=False, font=dict(color='green'))
    fig.add_annotation(x=0.02, y=0.98, xref='paper', yref='paper', 
                       text='Gadget / High-Efficiency', showarrow=False)
    fig.add_annotation(x=0.98, y=0.02, xref='paper', yref='paper', 
                       text='Inefficient Compilers', showarrow=False, font=dict(color='red'))

    fig.update_traces(marker=dict(size=10, opacity=0.8, line=dict(width=0.5, color='white')))
    fig.update_layout(coloraxis_colorbar=dict(title='APY ($M)'))

    return fig

def build_team_heatmap(df: pd.DataFrame):
    if df.empty:
        return px.treemap(title='No team data available')
        
    df = df.copy()
    df['team_total_cap_m'] = df['team_total_cap_dollars'].astype(float) / 1_000_000.0
    
    fig = px.treemap(
        df, 
        path=[px.Constant('NFL'), 'team'], 
        values='team_total_cap_dollars', 
        color='team_total_epa', 
        hover_data=['team_total_cap_m'],
        color_continuous_scale='RdYlGn',
        color_continuous_midpoint=0
    )
    return fig
