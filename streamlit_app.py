
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO

def create_cvg_identifier(row):
    """Create CVG identifier using full names."""
    return f"{row['Commodity']}_{row['Variety']}_{row['Grade']}_{row['Market']}"

# Add this function near the top of the file, after imports
def parse_mixed_dates(date):
    try:
        return pd.to_datetime(date, format="%Y-%m-%d")  # Try ISO format first
    except ValueError:
        try:
            return pd.to_datetime(date, format="%d-%m-%Y")  # Try day-first format
        except ValueError:
            return pd.NaT  # If neither works, return NaT

def select_best_cvgs(df, n=3, preferred_markets=None):
    """Select best CVG combinations based on data completeness."""
    df['Date'] = df['Date'].apply(parse_mixed_dates)
    unique_commodities = df['Commodity'].unique()
    best_cvgs = []
    
    for commodity in unique_commodities:
        commodity_df = df[df['Commodity'] == commodity]
        cvg_stats = commodity_df.groupby('CVG').agg({
            'Date': ['min', 'max', 'count'],
            'Modal': 'count'
        }).reset_index()
        
        cvg_stats.columns = ['CVG', 'start_date', 'end_date', 'total_count', 'non_null_count']
        cvg_stats['start_date'] = pd.to_datetime(cvg_stats['start_date'])
        cvg_stats['end_date'] = pd.to_datetime(cvg_stats['end_date'])
        cvg_stats['span'] = (cvg_stats['end_date'] - cvg_stats['start_date']).dt.days
        cvg_stats['years_span'] = cvg_stats['span'] / 365.25
        cvg_stats['records_per_year'] = cvg_stats['non_null_count'] / cvg_stats['years_span']
        
        five_years_ago = pd.Timestamp.now() - pd.DateOffset(years=5)
        cvg_stats = cvg_stats[
            (cvg_stats['end_date'] >= five_years_ago) & 
            (cvg_stats['non_null_count'] >= 500)
        ]
        
        cvg_stats = cvg_stats.sort_values(['non_null_count', 'records_per_year'], ascending=[False, False])
        best_cvgs.extend(cvg_stats.head(n)['CVG'].tolist())
    
    return best_cvgs

def process_data(commodity_df, weather_df, selected_cvg):
    # Filter commodity data for selected CVG
    df = commodity_df[commodity_df['CVG'] == selected_cvg][['Date', 'Modal', 'Arrivals']]
    
    # Handle mixed date formats
    df['Date'] = df['Date'].apply(parse_mixed_dates)
    
    # Process weather data - Fix date conversion
    weather_df['Date'] = pd.to_datetime({
        'year': weather_df['YEAR'],
        'month': weather_df['MO'],
        'day': weather_df['DY']
    })
    weather_df.drop(['YEAR', 'MO', 'DY'], axis=1, inplace=True)
    weather_df.replace(-999, pd.NA, inplace=True)
    
    # Create weekly and monthly data
    df_weekly = df.resample('W-SUN', on='Date').agg({
        'Modal': 'mean',
        'Arrivals': 'sum'
    }).reset_index()
    
    df_monthly = df.resample('MS', on='Date').agg({
        'Modal': 'mean',
        'Arrivals': 'sum'
    }).reset_index()
    
    # Process weather data
    weather_weekly = weather_df.resample('W-SUN', on='Date').mean().reset_index()
    weather_monthly = weather_df.resample('MS', on='Date').mean().reset_index()
    
    # Merge data
    weekly_final = pd.merge(df_weekly, weather_weekly, on='Date', how='left')
    monthly_final = pd.merge(df_monthly, weather_monthly, on='Date', how='left')
    
    return weekly_final, monthly_final

def main():
    # Initialize session state for processed data and file data
    if 'weekly_data' not in st.session_state:
        st.session_state.weekly_data = None
    if 'monthly_data' not in st.session_state:
        st.session_state.monthly_data = None
    if 'processed' not in st.session_state:
        st.session_state.processed = False
    if 'commodity_data' not in st.session_state:
        st.session_state.commodity_data = None
    if 'weather_data' not in st.session_state:
        st.session_state.weather_data = None
    if 'selected_cvg' not in st.session_state:
        st.session_state.selected_cvg = None

    st.title("Agricultural Data Processing Tool")
    
    # File upload with session state persistence
    commodity_file = st.file_uploader("Upload Commodity Data (CSV)", type=['csv'])
    weather_file = st.file_uploader("Upload Weather Data (CSV)", type=['csv'])
    
    if commodity_file and weather_file:
        try:
            # Load data only if not already in session state
            if st.session_state.commodity_data is None:
                st.session_state.commodity_data = pd.read_csv(commodity_file)
                st.session_state.commodity_data['CVG'] = st.session_state.commodity_data.apply(create_cvg_identifier, axis=1)
            
            if st.session_state.weather_data is None:
                st.session_state.weather_data = pd.read_csv(weather_file)
            
            # Get best CVGs
            best_cvgs = select_best_cvgs(st.session_state.commodity_data)
            
            # CVG selection with session state
            selected_cvg = st.selectbox(
                "Select CVG Combination",
                options=best_cvgs,
                help="Select the Commodity-Variety-Grade combination to analyze"
            )
            
            # Process data if button clicked or CVG changed
            if st.button("Process Data") or (st.session_state.processed and st.session_state.selected_cvg != selected_cvg):
                st.session_state.selected_cvg = selected_cvg
                weekly_final, monthly_final = process_data(
                    st.session_state.commodity_data,
                    st.session_state.weather_data,
                    selected_cvg
                )
                st.session_state.weekly_data = weekly_final
                st.session_state.monthly_data = monthly_final
                st.session_state.processed = True
            
            # Show results if data is processed
            if st.session_state.processed:
                # Create download buttons
                st.subheader("Download Processed Data")
                
                col1, col2 = st.columns(2)
                with col1:
                    weekly_buffer = BytesIO()
                    st.session_state.weekly_data.to_csv(weekly_buffer, index=False)
                    weekly_buffer.seek(0)
                    st.download_button(
                        label="Download Weekly Data",
                        data=weekly_buffer,
                        file_name=f"{selected_cvg}_weekly_data.csv",
                        mime="text/csv",
                        key="weekly_download"
                    )
                    
                with col2:
                    monthly_buffer = BytesIO()
                    st.session_state.monthly_data.to_csv(monthly_buffer, index=False)
                    monthly_buffer.seek(0)
                    st.download_button(
                        label="Download Monthly Data",
                        data=monthly_buffer,
                        file_name=f"{selected_cvg}_monthly_data.csv",
                        mime="text/csv",
                        key="monthly_download"
                    )
                
                # Display preview
                st.subheader("Data Preview")
                col1, col2 = st.columns(2)
                with col1:
                    st.write("Weekly Data Preview")
                    st.dataframe(st.session_state.weekly_data.tail())
                with col2:
                    st.write("Monthly Data Preview")
                    st.dataframe(st.session_state.monthly_data.tail())
                
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
            st.info("Please ensure your data files are in the correct format.")

if __name__ == "__main__":
    main()
