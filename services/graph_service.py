import pandas as pd
from config import MONTH_NAMES_SHORT

def generate_xy_graph_data(df_filtered, date_col, title="Event Timeline"):
    """Generate XY graph data for ANY filtered dataset"""
    
    if not date_col or date_col not in df_filtered.columns:
        return None
    
    df_valid = df_filtered[df_filtered[date_col].notna()].copy()
    
    if len(df_valid) == 0:
        return None
    
    df_valid['date_only'] = df_valid[date_col].dt.date
    daily_counts = df_valid.groupby('date_only').size().reset_index(name='count')
    daily_counts['date_only'] = daily_counts['date_only'].astype(str)
    
    total = int(daily_counts['count'].sum())
    avg = round(daily_counts['count'].mean(), 2)
    max_val = int(daily_counts['count'].max())
    min_val = int(daily_counts['count'].min())
    
    # Determine trend
    if len(daily_counts) >= 3:
        counts = daily_counts['count'].values
        first_half_avg = counts[:len(counts)//2].mean()
        second_half_avg = counts[len(counts)//2:].mean()
        
        if second_half_avg > first_half_avg * 1.1:
            trend = "increasing"
        elif second_half_avg < first_half_avg * 0.9:
            trend = "decreasing"
        else:
            trend = "stable"
    else:
        trend = "limited data"
    
    return {
        "type": "xy_line",
        "title": title,
        "x_axis": "Date",
        "y_axis": "Event Count",
        "data": {
            "dates": daily_counts['date_only'].tolist(),
            "counts": daily_counts['count'].tolist()
        },
        "stats": {
            "total": total,
            "avg": avg,
            "max": max_val,
            "min": min_val,
            "trend": trend,
            "date_range": {
                "start": daily_counts['date_only'].iloc[0],
                "end": daily_counts['date_only'].iloc[-1]
            }
        }
    }


def generate_comparison_graph_data(df, key_columns, equipments, filters=None):
    """Generate multi-dataset graph for equipment comparison"""
    
    date_col = key_columns.get('date')
    identifier_col = key_columns.get('identifier')
    
    if not date_col or not identifier_col:
        print("  ⚠️ Missing date or identifier column")
        return None
    
    print(f"  🎨 Generating comparison graph for: {equipments}")
    print(f"  📋 Filters: {filters}")
    
    datasets = []
    all_dates_set = set()
    
    # Collect all dates
    for equipment in equipments:
        df_eq = df[df[identifier_col].astype(str).str.contains(equipment, case=False, na=False)]
        
        # Apply filters
        if filters:
            if filters.get('months') and date_col in df_eq.columns:
                df_eq = df_eq[df_eq[date_col].dt.month.isin(filters['months'])]
            
            if filters.get('date_range') and date_col in df_eq.columns:
                start_day, end_day = filters['date_range']
                df_eq = df_eq[(df_eq[date_col].dt.day >= start_day) & (df_eq[date_col].dt.day <= end_day)]
            
            if filters.get('date_range_full') and date_col in df_eq.columns:
                start_date, end_date = filters['date_range_full']
                df_eq = df_eq[
                    (df_eq[date_col].dt.date >= pd.to_datetime(start_date).date()) &
                    (df_eq[date_col].dt.date <= pd.to_datetime(end_date).date())
                ]
        
        df_valid = df_eq[df_eq[date_col].notna()].copy()
        if len(df_valid) > 0:
            df_valid['date_only'] = df_valid[date_col].dt.date
            all_dates_set.update(df_valid['date_only'].unique())
    
    all_dates = sorted(list(all_dates_set))
    
    if len(all_dates) == 0:
        print("  ⚠️ No dates found after filtering")
        return None
    
    print(f"  📅 Date range: {all_dates[0]} to {all_dates[-1]} ({len(all_dates)} days)")
    
    # Generate dataset for each equipment
    for equipment in equipments:
        df_eq = df[df[identifier_col].astype(str).str.contains(equipment, case=False, na=False)]
        
        # Apply same filters
        if filters:
            if filters.get('months') and date_col in df_eq.columns:
                df_eq = df_eq[df_eq[date_col].dt.month.isin(filters['months'])]
            if filters.get('date_range') and date_col in df_eq.columns:
                start_day, end_day = filters['date_range']
                df_eq = df_eq[(df_eq[date_col].dt.day >= start_day) & (df_eq[date_col].dt.day <= end_day)]
            if filters.get('date_range_full') and date_col in df_eq.columns:
                start_date, end_date = filters['date_range_full']
                df_eq = df_eq[
                    (df_eq[date_col].dt.date >= pd.to_datetime(start_date).date()) &
                    (df_eq[date_col].dt.date <= pd.to_datetime(end_date).date())
                ]
        
        df_valid = df_eq[df_eq[date_col].notna()].copy()
        
        if len(df_valid) > 0:
            df_valid['date_only'] = df_valid[date_col].dt.date
            daily_counts = df_valid.groupby('date_only').size()
            
            counts = [int(daily_counts.get(date, 0)) for date in all_dates]
            
            datasets.append({
                "label": equipment,
                "dates": [str(d) for d in all_dates],
                "counts": counts,
                "total": int(daily_counts.sum())
            })
            
            print(f"    ✅ {equipment}: {daily_counts.sum()} total events")
    
    if len(datasets) == 0:
        print("  ⚠️ No datasets generated")
        return None
    
    # Build title
    title_parts = ["Comparison"]
    if filters:
        if filters.get('months'):
            months_str = ', '.join([MONTH_NAMES_SHORT.get(m, str(m)) for m in filters['months']])
            title_parts.append(months_str)
        
        if filters.get('date_range'):
            start_day, end_day = filters['date_range']
            title_parts.append(f"({start_day}-{end_day})")
    
    final_title = f"{' '.join(title_parts)}: {' vs '.join(equipments)}"
    
    return {
        "type": "comparison",
        "title": final_title,
        "datasets": datasets
    }


def generate_area_comparison_graph(df, key_columns, areas, filters=None):
    """Generate multi-dataset graph for area comparison"""
    
    date_col = key_columns.get('date')
    area_col = key_columns.get('area')
    
    if not date_col or not area_col:
        return None
    
    datasets = []
    all_dates_set = set()
    
    for area in areas:
        df_area = df[df[area_col] == area]
        
        if filters:
            if filters.get('months') and date_col in df_area.columns:
                df_area = df_area[df_area[date_col].dt.month.isin(filters['months'])]
            
            if filters.get('date_range') and date_col in df_area.columns:
                start_day, end_day = filters['date_range']
                df_area = df_area[(df_area[date_col].dt.day >= start_day) & (df_area[date_col].dt.day <= end_day)]
        
        df_valid = df_area[df_area[date_col].notna()].copy()
        if len(df_valid) > 0:
            df_valid['date_only'] = df_valid[date_col].dt.date
            all_dates_set.update(df_valid['date_only'].unique())
    
    all_dates = sorted(list(all_dates_set))
    if len(all_dates) == 0:
        return None
    
    for area in areas:
        df_area = df[df[area_col] == area]
        
        if filters:
            if filters.get('months') and date_col in df_area.columns:
                df_area = df_area[df_area[date_col].dt.month.isin(filters['months'])]
            if filters.get('date_range') and date_col in df_area.columns:
                start_day, end_day = filters['date_range']
                df_area = df_area[(df_area[date_col].dt.day >= start_day) & (df_area[date_col].dt.day <= end_day)]
        
        df_valid = df_area[df_area[date_col].notna()].copy()
        
        if len(df_valid) > 0:
            df_valid['date_only'] = df_valid[date_col].dt.date
            daily_counts = df_valid.groupby('date_only').size()
            counts = [int(daily_counts.get(date, 0)) for date in all_dates]
            
            datasets.append({
                "label": area,
                "dates": [str(d) for d in all_dates],
                "counts": counts,
                "total": int(daily_counts.sum())
            })
    
    if len(datasets) == 0:
        return None
    
    return {
        "type": "comparison",
        "title": f"Area Comparison: {' vs '.join(areas)}",
        "datasets": datasets
    }