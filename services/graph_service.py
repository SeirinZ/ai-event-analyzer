"""
Graph Service for generating visualization data
"""
import pandas as pd
import numpy as np

class GraphService:
    """Handles graph data generation"""
    
    def __init__(self, key_columns):
        self.key_columns = key_columns
    
    def generate_xy_graph_data(self, df_filtered, title="Event Timeline"):
        """Generate XY graph data for ANY filtered dataset"""
        
        date_col = self.key_columns.get('date')
        if not date_col or date_col not in df_filtered.columns:
            return None
        
        df_valid = df_filtered[df_filtered[date_col].notna()].copy()
        
        if len(df_valid) == 0:
            return None
        
        # Group by date
        df_valid['date_only'] = df_valid[date_col].dt.date
        daily_counts = df_valid.groupby('date_only').size().reset_index(name='count')
        daily_counts['date_only'] = daily_counts['date_only'].astype(str)
        
        # Calculate stats
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
                trend = "Increase"
            elif second_half_avg < first_half_avg * 0.9:
                trend = "Decrease"
            else:
                trend = "Stable"
        else:
            trend = "Limited data"
        
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
    
    def generate_comparison_graph_data(self, df, equipments, filters=None):
        """Generate multi-dataset graph for equipment comparison - SUPER FIXED"""
        
        date_col = self.key_columns.get('date')
        identifier_col = self.key_columns.get('identifier')
        
        if not date_col or not identifier_col:
            print("  ❌ Missing date or identifier column")
            return None
        
        print(f"  📊 Generating comparison graph for: {equipments}")
        print(f"  🔧 Filters: {filters}")
        
        # Step 1: Collect ALL dates across all equipment
        all_dates_set = set()
        equipment_data = {}
        
        for equipment in equipments:
            # Filter by equipment
            df_eq = df[df[identifier_col].astype(str).str.contains(equipment, case=False, na=False)]
            
            # Apply time filters
            df_eq = self._apply_filters(df_eq, filters, date_col)
            
            # Get valid dates
            df_valid = df_eq[df_eq[date_col].notna()].copy()
            
            if len(df_valid) > 0:
                df_valid['date_only'] = df_valid[date_col].dt.date
                all_dates_set.update(df_valid['date_only'].unique())
                equipment_data[equipment] = df_valid
                print(f"    ✓ {equipment}: {len(df_valid)} events")
            else:
                print(f"    ⚠️ {equipment}: No data after filtering")
                equipment_data[equipment] = None
        
        # Get sorted list of all dates
        all_dates = sorted(list(all_dates_set))
        
        if len(all_dates) == 0:
            print("  ❌ No dates found after filtering")
            return None
        
        print(f"  📅 Date range: {all_dates[0]} to {all_dates[-1]} ({len(all_dates)} days)")
        
        # Step 2: Build datasets with FILLED dates (0 for missing)
        datasets = []
        
        for equipment in equipments:
            df_valid = equipment_data.get(equipment)
            
            if df_valid is None or len(df_valid) == 0:
                # Equipment has NO data - fill all with 0
                counts = [0] * len(all_dates)
                total = 0
                print(f"    ⚠️ {equipment}: All zeros (no data)")
            else:
                # Count events per date
                daily_counts = df_valid.groupby('date_only').size()
                
                # CRITICAL: Fill missing dates with 0
                counts = []
                for date in all_dates:
                    count_value = daily_counts.get(date, 0)
                    # ENSURE IT'S A PLAIN PYTHON INT
                    if hasattr(count_value, 'item'):  # numpy type
                        count_value = int(count_value.item())
                    else:
                        count_value = int(count_value)
                    counts.append(count_value)
                
                total = int(daily_counts.sum())
                
                # DEBUG: Print actual counts
                print(f"    ✅ {equipment}: {total} total events")
                print(f"       Raw counts: {counts[:5]}... (showing first 5)")
                print(f"       Max count: {max(counts)}, Min: {min(counts)}")
            
            # CRITICAL: Ensure dates are strings and counts are plain ints
            datasets.append({
                "label": equipment,
                "dates": [str(d) for d in all_dates],
                "counts": counts,  # Already converted to plain ints above
                "total": total
            })
        
        # Final verification with actual sample
        print(f"  🔍 Final Verification:")
        for ds in datasets:
            print(f"    • {ds['label']}:")
            print(f"      - Total: {ds['total']}")
            print(f"      - Sample counts (first 5): {ds['counts'][:5]}")
            print(f"      - Type check: {type(ds['counts'][0])}")
            print(f"      - Max: {max(ds['counts'])}, Non-zero days: {len([c for c in ds['counts'] if c > 0])}")
        
        # Build title
        title_parts = ["Comparison"]
        if filters:
            if filters.get('months'):
                month_names = {1:'Jan',2:'Feb',3:'Mar',4:'Apr',5:'May',6:'Jun',
                              7:'Jul',8:'Aug',9:'Sep',10:'Oct',11:'Nov',12:'Dec'}
                months_str = ', '.join([month_names.get(m, str(m)) for m in filters['months']])
                title_parts.append(months_str)
            
            if filters.get('date_range'):
                start_day, end_day = filters['date_range']
                title_parts.append(f"({start_day}-{end_day})")
            
            if filters.get('date_range_full'):
                start_date, end_date = filters['date_range_full']
                title_parts.append(f"({start_date} to {end_date})")
        
        final_title = f"{' '.join(title_parts)}: {' vs '.join(equipments)}"
        print(f"  📊 Final graph title: {final_title}")
        
        return {
            "type": "comparison",
            "title": final_title,
            "datasets": datasets
        }
    
    def _apply_filters(self, df_eq, filters, date_col):
        """Apply filters to dataframe"""
        if not filters:
            return df_eq
        
        if filters.get('months') and date_col in df_eq.columns:
            df_eq = df_eq[df_eq[date_col].dt.month.isin(filters['months'])]
            print(f"      → After month filter: {len(df_eq)} rows")
        
        if filters.get('date_range') and date_col in df_eq.columns:
            start_day, end_day = filters['date_range']
            df_eq = df_eq[(df_eq[date_col].dt.day >= start_day) & (df_eq[date_col].dt.day <= end_day)]
            print(f"      → After date range filter: {len(df_eq)} rows")
        
        if filters.get('date_range_full') and date_col in df_eq.columns:
            start_date, end_date = filters['date_range_full']
            df_eq = df_eq[
                (df_eq[date_col].dt.date >= pd.to_datetime(start_date).date()) &
                (df_eq[date_col].dt.date <= pd.to_datetime(end_date).date())
            ]
            print(f"      → After full date range filter: {len(df_eq)} rows")
        
        return df_eq
    
    def should_generate_graph(self, query):
        """Check if query asks for graph"""
        graph_keywords = ['trend', 'grafik', 'chart', 'graph', 'pola', 'pattern', 'distribusi waktu', 'timeline', 'visualize', 'visualisasi']
        return any(kw in query.lower() for kw in graph_keywords)