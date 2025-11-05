import os
import pandas as pd

def load_csv(path):
    """Load and preprocess CSV file"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ File {path} not found.")
    
    try:
        df = pd.read_csv(path, encoding="utf-8", low_memory=False)
    except:
        df = pd.read_csv(path, encoding="latin1", low_memory=False)

    df.columns = [c.strip() for c in df.columns]
    
    # Convert date columns
    for col in df.columns:
        if any(kw in col.lower() for kw in ["time", "date", "timestamp"]):
            df[col] = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)
    
    print(f"✅ Loaded: {len(df):,} rows × {len(df.columns)} cols")
    return df


def identify_key_columns(df):
    """Identify key columns with 3-level hierarchy support"""
    
    mapping = {
        'identifier': None,        # Level 1: Equipment code
        'equipment_name': None,    # Level 2: Specific name
        'pi_tag': None,           # Level 3: Sensor tag
        'category': None,
        'area': None,
        'status': None,
        'severity': None,
        'date': None,
        'description': None,
        'limit_type': None
    }
    
    # Level 1: Equipment identifier
    for col in ['Equipment', 'Asset', 'Equipment Code', 'Asset Code']:
        if col in df.columns:
            mapping['identifier'] = col
            break
    
    # Level 2: Equipment name
    for col in ['Equipment Name', 'Asset Name', 'Name', 'Equipment_Name', 'Specific Equipment']:
        if col in df.columns and col != mapping['identifier']:
            mapping['equipment_name'] = col
            break
    
    # Level 3: PI Tag
    for col in ['TagNamePI', 'PI Tag', 'Tag Name', 'PI_Tag', 'Tag', 'Sensor']:
        if col in df.columns:
            mapping['pi_tag'] = col
            break
    
    # Category
    for col in ['Asset Category', 'Type', 'Category', 'Event Type', 'Alarm Type']:
        if col in df.columns:
            mapping['category'] = col
            break
    
    # Area
    for col in ['Plant Area', 'Area Authority', 'Area', 'Location']:
        if col in df.columns:
            mapping['area'] = col
            break
    
    # Status
    for col in ['Status', 'Event Status', 'Alarm Status']:
        if col in df.columns:
            mapping['status'] = col
            break
    
    # Severity
    if 'Severity' in df.columns:
        mapping['severity'] = 'Severity'
    
    # Limit type (High/Low)
    for col in df.columns:
        if any(kw in col.lower() for kw in ['limit', 'alarm']) and col not in [mapping['category']]:
            sample_values = df[col].dropna().astype(str).str.lower().unique()
            if any('high' in str(v) or 'low' in str(v) for v in sample_values):
                mapping['limit_type'] = col
                break
    
    # Date
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            mapping['date'] = col
            break
    
    # Description
    for col in ['Description', 'TextMessage', 'Reason', 'Message', 'Details']:
        if col in df.columns:
            mapping['description'] = col
            break
    
    return mapping


def get_enhanced_profile(df, key_columns):
    """Build comprehensive data profile"""
    
    profile = {
        "total_rows": len(df),
        "total_events": len(df),
        "structure": {},
        "key_values": {}
    }
    
    # Date range
    if key_columns['date']:
        date_col = key_columns['date']
        valid_dates = df[date_col].dropna()
        if len(valid_dates) > 0:
            min_date = valid_dates.min()
            max_date = valid_dates.max()
            
            profile["date_range"] = {
                "start": str(min_date.date()),
                "end": str(max_date.date()),
                "total_days": (max_date - min_date).days + 1,
                "span_months": sorted(list(valid_dates.dt.month.unique()))
            }
            
            # Daily stats
            df_temp = df[df[date_col].notna()].copy()
            df_temp['date_only'] = df_temp[date_col].dt.date
            events_per_day = df_temp.groupby('date_only').size()
            
            profile["daily_stats"] = {
                "avg_events_per_day": round(events_per_day.mean(), 2),
                "max_events_in_day": int(events_per_day.max()),
                "min_events_in_day": int(events_per_day.min()),
                "total_active_days": len(events_per_day)
            }
    
    # Profile key columns
    for role, col_name in key_columns.items():
        if col_name and col_name in df.columns:
            profile["structure"][role] = col_name
            
            if role in ['identifier', 'category', 'area', 'status', 'severity']:
                value_counts = df[col_name].value_counts()
                total = len(df)
                
                top_values = {}
                for val, count in value_counts.head(20).items():
                    top_values[str(val)] = {
                        "count": int(count),
                        "percentage": round(count / total * 100, 2)
                    }
                
                profile["key_values"][role] = {
                    "total_unique": len(value_counts),
                    "top_values": top_values
                }
    
    return profile