"""
Data loading and profiling
"""
import os
import pandas as pd
from typing import Dict, Any

class DataLoader:
    """Handles CSV loading, column mapping, and data profiling"""
    
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.df = None
        self.key_columns = {}
        self.profile = {}
    
    def load_csv(self):
        """Load CSV with encoding fallback"""
        if not os.path.exists(self.csv_path):
            raise FileNotFoundError(f"❌ File {self.csv_path} not found.")
        
        try:
            self.df = pd.read_csv(self.csv_path, encoding="utf-8", low_memory=False)
        except:
            self.df = pd.read_csv(self.csv_path, encoding="latin1", low_memory=False)
        
        # Clean column names
        self.df.columns = [c.strip() for c in self.df.columns]
        
        # Parse date columns
        for col in self.df.columns:
            if any(kw in col.lower() for kw in ["time", "date", "timestamp"]):
                self.df[col] = pd.to_datetime(self.df[col], errors="coerce", infer_datetime_format=True)
        
        print(f"✅ Loaded: {len(self.df):,} rows × {len(self.df.columns)} cols")
        
        return self.df
    
    def identify_key_columns(self) -> Dict[str, str]:
        """Identify key columns with 3-level hierarchy support"""
        
        mapping = {
            'identifier': None,        # Level 1: Equipment code (GB-651)
            'equipment_name': None,    # Level 2: Specific name (AT-651)
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
            if col in self.df.columns:
                mapping['identifier'] = col
                break
        
        # Level 2: Equipment name (more specific)
        for col in ['Equipment Name', 'Asset Name', 'Name', 'Equipment_Name', 'Specific Equipment']:
            if col in self.df.columns and col != mapping['identifier']:
                mapping['equipment_name'] = col
                break
        
        # Level 3: PI Tag
        for col in ['TagNamePI', 'PI Tag', 'Tag Name', 'PI_Tag', 'Tag', 'Sensor']:
            if col in self.df.columns:
                mapping['pi_tag'] = col
                break
        
        # Category
        for col in ['Asset Category', 'Type', 'Category', 'Event Type', 'Alarm Type']:
            if col in self.df.columns:
                mapping['category'] = col
                break
        
        # Area
        for col in ['Plant Area', 'Area Authority', 'Area', 'Location']:
            if col in self.df.columns:
                mapping['area'] = col
                break
        
        # Status
        for col in ['Status', 'Event Status', 'Alarm Status']:
            if col in self.df.columns:
                mapping['status'] = col
                break
        
        # Severity
        if 'Severity' in self.df.columns:
            mapping['severity'] = 'Severity'
        
        # Limit type (High/Low)
        for col in self.df.columns:
            if any(kw in col.lower() for kw in ['limit', 'alarm']) and col not in [mapping['category']]:
                sample_values = self.df[col].dropna().astype(str).str.lower().unique()
                if any('high' in str(v) or 'low' in str(v) for v in sample_values):
                    mapping['limit_type'] = col
                    break
        
        # Date
        for col in self.df.columns:
            if pd.api.types.is_datetime64_any_dtype(self.df[col]):
                mapping['date'] = col
                break
        
        # Description
        for col in ['Description', 'TextMessage', 'Reason', 'Message', 'Details']:
            if col in self.df.columns:
                mapping['description'] = col
                break
        
        self.key_columns = mapping
        return mapping
    
    def build_profile(self) -> Dict[str, Any]:
        """Build comprehensive data profile"""
        
        profile = {
            "total_rows": len(self.df),
            "total_events": len(self.df),
            "structure": {},
            "key_values": {}
        }
        
        # Date range
        if self.key_columns['date']:
            date_col = self.key_columns['date']
            valid_dates = self.df[date_col].dropna()
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
                df_temp = self.df[self.df[date_col].notna()].copy()
                df_temp['date_only'] = df_temp[date_col].dt.date
                events_per_day = df_temp.groupby('date_only').size()
                
                profile["daily_stats"] = {
                    "avg_events_per_day": round(events_per_day.mean(), 2),
                    "max_events_in_day": int(events_per_day.max()),
                    "min_events_in_day": int(events_per_day.min()),
                    "total_active_days": len(events_per_day)
                }
        
        # Profile key columns
        for role, col_name in self.key_columns.items():
            if col_name and col_name in self.df.columns:
                profile["structure"][role] = col_name
                
                if role in ['identifier', 'category', 'area', 'status', 'severity']:
                    value_counts = self.df[col_name].value_counts()
                    total = len(self.df)
                    
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
        
        self.profile = profile
        return profile
    
    def get_data(self):
        """Get loaded dataframe"""
        return self.df
    
    def get_key_columns(self):
        """Get key columns mapping"""
        return self.key_columns
    
    def get_profile(self):
        """Get data profile"""
        return self.profile