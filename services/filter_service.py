"""
Filter Service for extracting and applying filters from queries
"""
import re
import pandas as pd
from datetime import datetime
from utils.helpers import parse_month_from_query, parse_date_range_from_query, extract_equipment_codes

class FilterService:
    """Handles filter extraction and application"""
    
    def __init__(self, df, key_columns):
        self.df = df
        self.key_columns = key_columns
    
    def extract_filters_from_query(self, query):
        """Extract filters from query - ENHANCED for all scenarios"""
        
        q = query.lower()
        filters = {}
        df_filtered = self.df.copy()
        filter_descriptions = []
        
        date_col = self.key_columns['date']
        
        # Month filters
        found_months = parse_month_from_query(query)
        
        if found_months and date_col:
            df_filtered = df_filtered[df_filtered[date_col].dt.month.isin(found_months)]
            filters['months'] = found_months
            
            from utils.helpers import get_month_name
            if len(found_months) == 1:
                filter_descriptions.append(f"month {get_month_name(found_months[0])}")
            else:
                month_strs = [get_month_name(m) for m in sorted(found_months)]
                filter_descriptions.append(f"months {' and '.join(month_strs)}")
        
        # Date range filters
        date_info = parse_date_range_from_query(query)
        
        if date_info and date_col and date_col in df_filtered.columns:
            if date_info['type'] == 'cross_month':
                # Cross-month range
                year = df_filtered[date_col].dt.year.mode()[0] if len(df_filtered) > 0 else datetime.now().year
                
                try:
                    start_date = datetime(year, date_info['start_month'], date_info['start_day']).date()
                    end_date = datetime(year, date_info['end_month'], date_info['end_day']).date()
                    
                    df_filtered = df_filtered[
                        (df_filtered[date_col].dt.date >= start_date) & 
                        (df_filtered[date_col].dt.date <= end_date)
                    ]
                    filters['date_range_full'] = (str(start_date), str(end_date))
                    
                    from utils.helpers import get_month_name
                    filter_descriptions.append(
                        f"{date_info['start_day']} {get_month_name(date_info['start_month'])} - " +
                        f"{date_info['end_day']} {get_month_name(date_info['end_month'])}"
                    )
                except:
                    pass
            
            elif date_info['type'] == 'same_month':
                # Same month range
                df_filtered = df_filtered[
                    (df_filtered[date_col].dt.day >= date_info['start_day']) & 
                    (df_filtered[date_col].dt.day <= date_info['end_day'])
                ]
                filters['date_range'] = (date_info['start_day'], date_info['end_day'])
                filter_descriptions.append(f"date {date_info['start_day']}-{date_info['end_day']}")
            
            elif date_info['type'] == 'single_day':
                # Single day
                df_filtered = df_filtered[df_filtered[date_col].dt.day == date_info['day']]
                filters['day'] = date_info['day']
                filter_descriptions.append(f"date {date_info['day']}")
        
        # Equipment/Identifier filter
        found_identifiers = extract_equipment_codes(query)
        
        if found_identifiers and self.key_columns.get('identifier'):
            id_col = self.key_columns['identifier']
            if id_col in df_filtered.columns:
                # Check if comparison query
                comparison_keywords = ['bandingkan', 'vs', 'versus', 'dibanding', 'compare', 'perbandingan', 'dan']
                is_comparison = any(kw in q for kw in comparison_keywords) and len(found_identifiers) >= 2
                
                if is_comparison:
                    # Don't filter - let comparison handler handle it
                    filters['comparison_identifiers'] = found_identifiers
                elif len(found_identifiers) == 1:
                    # Single equipment - apply filter
                    identifier = found_identifiers[0]
                    df_filtered = df_filtered[
                        df_filtered[id_col].astype(str).str.contains(identifier, case=False, na=False)
                    ]
                    filters['identifier'] = identifier
                    filter_descriptions.append(f"equipment {identifier}")
                else:
                    # Multiple equipment - filter to first
                    identifier = found_identifiers[0]
                    df_filtered = df_filtered[
                        df_filtered[id_col].astype(str).str.contains(identifier, case=False, na=False)
                    ]
                    filters['identifier'] = identifier
                    filter_descriptions.append(f"equipment {identifier}")
        
        # Value filters (category, area, status, severity)
        for role, col_name in self.key_columns.items():
            if not col_name or col_name not in self.df.columns:
                continue
            
            if role in ['category', 'area', 'status', 'severity'] and role not in filters:
                unique_values = self.df[col_name].dropna().unique()
                
                for value in unique_values:
                    value_str = str(value).lower()
                    
                    # Skip short values unless exact match
                    if len(value_str) < 4:
                        if value_str == q or f" {value_str} " in f" {q} ":
                            df_filtered = df_filtered[df_filtered[col_name] == value]
                            filters[col_name] = value
                            filter_descriptions.append(f"{role}: {value}")
                            break
                    else:
                        # Check word match with boundaries
                        pattern = r'\b' + re.escape(value_str) + r'\b'
                        if re.search(pattern, q):
                            df_filtered = df_filtered[df_filtered[col_name] == value]
                            filters[col_name] = value
                            filter_descriptions.append(f"{role}: {value}")
                            break
        
        return df_filtered, filters, filter_descriptions
    
    def apply_time_filters(self, df_input, filters):
        """Apply time-based filters to dataframe"""
        
        df_result = df_input.copy()
        date_col = self.key_columns.get('date')
        
        if not date_col or date_col not in df_result.columns:
            return df_result
        
        # Apply month filter
        if filters.get('months'):
            df_result = df_result[df_result[date_col].dt.month.isin(filters['months'])]
        
        # Apply date range filter
        if filters.get('date_range'):
            start_day, end_day = filters['date_range']
            df_result = df_result[
                (df_result[date_col].dt.day >= start_day) & 
                (df_result[date_col].dt.day <= end_day)
            ]
        
        # Apply full date range filter
        if filters.get('date_range_full'):
            start_date, end_date = filters['date_range_full']
            df_result = df_result[
                (df_result[date_col].dt.date >= pd.to_datetime(start_date).date()) &
                (df_result[date_col].dt.date <= pd.to_datetime(end_date).date())
            ]
        
        # Apply single day filter
        if filters.get('day'):
            df_result = df_result[df_result[date_col].dt.day == filters['day']]
        
        return df_result
    
    def build_context(self, query, df_filtered, filters, filter_descriptions, lang='en'):
        """Build crystal clear context for LLM"""
        
        context_parts = []
        
        # Internal rule (same for both languages)
        context_parts.append("🔴 INTERNAL RULE - DON'T EXPLAIN TO USER:")
        context_parts.append("• 1 row = 1 event (for your understanding only)")
        context_parts.append("• Count from number of rows")
        context_parts.append("• User doesn't need to know this detail")
        context_parts.append("")
        
        # Dataset overview
        context_parts.append("📊 DATASET INFO:")
        context_parts.append(f"Total events in system: **{len(self.df):,} events**")
        
        # Column info with hierarchy
        col_info = []
        if self.key_columns['identifier']:
            col_info.append(f"• Level 1 - `{self.key_columns['identifier']}`: Equipment code")
        if self.key_columns.get('equipment_name'):
            col_info.append(f"• Level 2 - `{self.key_columns['equipment_name']}`: Specific equipment name")
        if self.key_columns.get('pi_tag'):
            col_info.append(f"• Level 3 - `{self.key_columns['pi_tag']}`: PI sensor tags")
        if self.key_columns['category']:
            col_info.append(f"• `{self.key_columns['category']}`: Event category/type")
        if self.key_columns['area']:
            col_info.append(f"• `{self.key_columns['area']}`: Plant area")
        
        context_parts.append("\n".join(col_info))
        
        # Filter results
        context_parts.append(f"\n🔍 FILTER RESULTS:")
        if filter_descriptions:
            context_parts.append(f"Applied filters: {', '.join(filter_descriptions)}")
        else:
            context_parts.append("No filters (all data)")
        
        context_parts.append(f"\n🎯 **MATCHED: {len(df_filtered):,} events**")
        
        if len(df_filtered) > 0:
            # DETECT IF USER WANTS ALL DATA
            show_all_keywords = ['all', 'semua', 'complete', 'lengkap', 'seluruh', 'daftar', 'list', 'apa saja']
            show_all = any(kw in query.lower() for kw in show_all_keywords)
            
            # If show_all, NO LIMIT. Otherwise limit to 20
            limit = None if show_all else 20
            
            if show_all:
                context_parts.append("\n⚠️ USER REQUESTED ALL/COMPLETE DATA - SHOW EVERYTHING!")
            
            context_parts.append("\n📊 BREAKDOWN:")
            
            # Add hierarchical breakdown
            self._add_hierarchical_breakdown(context_parts, df_filtered, limit, show_all)
        
        return "\n".join(context_parts)
    
    def _add_hierarchical_breakdown(self, context_parts, df_filtered, limit, show_all):
        """Add hierarchical breakdown to context"""
        
        # Level 1: Equipment
        identifier_col = self.key_columns.get('identifier')
        if identifier_col and identifier_col in df_filtered.columns:
            equipment_counts = df_filtered[identifier_col].value_counts()
            context_parts.append(f"\n**Level 1 - Equipment Codes ({len(equipment_counts)} unique):**")
            
            items_to_show = equipment_counts.items() if show_all else equipment_counts.head(limit).items()
            for i, (eq, count) in enumerate(items_to_show, 1):
                pct = (count / len(df_filtered) * 100)
                context_parts.append(f"  {i}. {eq}: {count:,} events ({pct:.1f}%)")
            
            if not show_all and len(equipment_counts) > limit:
                context_parts.append(f"  ... and {len(equipment_counts) - limit} more (use 'show all' to see complete list)")
        
        # Level 2: Equipment Names
        eq_name_col = self.key_columns.get('equipment_name')
        if eq_name_col and eq_name_col in df_filtered.columns:
            name_counts = df_filtered[eq_name_col].value_counts()
            if len(name_counts) > 0:
                context_parts.append(f"\n**Level 2 - Specific Equipment Names ({len(name_counts)} unique):**")
                items_to_show = name_counts.items() if show_all else name_counts.head(limit).items()
                for i, (name, count) in enumerate(items_to_show, 1):
                    pct = (count / len(df_filtered) * 100)
                    context_parts.append(f"  {i}. {name}: {count:,} events ({pct:.1f}%)")
                if not show_all and len(name_counts) > limit:
                    context_parts.append(f"  ... and {len(name_counts) - limit} more")
        
        # Level 3: PI Tags
        pi_tag_col = self.key_columns.get('pi_tag')
        if pi_tag_col and pi_tag_col in df_filtered.columns:
            tag_counts = df_filtered[pi_tag_col].value_counts()
            if len(tag_counts) > 0:
                context_parts.append(f"\n**Level 3 - PI Tag Sensors ({len(tag_counts)} unique):**")
                items_to_show = tag_counts.items() if show_all else tag_counts.head(limit).items()
                for i, (tag, count) in enumerate(items_to_show, 1):
                    pct = (count / len(df_filtered) * 100)
                    context_parts.append(f"  {i}. `{tag}`: {count:,} events ({pct:.1f}%)")
                if not show_all and len(tag_counts) > limit:
                    context_parts.append(f"  ... and {len(tag_counts) - limit} more")
        
        # Other dimensions  
        for role in ['category', 'area', 'status', 'severity']:
            col = self.key_columns.get(role)
            if col and col in df_filtered.columns:
                counts = df_filtered[col].value_counts()
                if len(counts) > 0:
                    context_parts.append(f"\n**{role.title()}:**")
                    items_to_show = counts.items() if show_all else counts.head(10).items()
                    for i, (val, count) in enumerate(items_to_show, 1):
                        pct = (count / len(df_filtered) * 100)
                        context_parts.append(f"  {i}. {val}: {count:,} ({pct:.1f}%)")