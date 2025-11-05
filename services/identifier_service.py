import re
import pandas as pd
from config import MONTHS_MAP, MONTH_NAMES
from utils.translations import detect_language, t
from services.llm_service import call_llm
from services.filter_service import calculate_confidence
from services.graph_service import generate_xy_graph_data

def is_identifier_query(query):
    """Check if query contains equipment identifier"""
    patterns = [
        r'\b([A-Z]{2,}-\d+[A-Z]*)\b',
        r'\b([A-Z]{2,}\d+[A-Z]*)\b',
    ]
    return any(re.search(p, query.upper()) for p in patterns)


def search_by_identifier(query, df, key_columns):
    """Enhanced identifier search with LLM analysis"""
    
    patterns = [
        r'\b([A-Z]{2,}-\d+[A-Z]*)\b',
        r'\b([A-Z]{2,}\d+[A-Z]*)\b',
        r'\b([A-Z]{2}-\d+)\b',
    ]
    
    identifier = None
    for pattern in patterns:
        matches = re.findall(pattern, query.upper())
        if matches:
            identifier = matches[0]
            break
    
    if not identifier:
        return None, None, 0, None
    
    lang = detect_language(query)
    
    # Search columns
    search_cols = []
    if key_columns.get('identifier'):
        search_cols.append(key_columns['identifier'])
    for col in ['Equipment Name', 'TagNamePI', 'Asset Name']:
        if col in df.columns:
            search_cols.append(col)
    
    mask = pd.Series([False] * len(df))
    found_in_cols = []
    
    for col in search_cols:
        if col in df.columns:
            col_match = df[col].astype(str).str.contains(identifier, case=False, na=False)
            if col_match.any():
                mask = mask | col_match
                found_in_cols.append(col)
    
    df_found = df[mask]
    
    # Apply ALL time filters with cross-month support
    q = query.lower()
    date_col = key_columns.get('date')
    
    if date_col and date_col in df_found.columns:
        # Cross-month date range (28 august - 16 september)
        cross_month_pattern = r'(\d{1,2})\s+(january|jan|februari|feb|february|march|mar|maret|april|apr|may|mei|june|jun|juni|july|jul|juli|august|aug|agustus|ags|september|sept|sep|october|oct|oktober|okt|november|nov|december|dec|desember|des)\s*[-–to/vs]\s*(\d{1,2})\s+(january|jan|februari|feb|february|march|mar|maret|april|apr|may|mei|june|jun|juni|july|jul|juli|august|aug|agustus|ags|september|sept|sep|october|oct|oktober|okt|november|nov|december|dec|desember|des)'
        match = re.search(cross_month_pattern, q, re.IGNORECASE)
        
        applied_filter = False
        
        if match:
            start_day = int(match.group(1))
            start_month_str = match.group(2).lower()
            end_day = int(match.group(3))
            end_month_str = match.group(4).lower()
            
            start_month = MONTHS_MAP.get(start_month_str)
            end_month = MONTHS_MAP.get(end_month_str)
            
            if start_month and end_month:
                from datetime import datetime as dt_class
                year = df_found[date_col].dt.year.mode()[0] if len(df_found) > 0 else dt_class.now().year
                
                try:
                    start_date = dt_class(year, start_month, start_day).date()
                    end_date = dt_class(year, end_month, end_day).date()
                    
                    df_found = df_found[
                        (df_found[date_col].dt.date >= start_date) & 
                        (df_found[date_col].dt.date <= end_date)
                    ]
                    applied_filter = True
                    print(f"  ✓ Applied cross-month date range: {start_date} to {end_date}")
                except Exception as e:
                    print(f"  ⚠️ Error applying cross-month filter: {e}")
        
        # Single month filter
        if not applied_filter:
            for month_str, month_num in MONTHS_MAP.items():
                if month_str in q and len(month_str) > 3:
                    df_found = df_found[df_found[date_col].dt.month == month_num]
                    print(f"  ✓ Applied month filter: {month_str}")
                    applied_filter = True
                    break
        
        # Date range filter (same month)
        if not applied_filter:
            date_range_pattern = r'(date|tanggal)\s*(\d{1,2})\s*[-–to/sd]\s*(\d{1,2})'
            match = re.search(date_range_pattern, q)
            if match:
                start_day = int(match.group(2))
                end_day = int(match.group(3))
                df_found = df_found[(df_found[date_col].dt.day >= start_day) & (df_found[date_col].dt.day <= end_day)]
                print(f"  ✓ Applied date range: {start_day}-{end_day}")
                applied_filter = True
        
        # Week filter
        if not applied_filter:
            week_pattern = r'(week|minggu)\s*(\d{1,2})'
            match = re.search(week_pattern, q)
            if match:
                week_num = int(match.group(2))
                df_found = df_found[df_found[date_col].dt.isocalendar().week == week_num]
                print(f"  ✓ Applied week filter: week {week_num}")
    
    if len(df_found) == 0:
        return None, f"❌ No data found for `{identifier}` with those filters", 0, None
    
    # Build COMPREHENSIVE report
    explanation = f"# 🔍 Search Results: `{identifier}`\n\n"
    explanation += f"**Found {len(df_found):,} events** in columns: {', '.join([f'`{c}`' for c in found_in_cols])}\n\n"
    
    # Equipment Hierarchy Information
    explanation += f"## 🏗️ Equipment Hierarchy\n\n"
    
    # Level 1: Equipment
    if key_columns.get('identifier'):
        col = key_columns['identifier']
        unique_eq = df_found[col].dropna().unique()
        if len(unique_eq) <= 5:
            explanation += f"**Equipment Codes:** {', '.join([f'`{e}`' for e in unique_eq])}\n"
    
    # Level 2: Equipment Names
    if key_columns.get('equipment_name'):
        col = key_columns['equipment_name']
        if col in df_found.columns:
            unique_names = df_found[col].dropna().unique()
            if len(unique_names) > 0:
                explanation += f"\n**Specific Equipment Names ({len(unique_names)}):**\n"
                for name in list(unique_names)[:10]:
                    count = len(df_found[df_found[col] == name])
                    pct = (count / len(df_found) * 100)
                    explanation += f"  • {name}: {count} events ({pct:.1f}%)\n"
                if len(unique_names) > 10:
                    explanation += f"  • ... and {len(unique_names) - 10} more\n"
    
    # Level 3: PI Tags
    if key_columns.get('pi_tag'):
        col = key_columns['pi_tag']
        if col in df_found.columns:
            unique_tags = df_found[col].dropna().unique()
            if len(unique_tags) > 0:
                explanation += f"\n**PI Tag Sensors ({len(unique_tags)}):**\n"
                tag_counts = df_found[col].value_counts()
                for tag, count in list(tag_counts.items())[:10]:
                    pct = (count / len(df_found) * 100)
                    explanation += f"  • `{tag}`: {count} events ({pct:.1f}%)\n"
                if len(unique_tags) > 10:
                    explanation += f"  • ... and {len(unique_tags) - 10} more sensors\n"
    
    explanation += "\n"
    
    # Timeline with Graph
    graph_data = None
    
    if date_col and date_col in df_found.columns:
        df_valid = df_found[df_found[date_col].notna()].copy()
        if len(df_valid) > 0:
            min_date = df_valid[date_col].min()
            max_date = df_valid[date_col].max()
            days_span = (max_date - min_date).days + 1
            
            explanation += f"## 📅 Timeline\n\n"
            explanation += f"• **Period:** {min_date.date()} to {max_date.date()} ({days_span} days)\n"
            
            df_valid['date_only'] = df_valid[date_col].dt.date
            events_by_date = df_valid.groupby('date_only').size().sort_index()
            
            avg_per_day = events_by_date.mean()
            explanation += f"• **Average:** {avg_per_day:.1f} events/day\n"
            explanation += f"• **Range:** {events_by_date.min()}-{events_by_date.max()} events/day\n"
            
            # Generate graph
            graph_data = generate_xy_graph_data(df_found, date_col, f"Timeline {identifier}")
            
            if graph_data:
                explanation += f"• **Trend:** {graph_data['stats']['trend']}\n"
            
            # Top 5 busiest days
            top_days = events_by_date.sort_values(ascending=False).head(5)
            explanation += f"\n**Top 5 Busiest Days:**\n"
            for date, count in top_days.items():
                explanation += f"  • {date}: {count} events\n"
            explanation += "\n"
    
    # Event Details Breakdown
    explanation += f"## 📊 Event Breakdown\n\n"
    
    # Category/Alarm Type
    if key_columns.get('category'):
        col = key_columns['category']
        if col in df_found.columns:
            counts = df_found[col].value_counts()
            if len(counts) > 0:
                explanation += f"**Event Type/Category:**\n"
                for val, count in counts.items():
                    pct = (count / len(df_found) * 100)
                    explanation += f"  • {val}: {count} events ({pct:.1f}%)\n"
                explanation += "\n"
    
    # Limit Type (High/Low)
    if key_columns.get('limit_type'):
        col = key_columns['limit_type']
        if col in df_found.columns:
            counts = df_found[col].value_counts()
            if len(counts) > 0:
                explanation += f"**Limit/Alarm Type:**\n"
                for val, count in counts.items():
                    pct = (count / len(df_found) * 100)
                    explanation += f"  • {val}: {count} events ({pct:.1f}%)\n"
                explanation += "\n"
    
    # Other dimensions
    for role in ['area', 'status', 'severity']:
        col = key_columns.get(role)
        if col and col in df_found.columns:
            counts = df_found[col].value_counts()
            if len(counts) > 0:
                explanation += f"**{role.title()}:**\n"
                for val, count in counts.items():
                    pct = (count / len(df_found) * 100)
                    explanation += f"  • {val}: {count} events ({pct:.1f}%)\n"
                explanation += "\n"
    
    # ROOT CAUSE ANALYSIS - LLM POWERED
    if key_columns.get('description'):
        desc_col = key_columns['description']
        if desc_col in df_found.columns:
            descriptions = df_found[desc_col].dropna()
            if len(descriptions) > 0:
                explanation += f"## 🔬 {t('root_cause', lang)}\n\n"
                
                # Get LLM analysis
                sample_descriptions = descriptions.head(10).tolist()
                analysis_prompt = f"""Analyze these alarm/event descriptions and provide root cause analysis.

Sample descriptions:
{chr(10).join([f"- {d}" for d in sample_descriptions])}

Provide:
1. Common patterns/issues
2. Likely root causes
3. Equipment health indicators

Keep it concise (max 5 bullet points)."""

                try:
                    root_cause_analysis = call_llm(analysis_prompt, temperature=0.3)
                    explanation += f"{root_cause_analysis}\n\n"
                except:
                    # Fallback to keyword analysis
                    all_text = ' '.join(descriptions.astype(str).str.lower())
                    keywords = ['high', 'low', 'limit', 'alarm', 'trip', 'fault', 'failure', 'leak', 'pressure', 'temperature', 'flow']
                    found_keywords = {kw: all_text.count(kw) for kw in keywords if kw in all_text}
                    
                    if found_keywords:
                        sorted_kw = sorted(found_keywords.items(), key=lambda x: x[1], reverse=True)[:5]
                        explanation += f"**Common Issues Detected:**\n"
                        for kw, count in sorted_kw:
                            explanation += f"  • {kw.title()}: mentioned {count} times\n"
                        explanation += "\n"
                
                # Sample descriptions
                explanation += f"**Sample Event Descriptions:**\n"
                for desc in descriptions.head(3):
                    explanation += f"  • {desc}\n"
                explanation += "\n"
    
    # RECOMMENDATIONS - LLM POWERED
    explanation += f"## 💡 {t('corrective_actions', lang)}\n\n"
    
    # Build context for LLM
    context_for_rec = f"""Equipment: {identifier}
Total events: {len(df_found)}
Period: {df_found[date_col].min().date() if date_col and date_col in df_found.columns else 'N/A'} to {df_found[date_col].max().date() if date_col and date_col in df_found.columns else 'N/A'}"""
    
    # Add severity context
    if key_columns.get('severity') and key_columns['severity'] in df_found.columns:
        sev_col = key_columns['severity']
        critical_count = len(df_found[df_found[sev_col].astype(str).str.contains('critical|high', case=False, na=False)])
        context_for_rec += f"\nCritical/High severity: {critical_count} events ({critical_count/len(df_found)*100:.1f}%)"
    
    # Add frequency context
    if date_col and date_col in df_found.columns:
        df_valid = df_found[df_found[date_col].notna()].copy()
        if len(df_valid) > 0:
            df_valid['date_only'] = df_valid[date_col].dt.date
            daily = df_valid.groupby('date_only').size()
            context_for_rec += f"\nAverage frequency: {daily.mean():.1f} events/day"
    
    rec_prompt = f"""Based on this equipment data, provide specific actionable recommendations:

{context_for_rec}

Provide 4-5 specific, actionable recommendations for maintenance team. Focus on:
- Immediate actions needed
- Preventive measures
- Monitoring improvements

Format: numbered list, concise."""

    try:
        recommendations = call_llm(rec_prompt, temperature=0.3)
        explanation += f"{recommendations}\n"
    except:
        # Fallback
        explanation += f"**Regular Monitoring Recommendations:**\n"
        explanation += f"1. Set up automated alerts for this equipment\n"
        explanation += f"2. Review sensor calibration schedule\n"
        explanation += f"3. Analyze patterns in time-of-day and day-of-week\n"
        explanation += f"4. Consider equipment upgrade if failure rate is high\n"
    
    # Calculate confidence
    confidence, _ = calculate_confidence(df_found, {}, query, "identifier_search")
    
    return df_found.to_dict('records'), explanation, confidence, graph_data


def handle_pi_tag_lookup(query, df, key_columns):
    """Handle queries asking for PI tag names"""
    
    print("  🏷️ PI TAG LOOKUP MODE")
    
    # Extract equipment identifier
    identifier_patterns = [
        r'\b([A-Z]{2,}-\d+[A-Z]*)\b',
        r'\b([A-Z]{2,}\d+[A-Z]*)\b',
    ]
    
    found_identifier = None
    for pattern in identifier_patterns:
        matches = re.findall(pattern, query.upper())
        if matches:
            found_identifier = matches[0]
            break
    
    if not found_identifier:
        return None
    
    print(f"  🎯 Looking up PI tags for: {found_identifier}")
    
    identifier_col = key_columns.get('identifier')
    if not identifier_col or identifier_col not in df.columns:
        return "❌ Equipment column not found"
    
    df_match = df[df[identifier_col].astype(str).str.contains(found_identifier, case=False, na=False)]
    
    if len(df_match) == 0:
        return f"❌ Equipment `{found_identifier}` not found"
    
    # Look for PI tag columns
    pi_tag_columns = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['tag', 'pi', 'tagname', 'tag_name', 'pitag']):
            pi_tag_columns.append(col)
    
    if len(pi_tag_columns) == 0:
        return f"❌ No PI Tag columns found"
    
    print(f"  📋 Found PI tag columns: {pi_tag_columns}")
    
    report = f"# 🏷️ PI Tag Names for {found_identifier}\n\n"
    report += f"Found **{len(df_match)} events** for this equipment.\n\n"
    
    for col in pi_tag_columns:
        unique_tags = df_match[col].dropna().unique()
        if len(unique_tags) > 0:
            report += f"## {col}\n\n"
            report += f"Total **{len(unique_tags)} unique tag(s)**:\n\n"
            
            for i, tag in enumerate(sorted(unique_tags), 1):
                count = len(df_match[df_match[col] == tag])
                pct = (count / len(df_match) * 100)
                report += f"{i}. `{tag}` - {count} events ({pct:.1f}%)\n"
            
            report += "\n"
    
    # Additional info
    report += f"---\n\n"
    report += f"💡 **Additional Info:**\n"
    report += f"• Total events: {len(df_match)}\n"
    
    date_col = key_columns.get('date')
    if date_col and date_col in df_match.columns:
        df_valid = df_match[df_match[date_col].notna()]
        if len(df_valid) > 0:
            min_date = df_valid[date_col].min()
            max_date = df_valid[date_col].max()
            report += f"• Period: {min_date.date()} to {max_date.date()}\n"
    
    if key_columns.get('category'):
        cat_col = key_columns['category']
        if cat_col in df_match.columns:
            top_cat = df_match[cat_col].value_counts().head(3)
            report += f"• Top categories: {', '.join([str(k) for k in top_cat.index])}\n"
    
    print(f"  ✅ PI tag lookup complete")
    
    return report