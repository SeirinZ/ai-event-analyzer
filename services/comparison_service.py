import re
from config import MONTHS_MAP, MONTH_NAMES
from utils.translations import detect_language, t

def handle_comparison_query(query, df, key_columns):
    """Handle ALL comparison types - UNIVERSAL HANDLER"""
    
    q = query.lower()
    lang = detect_language(query)
    
    comparison_keywords = ['bandingkan', 'vs', 'versus', 'dibanding', 'compare', 'perbandingan', 'comparison']
    if not any(kw in q for kw in comparison_keywords):
        return None
    
    print("  🔄 UNIVERSAL COMPARISON MODE")
    
    date_col = key_columns.get('date')
    identifier_col = key_columns.get('identifier')
    category_col = key_columns.get('category')
    area_col = key_columns.get('area')
    
    # ========================================
    # STEP 1: DETECT WHAT TO COMPARE
    # ========================================
    
    # Extract equipment codes
    identifier_patterns = [r'\b([A-Z]{2,}-\d+[A-Z]*)\b', r'\b([A-Z]{2,}\d+[A-Z]*)\b']
    found_equipments = []
    for pattern in identifier_patterns:
        found_equipments.extend(re.findall(pattern, query.upper()))
    found_equipments = list(dict.fromkeys(found_equipments))
    
    # Detect date range FIRST (08-17 september)
    date_with_month_pattern = r'(\d{1,2})\s*[-–to/sd]\s*(\d{1,2})\s+(january|jan|februari|feb|february|march|mar|maret|april|apr|may|mei|june|jun|juni|july|jul|juli|august|aug|agustus|ags|september|sept|sep|october|oct|oktober|okt|november|nov|december|dec|desember|des)'
    date_range_match = re.search(date_with_month_pattern, q, re.IGNORECASE)
    
    date_range = None
    found_months = []
    
    if date_range_match:
        start_day = int(date_range_match.group(1))
        end_day = int(date_range_match.group(2))
        month_str = date_range_match.group(3).lower()
        month_num = MONTHS_MAP.get(month_str)
        if month_num:
            date_range = (start_day, end_day)
            found_months = [month_num]
    else:
        for month_str, month_num in MONTHS_MAP.items():
            if month_str in q and len(month_str) > 3:
                found_months.append(month_num)
        found_months = list(set(found_months))
    
    # Extract areas
    found_areas = []
    if area_col and area_col in df.columns:
        unique_areas = df[area_col].dropna().unique()
        for area in unique_areas:
            area_str = str(area).lower()
            if len(area_str) >= 4 and area_str in q:
                pattern = r'\b' + re.escape(area_str) + r'\b'
                if re.search(pattern, q):
                    found_areas.append(area)
    
    # Extract categories
    found_categories = []
    if category_col and category_col in df.columns:
        unique_cats = df[category_col].dropna().unique()
        for cat in unique_cats:
            cat_str = str(cat).lower()
            if len(cat_str) >= 4 and cat_str in q:
                pattern = r'\b' + re.escape(cat_str) + r'\b'
                if re.search(pattern, q):
                    found_categories.append(cat)
    
    # Detect "top N"
    top_pattern = r'top\s*(\d+)'
    top_match = re.search(top_pattern, q, re.IGNORECASE)
    top_n = int(top_match.group(1)) if top_match else None
    
    print(f"  📊 Detected:")
    print(f"    - Equipments: {found_equipments}")
    print(f"    - Months: {found_months}")
    print(f"    - Date range: {date_range}")
    print(f"    - Areas: {found_areas}")
    print(f"    - Categories: {found_categories}")
    print(f"    - Top N: {top_n}")
    
    # ========================================
    # STEP 2: DETERMINE COMPARISON TYPE
    # ========================================
    
    comparison_entities = []
    comparison_type = None
    
    if top_n and identifier_col:
        top_equipments = df[identifier_col].value_counts().head(top_n).index.tolist()
        comparison_entities = top_equipments
        comparison_type = 'equipment'
        print(f"  🎯 Mode: Top {top_n} equipment")
    elif len(found_equipments) >= 2:
        comparison_entities = found_equipments
        comparison_type = 'equipment'
        print(f"  🎯 Mode: Equipment comparison")
    elif len(found_areas) >= 2:
        comparison_entities = found_areas
        comparison_type = 'area'
        print(f"  🎯 Mode: Area comparison")
    elif len(found_categories) >= 2:
        comparison_entities = found_categories
        comparison_type = 'category'
        print(f"  🎯 Mode: Category comparison")
    elif len(found_months) >= 2:
        comparison_entities = found_months
        comparison_type = 'month'
        if len(found_equipments) == 1:
            comparison_type = 'equipment_month'
            comparison_entities = {'equipment': found_equipments[0], 'months': found_months}
        print(f"  🎯 Mode: Month comparison")
    elif len(found_equipments) == 1 and len(found_months) >= 2:
        comparison_type = 'equipment_month'
        comparison_entities = {'equipment': found_equipments[0], 'months': found_months}
        print(f"  🎯 Mode: Single equipment, multi-month")
    else:
        print(f"  ⚠️ Could not determine comparison type")
        return None
    
    # ========================================
    # STEP 3: BUILD RESULTS
    # ========================================
    
    results = {}
    
    if comparison_type == 'equipment':
        for eq in comparison_entities:
            df_eq = df[df[identifier_col].astype(str).str.contains(eq, case=False, na=False)]
            
            if date_col and date_col in df_eq.columns:
                if found_months:
                    df_eq = df_eq[df_eq[date_col].dt.month.isin(found_months)]
                if date_range:
                    df_eq = df_eq[(df_eq[date_col].dt.day >= date_range[0]) & (df_eq[date_col].dt.day <= date_range[1])]
            
            results[eq] = {
                "count": len(df_eq),
                "breakdown": build_detailed_breakdown(df_eq, key_columns, show_all=True)
            }
    
    elif comparison_type == 'area':
        for area in comparison_entities:
            df_area = df[df[area_col] == area]
            
            if date_col and date_col in df_area.columns:
                if found_months:
                    df_area = df_area[df_area[date_col].dt.month.isin(found_months)]
                if date_range:
                    df_area = df_area[(df_area[date_col].dt.day >= date_range[0]) & (df_area[date_col].dt.day <= date_range[1])]
            
            results[area] = {
                "count": len(df_area),
                "breakdown": build_detailed_breakdown(df_area, key_columns, show_all=True)
            }
    
    elif comparison_type == 'category':
        for cat in comparison_entities:
            df_cat = df[df[category_col] == cat]
            
            if date_col and date_col in df_cat.columns:
                if found_months:
                    df_cat = df_cat[df_cat[date_col].dt.month.isin(found_months)]
                if date_range:
                    df_cat = df_cat[(df_cat[date_col].dt.day >= date_range[0]) & (df_cat[date_col].dt.day <= date_range[1])]
            
            results[cat] = {
                "count": len(df_cat),
                "breakdown": build_detailed_breakdown(df_cat, key_columns, show_all=True)
            }
    
    elif comparison_type == 'month':
        for month in comparison_entities:
            df_month = df[df[date_col].dt.month == month]
            results[MONTH_NAMES[month]] = {
                "count": len(df_month),
                "breakdown": build_detailed_breakdown(df_month, key_columns, show_all=True)
            }
    
    elif comparison_type == 'equipment_month':
        equipment = comparison_entities['equipment']
        months = comparison_entities['months']
        
        for month in months:
            df_eq_month = df[
                (df[identifier_col].astype(str).str.contains(equipment, case=False, na=False)) &
                (df[date_col].dt.month == month)
            ]
            
            if date_range:
                df_eq_month = df_eq_month[(df_eq_month[date_col].dt.day >= date_range[0]) & (df_eq_month[date_col].dt.day <= date_range[1])]
            
            results[f"{equipment} - {MONTH_NAMES[month]}"] = {
                "count": len(df_eq_month),
                "breakdown": build_detailed_breakdown(df_eq_month, key_columns, show_all=True)
            }
    
    # ========================================
    # STEP 4: BUILD TITLE
    # ========================================
    
    title_parts = []
    
    if comparison_type == 'equipment':
        title_parts.append(t('equipment', lang) + " " + t('comparison', lang))
    elif comparison_type == 'area':
        title_parts.append(f"{'Area' if lang == 'en' else 'Area'} {t('comparison', lang)}")
    elif comparison_type == 'category':
        title_parts.append(f"{'Category' if lang == 'en' else 'Kategori'} {t('comparison', lang)}")
    elif comparison_type in ['month', 'equipment_month']:
        title_parts.append(t('comparison', lang))
    
    time_info = []
    if found_months and comparison_type != 'month':
        month_strs = [MONTH_NAMES[m] for m in found_months]
        time_info.append(', '.join(month_strs))
    
    if date_range:
        time_info.append(f"date {date_range[0]}-{date_range[1]}")
    
    if time_info:
        title_parts.append(f"({', '.join(time_info)})")
    
    title = " ".join(title_parts)
    
    return build_enhanced_comparison_report(results, title, lang)


def build_detailed_breakdown(df_filtered, key_columns, show_all=False):
    """Build 3-level hierarchy breakdown"""
    
    breakdown = {}
    limit = None if show_all else 10
    
    # Level 1: Equipment
    if key_columns.get('identifier'):
        col = key_columns['identifier']
        if col in df_filtered.columns:
            counts = df_filtered[col].value_counts()
            breakdown['equipment'] = {str(k): int(v) for k, v in (counts.items() if show_all else counts.head(limit).items())}
    
    # Level 2: Equipment Name
    if key_columns.get('equipment_name'):
        col = key_columns['equipment_name']
        if col in df_filtered.columns:
            counts = df_filtered[col].value_counts()
            breakdown['equipment_name'] = {str(k): int(v) for k, v in (counts.items() if show_all else counts.head(limit).items())}
    
    # Level 3: PI Tag
    if key_columns.get('pi_tag'):
        col = key_columns['pi_tag']
        if col in df_filtered.columns:
            counts = df_filtered[col].value_counts()
            breakdown['pi_tag'] = {str(k): int(v) for k, v in (counts.items() if show_all else counts.head(limit).items())}
    
    # Other dimensions
    for role in ['category', 'area', 'status', 'severity', 'limit_type']:
        col = key_columns.get(role)
        if col and col in df_filtered.columns:
            counts = df_filtered[col].value_counts()
            breakdown[role] = {str(k): int(v) for k, v in counts.head(5).items()}
    
    return breakdown


def build_enhanced_comparison_report(results, title, lang='en'):
    """Build ENHANCED comparison with full insights"""
    
    report = f"# 📊 {title}\n\n"
    
    # Executive Summary
    total_all = sum(r['count'] for r in results.values())
    report += f"## 🎯 {t('executive_summary', lang)}\n\n"
    report += f"**{t('total_events', lang)}:** {total_all:,}\n"
    report += f"**Entities Compared:** {len(results)}\n\n"
    
    # Summary Table
    report += f"## 📈 {t('comparison', lang)} Summary\n\n"
    report += f"| Entity | {t('total_events', lang)} | Percentage | Status |\n"
    report += "|--------|--------|------------|--------|\n"
    
    sorted_results = sorted(results.items(), key=lambda x: x[1]['count'], reverse=True)
    max_count = max(r['count'] for r in results.values())
    min_count = min(r['count'] for r in results.values())
    
    for entity, data in sorted_results:
        count = data['count']
        pct = (count / total_all * 100) if total_all > 0 else 0
        
        if count == max_count:
            status = f"🔴 {t('highest', lang)}"
        elif count == min_count:
            status = f"🟢 {t('lowest', lang)}"
        else:
            status = f"⚪ {t('normal', lang)}"
        
        report += f"| **{entity}** | {count:,} | {pct:.1f}% | {status} |\n"
    
    report += "\n"
    
    # Detailed Analysis with FULL HIERARCHY
    report += f"## 🔍 {t('detailed_breakdown', lang)}\n\n"
    
    for entity, data in results.items():
        report += f"### {entity}\n\n"
        report += f"**{t('total_events', lang)}:** {data['count']:,}\n\n"
        
        breakdown = data.get('breakdown', {})
        
        # 3-Level Hierarchy Display
        if 'equipment' in breakdown:
            report += f"**🏭 {t('top_equipment', lang)}:**\n"
            for i, (eq, count) in enumerate(breakdown['equipment'].items(), 1):
                pct = (count / data['count'] * 100) if data['count'] > 0 else 0
                report += f"  {i}. {eq}: {count:,} events ({pct:.1f}%)\n"
            report += "\n"
        
        if 'equipment_name' in breakdown:
            report += f"**🔧 {t('specific_equipment', lang)}:**\n"
            for i, (name, count) in enumerate(breakdown['equipment_name'].items(), 1):
                pct = (count / data['count'] * 100) if data['count'] > 0 else 0
                report += f"  {i}. {name}: {count:,} events ({pct:.1f}%)\n"
            report += "\n"
        
        if 'pi_tag' in breakdown:
            report += f"**📡 {t('pi_sensors', lang)}:**\n"
            for i, (tag, count) in enumerate(breakdown['pi_tag'].items(), 1):
                pct = (count / data['count'] * 100) if data['count'] > 0 else 0
                report += f"  {i}. `{tag}`: {count:,} events ({pct:.1f}%)\n"
            report += "\n"
        
        # Other breakdowns
        for key in ['category', 'limit_type', 'area', 'status', 'severity']:
            if key in breakdown and breakdown[key]:
                report += f"**{key.replace('_', ' ').title()}:**\n"
                for val, count in breakdown[key].items():
                    pct = (count / data['count'] * 100) if data['count'] > 0 else 0
                    report += f"  • {val}: {count:,} ({pct:.1f}%)\n"
                report += "\n"
    
    # Key Insights
    report += f"## 💡 {t('insights', lang)}\n\n"
    
    highest = sorted_results[0]
    lowest = sorted_results[-1]
    
    diff = highest[1]['count'] - lowest[1]['count']
    if lowest[1]['count'] > 0:
        diff_pct = (diff / lowest[1]['count'] * 100)
        report += f"1. **{highest[0]}** has **{diff_pct:.0f}% more events** than {lowest[0]}\n"
    else:
        report += f"1. **{highest[0]}** has **{diff:,} more events** than {lowest[0]}\n"
    
    report += f"2. **Total difference:** {diff:,} events\n"
    
    if highest[1]['count'] / total_all > 0.6:
        report += f"3. **⚠️ High concentration:** {highest[0]} accounts for {highest[1]['count']/total_all*100:.1f}% of total\n"
    
    report += f"\n**📌 {t('recommendation', lang)}:** Focus on high-event entities to reduce incident rate.\n"
    
    return report