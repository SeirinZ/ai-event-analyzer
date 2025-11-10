"""
Identifier Search Service - ENHANCED with PI Tag & Equipment Name Search
"""
import re
import pandas as pd

class IdentifierService:
    """Handles identifier-based searches including Equipment Name and PI Tag"""
    
    def __init__(self, df, key_columns):
        self.df = df
        self.key_columns = key_columns
    
    def is_identifier_query(self, query):
        """Check if query contains equipment identifier, name, or PI tag"""
        # Check for equipment codes
        equipment_patterns = [
            r'\b([A-Z]{2,}-\d+[A-Z]*)\b',
            r'\b([A-Z]{2,}\d+[A-Z]*)\b',
        ]
        
        has_equipment_code = any(re.search(p, query.upper()) for p in equipment_patterns)
        
        # Check for specific queries about PI tag or equipment name
        pi_tag_keywords = ['pi tag', 'tag name', 'tagname', 'sensor', 'pi name']
        equipment_name_keywords = ['equipment name', 'nama equipment', 'specific equipment']
        
        has_pi_query = any(kw in query.lower() for kw in pi_tag_keywords)
        has_name_query = any(kw in query.lower() for kw in equipment_name_keywords)
        
        return has_equipment_code or has_pi_query or has_name_query
    
    def search_by_identifier(self, query, filters=None):
        """Search by equipment identifier, name, or PI tag with filters"""
        
        # Try to find equipment code
        patterns = [
            r'\b([A-Z]{2,}-\d+[A-Z]*)\b',
            r'\b([A-Z]{2,}\d+[A-Z]*)\b',
        ]
        
        identifier = None
        for pattern in patterns:
            matches = re.findall(pattern, query.upper())
            if matches:
                identifier = matches[0]
                break
        
        # If no equipment code found, try searching by equipment name or PI tag
        if not identifier:
            return self._search_by_name_or_tag(query, filters)
        
        # Search in identifier column
        id_col = self.key_columns.get('identifier')
        if not id_col or id_col not in self.df.columns:
            return None, f"❌ Equipment column not found", 0
        
        df_found = self.df[
            self.df[id_col].astype(str).str.contains(identifier, case=False, na=False)
        ]
        
        # Apply filters
        if filters:
            from services.filter_service import FilterService
            filter_service = FilterService(self.df, self.key_columns)
            df_found = filter_service.apply_time_filters(df_found, filters)
        
        if len(df_found) == 0:
            return None, f"❌ No data found for `{identifier}`", 0
        
        # Build comprehensive report
        report = self._build_comprehensive_report(df_found, identifier, filters)
        
        confidence = 90
        return df_found.to_dict('records'), report, confidence
    
    def _search_by_name_or_tag(self, query, filters):
        """Search by equipment name or PI tag"""
        
        q = query.lower()
        
        # Try Equipment Name
        eq_name_col = self.key_columns.get('equipment_name')
        if eq_name_col and eq_name_col in self.df.columns:
            # Extract potential equipment name (words in query)
            words = [w for w in query.split() if len(w) > 3 and w.upper() == w or '-' in w]
            
            for word in words:
                df_found = self.df[
                    self.df[eq_name_col].astype(str).str.contains(word, case=False, na=False)
                ]
                
                if len(df_found) > 0:
                    if filters:
                        from services.filter_service import FilterService
                        filter_service = FilterService(self.df, self.key_columns)
                        df_found = filter_service.apply_time_filters(df_found, filters)
                    
                    if len(df_found) > 0:
                        report = self._build_comprehensive_report(df_found, word, filters, search_type="Equipment Name")
                        return df_found.to_dict('records'), report, 85
        
        # Try PI Tag
        pi_tag_col = self.key_columns.get('pi_tag')
        if pi_tag_col and pi_tag_col in self.df.columns:
            words = [w for w in query.split() if len(w) > 3]
            
            for word in words:
                df_found = self.df[
                    self.df[pi_tag_col].astype(str).str.contains(word, case=False, na=False)
                ]
                
                if len(df_found) > 0:
                    if filters:
                        from services.filter_service import FilterService
                        filter_service = FilterService(self.df, self.key_columns)
                        df_found = filter_service.apply_time_filters(df_found, filters)
                    
                    if len(df_found) > 0:
                        report = self._build_comprehensive_report(df_found, word, filters, search_type="PI Tag")
                        return df_found.to_dict('records'), report, 85
        
        return None, "❌ No matching equipment, name, or PI tag found", 0
    
    def _build_comprehensive_report(self, df_found, identifier, filters, search_type="Equipment Code"):
        """Build comprehensive report with recommendations"""
        
        report = f"# 🔍 Search Results: `{identifier}` ({search_type})\n\n"
        report += f"## 📊 Executive Summary\n\n"
        report += f"**Found {len(df_found):,} events** for {search_type} `{identifier}`\n\n"
        
        # Equipment Hierarchy - FULL 3 LEVELS
        report += f"## 🏗️ Equipment Hierarchy (3 Levels)\n\n"
        
        # Level 1: Equipment Code
        if self.key_columns.get('identifier'):
            col = self.key_columns['identifier']
            unique_eq = df_found[col].dropna().unique()
            report += f"### Level 1: Equipment Codes\n"
            if len(unique_eq) <= 10:
                for eq in unique_eq:
                    count = len(df_found[df_found[col] == eq])
                    pct = (count / len(df_found) * 100)
                    report += f"  • `{eq}`: {count} events ({pct:.1f}%)\n"
            else:
                eq_counts = df_found[col].value_counts()
                for eq, count in eq_counts.head(10).items():
                    pct = (count / len(df_found) * 100)
                    report += f"  • `{eq}`: {count} events ({pct:.1f}%)\n"
                report += f"  • ... and {len(unique_eq) - 10} more\n"
            report += "\n"
        
        # Level 2: Equipment Names
        if self.key_columns.get('equipment_name'):
            col = self.key_columns['equipment_name']
            if col in df_found.columns:
                unique_names = df_found[col].dropna().unique()
                if len(unique_names) > 0:
                    report += f"### Level 2: Specific Equipment Names ({len(unique_names)} unique)\n"
                    name_counts = df_found[col].value_counts()
                    for name, count in list(name_counts.items())[:15]:
                        pct = (count / len(df_found) * 100)
                        report += f"  • **{name}**: {count} events ({pct:.1f}%)\n"
                    if len(unique_names) > 15:
                        report += f"  • ... and {len(unique_names) - 15} more\n"
                    report += "\n"
        
        # Level 3: PI Tags
        if self.key_columns.get('pi_tag'):
            col = self.key_columns['pi_tag']
            if col in df_found.columns:
                unique_tags = df_found[col].dropna().unique()
                if len(unique_tags) > 0:
                    report += f"### Level 3: PI Tag Sensors ({len(unique_tags)} unique)\n"
                    tag_counts = df_found[col].value_counts()
                    for tag, count in list(tag_counts.items())[:15]:
                        pct = (count / len(df_found) * 100)
                        report += f"  • `{tag}`: {count} events ({pct:.1f}%)\n"
                    if len(unique_tags) > 15:
                        report += f"  • ... and {len(unique_tags) - 15} more sensors\n"
                    report += "\n"
        
        # Timeline Analysis
        date_col = self.key_columns.get('date')
        if date_col and date_col in df_found.columns:
            df_valid = df_found[df_found[date_col].notna()].copy()
            if len(df_valid) > 0:
                min_date = df_valid[date_col].min()
                max_date = df_valid[date_col].max()
                days_span = (max_date - min_date).days + 1
                
                report += f"## 📅 Timeline Analysis\n\n"
                report += f"• **Period:** {min_date.date()} to {max_date.date()} ({days_span} days)\n"
                
                df_valid['date_only'] = df_valid[date_col].dt.date
                events_by_date = df_valid.groupby('date_only').size().sort_index()
                
                avg_per_day = events_by_date.mean()
                max_per_day = events_by_date.max()
                min_per_day = events_by_date.min()
                
                report += f"• **Average:** {avg_per_day:.1f} events/day\n"
                report += f"• **Range:** {min_per_day}-{max_per_day} events/day\n"
                
                # Trend analysis
                if len(events_by_date) >= 7:
                    first_week = events_by_date.head(7).mean()
                    last_week = events_by_date.tail(7).mean()
                    
                    if last_week > first_week * 1.2:
                        trend = "📈 INCREASING (⚠️ Needs attention)"
                    elif last_week < first_week * 0.8:
                        trend = "📉 DECREASING (✅ Improving)"
                    else:
                        trend = "➡️ STABLE"
                    
                    report += f"• **Trend:** {trend}\n"
                
                report += "\n"
                
                # Top 5 busiest days
                top_days = events_by_date.sort_values(ascending=False).head(5)
                report += f"**📈 Top 5 Busiest Days:**\n"
                for date, count in top_days.items():
                    report += f"  • {date}: **{count} events**\n"
                report += "\n"
        
        # Event Breakdown
        report += f"## 📊 Event Breakdown\n\n"
        
        for role in ['category', 'limit_type', 'area', 'status', 'severity']:
            col = self.key_columns.get(role)
            if col and col in df_found.columns:
                counts = df_found[col].value_counts()
                if len(counts) > 0:
                    report += f"**{role.replace('_', ' ').title()}:**\n"
                    for val, count in counts.head(5).items():
                        pct = (count / len(df_found) * 100)
                        report += f"  • {val}: {count} events ({pct:.1f}%)\n"
                    report += "\n"
        
        # KEY INSIGHTS
        report += f"## 💡 Key Insights\n\n"
        
        # Insight 1: Event concentration
        if self.key_columns.get('category'):
            cat_col = self.key_columns['category']
            if cat_col in df_found.columns:
                top_cat = df_found[cat_col].value_counts().iloc[0]
                top_cat_name = df_found[cat_col].value_counts().index[0]
                top_cat_pct = (top_cat / len(df_found) * 100)
                
                if top_cat_pct > 50:
                    report += f"1. **High Concentration:** {top_cat_pct:.1f}% of events are '{top_cat_name}' - indicates specific recurring issue\n"
                else:
                    report += f"1. **Diverse Events:** Events spread across multiple categories - indicates various issues\n"
        
        # Insight 2: Frequency analysis
        if date_col and date_col in df_found.columns:
            df_valid = df_found[df_found[date_col].notna()].copy()
            if len(df_valid) > 0:
                df_valid['date_only'] = df_valid[date_col].dt.date
                daily = df_valid.groupby('date_only').size()
                
                if daily.mean() > 10:
                    report += f"2. **High Frequency:** Average {daily.mean():.1f} events/day - requires immediate attention ⚠️\n"
                elif daily.mean() > 5:
                    report += f"2. **Moderate Frequency:** Average {daily.mean():.1f} events/day - monitor closely\n"
                else:
                    report += f"2. **Low Frequency:** Average {daily.mean():.1f} events/day - within normal range\n"
        
        # Insight 3: Area/Status  
        if self.key_columns.get('area'):
            area_col = self.key_columns['area']
            if area_col in df_found.columns:
                unique_areas = df_found[area_col].nunique()
                if unique_areas == 1:
                    area_name = df_found[area_col].iloc[0]
                    report += f"3. **Isolated to Area:** All events in '{area_name}' - localized issue\n"
                else:
                    report += f"3. **Multiple Areas:** Events across {unique_areas} areas - widespread issue\n"
        
        # RECOMMENDATIONS - ACTIONABLE!
        report += f"\n## 📌 Recommended Actions\n\n"
        
        # Calculate severity
        avg_daily = events_by_date.mean() if 'events_by_date' in locals() else 0
        
        if avg_daily > 10:
            report += f"### 🚨 URGENT (High Frequency: {avg_daily:.1f} events/day)\n\n"
            report += f"1. **Immediate Investigation Required**\n"
            report += f"   - Form incident response team\n"
            report += f"   - Review last 7 days of events in detail\n"
            report += f"   - Identify root cause ASAP\n\n"
            report += f"2. **Temporary Mitigation**\n"
            report += f"   - Increase monitoring frequency\n"
            report += f"   - Adjust alarm thresholds if false alarms\n"
            report += f"   - Consider temporary operational changes\n\n"
            report += f"3. **Preventive Actions**\n"
            report += f"   - Schedule emergency maintenance\n"
            report += f"   - Review and update SOPs\n"
            report += f"   - Implement additional safeguards\n"
        elif avg_daily > 5:
            report += f"### ⚠️ MONITOR CLOSELY (Moderate Frequency: {avg_daily:.1f} events/day)\n\n"
            report += f"1. **Scheduled Investigation**\n"
            report += f"   - Review event patterns weekly\n"
            report += f"   - Identify trending issues\n"
            report += f"   - Document findings\n\n"
            report += f"2. **Proactive Maintenance**\n"
            report += f"   - Schedule preventive maintenance\n"
            report += f"   - Check sensor calibration\n"
            report += f"   - Review equipment condition\n\n"
            report += f"3. **Process Optimization**\n"
            report += f"   - Analyze operational patterns\n"
            report += f"   - Identify improvement opportunities\n"
            report += f"   - Update maintenance schedule\n"
        else:
            report += f"### ✅ NORMAL OPERATION (Low Frequency: {avg_daily:.1f} events/day)\n\n"
            report += f"1. **Continue Regular Monitoring**\n"
            report += f"   - Monthly review of trends\n"
            report += f"   - Maintain current maintenance schedule\n"
            report += f"   - Keep alarm settings as-is\n\n"
            report += f"2. **Optimization Opportunities**\n"
            report += f"   - Fine-tune alarm thresholds\n"
            report += f"   - Document best practices\n"
            report += f"   - Share learnings with team\n\n"
            report += f"3. **Continuous Improvement**\n"
            report += f"   - Review quarterly for patterns\n"
            report += f"   - Update procedures as needed\n"
            report += f"   - Train operators on prevention\n"
        
        return report