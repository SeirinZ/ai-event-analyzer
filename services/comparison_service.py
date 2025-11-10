"""
Comparison Service for handling all comparison queries
"""
import re
import pandas as pd
from utils.translations import t
from utils.helpers import extract_equipment_codes, parse_month_from_query, get_month_name

class ComparisonService:
    """Handles comparison queries"""
    
    def __init__(self, df, key_columns):
        self.df = df
        self.key_columns = key_columns
    
    def handle_comparison_query(self, query, filters=None):
        """Universal comparison handler"""
        
        q = query.lower()
        lang = 'id' if any(kw in q for kw in ['bandingkan', 'perbandingan']) else 'en'
        
        comparison_keywords = ['bandingkan', 'vs', 'versus', 'dibanding', 'compare', 'perbandingan', 'comparison']
        if not any(kw in q for kw in comparison_keywords):
            return None
        
        print("  🔄 COMPARISON MODE")
        
        # Extract what to compare
        found_equipments = extract_equipment_codes(query)
        found_months = parse_month_from_query(query)
        
        # Detect comparison type
        if len(found_equipments) >= 2:
            return self._compare_equipments(found_equipments, filters, lang)
        elif len(found_months) >= 2:
            return self._compare_months(found_months, filters, lang)
        
        return None
    
    def _compare_equipments(self, equipments, filters, lang):
        """Compare multiple equipments"""
        results = {}
        
        identifier_col = self.key_columns.get('identifier')
        if not identifier_col:
            return None
        
        for eq in equipments:
            df_eq = self.df[self.df[identifier_col].astype(str).str.contains(eq, case=False, na=False)]
            
            # Apply filters
            if filters:
                from services.filter_service import FilterService
                filter_service = FilterService(self.df, self.key_columns)
                df_eq = filter_service.apply_time_filters(df_eq, filters)
            
            results[eq] = {
                "count": len(df_eq),
                "breakdown": self._build_breakdown(df_eq)
            }
        
        return self._build_comparison_report(results, f"{t('equipment', lang)} {t('comparison', lang)}", lang)
    
    def _compare_months(self, months, filters, lang):
        """Compare multiple months"""
        results = {}
        date_col = self.key_columns.get('date')
        
        if not date_col:
            return None
        
        for month in months:
            df_month = self.df[self.df[date_col].dt.month == month]
            month_name = get_month_name(month, lang)
            
            results[month_name] = {
                "count": len(df_month),
                "breakdown": self._build_breakdown(df_month)
            }
        
        return self._build_comparison_report(results, t('comparison', lang), lang)
    
    def _build_breakdown(self, df_filtered):
        """Build breakdown of filtered data"""
        breakdown = {}
        
        for role in ['identifier', 'category', 'area']:
            col = self.key_columns.get(role)
            if col and col in df_filtered.columns:
                counts = df_filtered[col].value_counts().head(5)
                breakdown[role] = {str(k): int(v) for k, v in counts.items()}
        
        return breakdown
    
    def _build_comparison_report(self, results, title, lang):
        """Build comparison report"""
        report = f"# 📊 {title}\n\n"
        
        total_all = sum(r['count'] for r in results.values())
        report += f"## 🎯 {t('executive_summary', lang)}\n\n"
        report += f"**{t('total_events', lang)}:** {total_all:,}\n"
        report += f"**Entities Compared:** {len(results)}\n\n"
        
        # Summary table
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
        
        # Detailed breakdown
        report += f"## 🔍 {t('detailed_breakdown', lang)}\n\n"
        
        for entity, data in results.items():
            report += f"### {entity}\n\n"
            report += f"**{t('total_events', lang)}:** {data['count']:,}\n\n"
            
            breakdown = data.get('breakdown', {})
            
            for key in ['identifier', 'category', 'area']:
                if key in breakdown and breakdown[key]:
                    report += f"**{key.title()}:**\n"
                    for val, count in breakdown[key].items():
                        pct = (count / data['count'] * 100) if data['count'] > 0 else 0
                        report += f"  • {val}: {count:,} ({pct:.1f}%)\n"
                    report += "\n"
        
        # Insights
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
        
        return report