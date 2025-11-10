"""
Temporal Analysis Service
"""
from utils.translations import t
from utils.helpers import get_month_name, get_day_name

class TemporalService:
    """Handles temporal/time-based analysis"""
    
    def __init__(self, df, key_columns):
        self.df = df
        self.key_columns = key_columns
    
    def handle_temporal_query(self, query, lang='en'):
        """Handle temporal analysis queries"""
        
        q = query.lower()
        date_col = self.key_columns.get('date')
        
        if not date_col or date_col not in self.df.columns:
            return None
        
        df_valid = self.df[self.df[date_col].notna()].copy()
        if len(df_valid) == 0:
            return None
        
        # Detect query type
        asking_month = 'month' in q or 'bulan' in q
        asking_date = 'date' in q or 'tanggal' in q
        asking_day = ('day' in q or 'hari' in q) and not asking_date
        asking_most = any(kw in q for kw in ['most', 'terbanyak', 'tertinggi', 'maksimal', 'paling banyak'])
        asking_least = any(kw in q for kw in ['least', 'tersedikit', 'terendah', 'minimal', 'paling sedikit'])
        
        # MONTH ANALYSIS
        if asking_month:
            df_valid['month'] = df_valid[date_col].dt.month
            month_counts = df_valid.groupby('month').size().sort_values(ascending=False)
            
            if asking_most:
                top_month = month_counts.index[0]
                top_count = month_counts.iloc[0]
                month_name = get_month_name(top_month, lang)
                
                # Get top dates
                top_dates = df_valid[df_valid['month'] == top_month][date_col].dt.date.value_counts().head(5)
                
                report = f"# 📊 {t('most_events', lang)} - {t('month', lang)}\n\n"
                report += f"**{month_name}** had the most events: **{top_count:,} events**\n\n"
                report += f"## 📅 Top 5 dates in {month_name}:\n\n"
                for date, count in top_dates.items():
                    report += f"• **{date}**: {count:,} events\n"
                return report
                
            elif asking_least:
                least_month = month_counts.index[-1]
                least_count = month_counts.iloc[-1]
                month_name = get_month_name(least_month, lang)
                
                report = f"# 📊 {t('least_events', lang)} - {t('month', lang)}\n\n"
                report += f"**{month_name}** had the least events: **{least_count:,} events**\n"
                return report
            else:
                report = f"# 📊 {t('total_events', lang)} by {t('month', lang)}\n\n"
                report += "## 📊 Monthly Ranking:\n\n"
                for i, (month, count) in enumerate(month_counts.items(), 1):
                    pct = (count / len(df_valid) * 100)
                    report += f"{i}. **{get_month_name(month, lang)}**: {count:,} events ({pct:.1f}%)\n"
                return report
        
        # DATE ANALYSIS
        elif asking_date:
            df_valid['day'] = df_valid[date_col].dt.day
            day_counts = df_valid.groupby('day').size().sort_values(ascending=False)
            
            if asking_most:
                top_day = day_counts.index[0]
                top_count = day_counts.iloc[0]
                
                actual_dates = df_valid[df_valid['day'] == top_day].groupby(df_valid[date_col].dt.date).size().sort_values(ascending=False)
                
                report = f"# 📊 {t('most_events', lang)} - {t('date', lang)}\n\n"
                report += f"**Day {top_day}** of the month had most events: **{top_count:,} events total**\n\n"
                report += f"## 📅 Specific dates with day {top_day}:\n\n"
                for date, count in actual_dates.head(10).items():
                    report += f"• **{date}**: {count:,} events\n"
                return report
                
            elif asking_least:
                least_day = day_counts.index[-1]
                least_count = day_counts.iloc[-1]
                
                report = f"# 📊 {t('least_events', lang)} - {t('date', lang)}\n\n"
                report += f"**Day {least_day}** of the month had least events: **{least_count:,} events**\n"
                return report
        
        # DAY OF WEEK ANALYSIS  
        elif asking_day:
            df_valid['dow'] = df_valid[date_col].dt.dayofweek
            dow_counts = df_valid.groupby('dow').size().sort_values(ascending=False)
            
            if asking_most:
                top_dow = dow_counts.index[0]
                top_count = dow_counts.iloc[0]
                day_name = get_day_name(top_dow, lang)
                
                report = f"# 📊 {t('most_events', lang)} - Day of Week\n\n"
                report += f"**{day_name}** had the most events: **{top_count:,} events**\n"
                return report
                
            elif asking_least:
                least_dow = dow_counts.index[-1]
                least_count = dow_counts.iloc[-1]
                day_name = get_day_name(least_dow, lang)
                
                report = f"# 📊 {t('least_events', lang)} - Day of Week\n\n"
                report += f"**{day_name}** had the least events: **{least_count:,} events**\n"
                return report
        
        return None