from config import MONTH_NAMES
from utils.translations import detect_language, t

def handle_temporal_analysis(query, df, key_columns):
    """Enhanced temporal analysis with complete date information"""
    
    print("  📊 TEMPORAL ANALYSIS MODE")
    
    q = query.lower()
    lang = detect_language(query)
    date_col = key_columns.get('date')
    
    if not date_col or date_col not in df.columns:
        return None
    
    df_valid = df[df[date_col].notna()].copy()
    if len(df_valid) == 0:
        return None
    
    # Detect what user is asking
    asking_month = 'month' in q or 'bulan' in q
    asking_date = 'date' in q or 'tanggal' in q
    asking_day = ('day' in q or 'hari' in q) and not asking_date
    asking_most = any(kw in q for kw in ['most', 'terbanyak', 'tertinggi', 'maksimal', 'paling banyak'])
    asking_least = any(kw in q for kw in ['least', 'tersedikit', 'terendah', 'minimal', 'paling sedikit'])
    
    report = ""
    
    # MONTH ANALYSIS
    if asking_month:
        df_valid['month'] = df_valid[date_col].dt.month
        month_counts = df_valid.groupby('month').size().sort_values(ascending=False)
        
        if asking_most:
            top_month = month_counts.index[0]
            top_count = month_counts.iloc[0]
            top_dates = df_valid[df_valid['month'] == top_month][date_col].dt.date.value_counts().head(5)
            
            report = f"# 📊 {t('most_events', lang)} - {'Month' if lang == 'en' else 'Bulan'}\n\n"
            report += f"**{MONTH_NAMES[top_month]}** had the most events: **{top_count:,} events**\n\n"
            report += f"## 📅 Top 5 dates in {MONTH_NAMES[top_month]}:\n\n"
            for date, count in top_dates.items():
                report += f"• **{date}**: {count:,} events\n"
            report += "\n"
            
        elif asking_least:
            least_month = month_counts.index[-1]
            least_count = month_counts.iloc[-1]
            least_dates = df_valid[df_valid['month'] == least_month][date_col].dt.date.value_counts().head(5)
            
            report = f"# 📊 {t('least_events', lang)} - {'Month' if lang == 'en' else 'Bulan'}\n\n"
            report += f"**{MONTH_NAMES[least_month]}** had the least events: **{least_count:,} events**\n\n"
            report += f"## 📅 Dates in {MONTH_NAMES[least_month]}:\n\n"
            for date, count in least_dates.items():
                report += f"• **{date}**: {count:,} events\n"
            report += "\n"
        else:
            report = f"# 📊 {t('total_events', lang)} by {'Month' if lang == 'en' else 'Bulan'}\n\n"
        
        report += f"## 📊 Monthly Ranking:\n\n"
        for i, (month, count) in enumerate(month_counts.items(), 1):
            pct = (count / len(df_valid) * 100)
            report += f"{i}. **{MONTH_NAMES[month]}**: {count:,} events ({pct:.1f}%)\n"
    
    # DATE ANALYSIS
    elif asking_date:
        df_valid['day'] = df_valid[date_col].dt.day
        day_counts = df_valid.groupby('day').size().sort_values(ascending=False)
        
        if asking_most:
            top_day = day_counts.index[0]
            top_count = day_counts.iloc[0]
            
            actual_dates = df_valid[df_valid['day'] == top_day].groupby(df_valid[date_col].dt.date).size().sort_values(ascending=False)
            
            report = f"# 📊 {t('most_events', lang)} - {'Date' if lang == 'en' else 'Tanggal'}\n\n"
            report += f"**Day {top_day}** of the month had most events: **{top_count:,} events total**\n\n"
            report += f"## 📅 Specific dates with day {top_day}:\n\n"
            for date, count in actual_dates.head(10).items():
                report += f"• **{date}**: {count:,} events\n"
            report += "\n"
            
        elif asking_least:
            least_day = day_counts.index[-1]
            least_count = day_counts.iloc[-1]
            
            actual_dates = df_valid[df_valid['day'] == least_day].groupby(df_valid[date_col].dt.date).size().sort_values(ascending=False)
            
            report = f"# 📊 {t('least_events', lang)} - {'Date' if lang == 'en' else 'Tanggal'}\n\n"
            report += f"**Day {least_day}** of the month had least events: **{least_count:,} events total**\n\n"
            report += f"## 📅 Specific dates with day {least_day}:\n\n"
            for date, count in actual_dates.items():
                report += f"• **{date}**: {count:,} events\n"
            report += "\n"
        else:
            report = f"# 📊 {t('total_events', lang)} by Day of Month\n\n"
        
        report += f"## 📊 Top 10 Days:\n\n"
        for i, (day, count) in enumerate(day_counts.head(10).items(), 1):
            pct = (count / len(df_valid) * 100)
            report += f"{i}. **Day {day}**: {count:,} events ({pct:.1f}%)\n"
    
    # DAY OF WEEK ANALYSIS  
    elif asking_day:
        df_valid['dow'] = df_valid[date_col].dt.dayofweek
        dow_counts = df_valid.groupby('dow').size().sort_values(ascending=False)
        
        day_names = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 
                    4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
        
        if asking_most:
            top_dow = dow_counts.index[0]
            top_count = dow_counts.iloc[0]
            
            top_dates = df_valid[df_valid['dow'] == top_dow].groupby(df_valid[date_col].dt.date).size().sort_values(ascending=False)
            
            report = f"# 📊 {t('most_events', lang)} - Day of Week\n\n"
            report += f"**{day_names[top_dow]}** had the most events: **{top_count:,} events**\n\n"
            report += f"## 📅 Top {day_names[top_dow]} dates:\n\n"
            for date, count in top_dates.head(10).items():
                report += f"• **{date}**: {count:,} events\n"
            report += "\n"
            
        elif asking_least:
            least_dow = dow_counts.index[-1]
            least_count = dow_counts.iloc[-1]
            
            least_dates = df_valid[df_valid['dow'] == least_dow].groupby(df_valid[date_col].dt.date).size().sort_values(ascending=False)
            
            report = f"# 📊 {t('least_events', lang)} - Day of Week\n\n"
            report += f"**{day_names[least_dow]}** had the least events: **{least_count:,} events**\n\n"
            report += f"## 📅 {day_names[least_dow]} dates:\n\n"
            for date, count in least_dates.items():
                report += f"• **{date}**: {count:,} events\n"
            report += "\n"
        else:
            report = f"# 📊 {t('total_events', lang)} by Day of Week\n\n"
        
        report += f"## 📊 Weekly Ranking:\n\n"
        for i, (dow, count) in enumerate(dow_counts.items(), 1):
            pct = (count / len(df_valid) * 100)
            report += f"{i}. **{day_names[dow]}**: {count:,} events ({pct:.1f}%)\n"
    
    if report:
        print(f"  ✅ Temporal analysis complete")
        return report
    
    return None