import pandas as pd
from utils.translations import detect_language, t

def detect_anomalies_enhanced(df_filtered, date_col, equipment_name=None, lang='en'):
    """Enhanced anomaly detection with detailed analysis - BILINGUAL"""
    
    if not date_col or date_col not in df_filtered.columns:
        return None
    
    df_valid = df_filtered[df_filtered[date_col].notna()].copy()
    if len(df_valid) < 7:
        return {
            'detected': False,
            'reason': 'insufficient_data',
            'message': 'Need at least 7 days of data for anomaly analysis' if lang == 'en' else 'Membutuhkan minimal 7 hari data untuk analisis anomali'
        }
    
    df_valid['date_only'] = df_valid[date_col].dt.date
    df_valid['hour'] = df_valid[date_col].dt.hour
    df_valid['day_of_week'] = df_valid[date_col].dt.dayofweek
    
    daily_counts = df_valid.groupby('date_only').size()
    
    # Statistical parameters
    mean = daily_counts.mean()
    std = daily_counts.std()
    median = daily_counts.median()
    q1 = daily_counts.quantile(0.25)
    q3 = daily_counts.quantile(0.75)
    iqr = q3 - q1
    
    # Multiple anomaly detection methods
    
    # Method 1: Z-score (2.5 sigma)
    zscore_threshold = mean + (2.5 * std)
    zscore_anomalies = daily_counts[daily_counts > zscore_threshold]
    
    # Method 2: IQR (Interquartile Range)
    iqr_upper = q3 + (1.5 * iqr)
    iqr_anomalies = daily_counts[daily_counts > iqr_upper]
    
    # Method 3: Percentage spike (3x median)
    spike_threshold = median * 3
    spike_anomalies = daily_counts[daily_counts > spike_threshold]
    
    # Combine all methods
    all_anomaly_dates = set()
    all_anomaly_dates.update(zscore_anomalies.index)
    all_anomaly_dates.update(iqr_anomalies.index)
    all_anomaly_dates.update(spike_anomalies.index)
    
    if len(all_anomaly_dates) == 0:
        return {
            'detected': False,
            'reason': 'no_anomalies',
            'message': 'No anomalies detected in the data' if lang == 'en' else 'Tidak ada anomali terdeteksi dalam data',
            'statistics': {
                'mean': round(mean, 2),
                'median': round(median, 2),
                'std': round(std, 2),
                'min': int(daily_counts.min()),
                'max': int(daily_counts.max())
            }
        }
    
    # Detailed anomaly analysis
    anomaly_details = []
    for date in sorted(all_anomaly_dates):
        count = daily_counts[date]
        df_anomaly_day = df_valid[df_valid['date_only'] == date]
        
        # Analyze what caused the anomaly
        hourly_dist = df_anomaly_day['hour'].value_counts()
        peak_hour = hourly_dist.idxmax() if len(hourly_dist) > 0 else None
        
        # Calculate severity
        zscore = (count - mean) / std if std > 0 else 0
        if zscore > 3:
            severity = 'critical'
        elif zscore > 2.5:
            severity = 'high'
        elif zscore > 2:
            severity = 'medium'
        else:
            severity = 'low'
        
        anomaly_details.append({
            'date': str(date),
            'count': int(count),
            'expected_range': f"{int(mean - std)}-{int(mean + std)}",
            'deviation': round(((count - mean) / mean * 100), 1),
            'zscore': round(zscore, 2),
            'severity': severity,
            'peak_hour': int(peak_hour) if peak_hour is not None else None,
            'day_of_week': df_anomaly_day['day_of_week'].iloc[0]
        })
    
    # Pattern analysis
    day_names_en = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    day_names_id = ['Senin', 'Selasa', 'Rabu', 'Kamis', 'Jumat', 'Sabtu', 'Minggu']
    day_names = day_names_en if lang == 'en' else day_names_id
    
    anomaly_days = [detail['day_of_week'] for detail in anomaly_details]
    most_common_day = max(set(anomaly_days), key=anomaly_days.count) if anomaly_days else None
    
    return {
        'detected': True,
        'count': len(anomaly_details),
        'anomalies': sorted(anomaly_details, key=lambda x: x['zscore'], reverse=True),
        'statistics': {
            'mean': round(mean, 2),
            'median': round(median, 2),
            'std': round(std, 2),
            'threshold_zscore': round(zscore_threshold, 2),
            'threshold_iqr': round(iqr_upper, 2),
            'min': int(daily_counts.min()),
            'max': int(daily_counts.max())
        },
        'patterns': {
            'most_common_day': day_names[most_common_day] if most_common_day is not None else None,
            'total_anomaly_events': sum([a['count'] for a in anomaly_details]),
            'avg_anomaly_size': round(sum([a['count'] for a in anomaly_details]) / len(anomaly_details), 2)
        }
    }


def build_anomaly_report(anomalies, equipment_name=None, filter_desc=None, lang='en'):
    """Build comprehensive anomaly report - BILINGUAL"""
    
    if not anomalies or not anomalies.get('detected'):
        title = f"{'Anomaly' if lang == 'en' else 'Anomali'} {equipment_name}" if equipment_name else t('anomaly_detected', lang).replace(' Detected', '').replace(' Terdeteksi', '')
        if filter_desc:
            title += f" ({filter_desc})"
        
        report = f"# 🔍 {title}\n\n"
        
        if anomalies and anomalies.get('reason') == 'insufficient_data':
            report += f"⚠️ **{anomalies['message']}**\n\n"
        else:
            report += f"✅ **{'No anomalies detected' if lang == 'en' else 'Tidak ada anomali terdeteksi'}**\n\n"
            if anomalies and 'statistics' in anomalies:
                stats = anomalies['statistics']
                if lang == 'en':
                    report += f"Data shows consistent patterns:\n"
                    report += f"• Average: {stats['mean']} events/day\n"
                    report += f"• Median: {stats['median']} events/day\n"
                    report += f"• Normal range: {stats['min']}-{stats['max']} events/day\n"
                else:
                    report += f"Data menunjukkan pola yang konsisten:\n"
                    report += f"• Rata-rata: {stats['mean']} events/hari\n"
                    report += f"• Median: {stats['median']} events/hari\n"
                    report += f"• Range normal: {stats['min']}-{stats['max']} events/hari\n"
        
        return report
    
    title = f"{'Anomalies Detected' if lang == 'en' else 'Anomali Terdeteksi'}"
    if equipment_name:
        title += f" - {equipment_name}"
    if filter_desc:
        title += f" ({filter_desc})"
    
    report = f"# ⚠️ {title}\n\n"
    
    stats = anomalies['statistics']
    patterns = anomalies['patterns']
    
    # Executive Summary
    report += f"## 📊 {t('executive_summary', lang)}\n\n"
    
    if lang == 'en':
        report += f"**Total anomalies detected:** {anomalies['count']} days\n\n"
        report += f"**Normal Statistics:**\n"
        report += f"• Daily average: {stats['mean']} events\n"
        report += f"• Median: {stats['median']} events\n"
        report += f"• Standard deviation: {stats['std']}\n"
        report += f"• Anomaly threshold: {stats['threshold_zscore']} events\n\n"
        report += f"**Total anomaly events:** {patterns['total_anomaly_events']} events ({patterns['avg_anomaly_size']} avg per day)\n"
        if patterns.get('most_common_day'):
            report += f"**Most common day:** {patterns['most_common_day']}\n"
    else:
        report += f"**Total anomali terdeteksi:** {anomalies['count']} hari\n\n"
        report += f"**Statistik Normal:**\n"
        report += f"• Rata-rata harian: {stats['mean']} events\n"
        report += f"• Median: {stats['median']} events\n"
        report += f"• Standar deviasi: {stats['std']}\n"
        report += f"• Threshold anomali: {stats['threshold_zscore']} events\n\n"
        report += f"**Total events anomali:** {patterns['total_anomaly_events']} events ({patterns['avg_anomaly_size']} avg per hari)\n"
        if patterns.get('most_common_day'):
            report += f"**Hari tersering:** {patterns['most_common_day']}\n"
    
    report += f"\n---\n\n"
    
    # Detailed Anomaly List
    report += f"## 🔴 {'Detailed Anomaly List' if lang == 'en' else 'Daftar Anomali Detail'}\n\n"
    
    # Group by severity
    critical = [a for a in anomalies['anomalies'] if a['severity'] == 'critical']
    high = [a for a in anomalies['anomalies'] if a['severity'] == 'high']
    medium = [a for a in anomalies['anomalies'] if a['severity'] == 'medium']
    low = [a for a in anomalies['anomalies'] if a['severity'] == 'low']
    
    day_names_en = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    day_names_id = ['Sen', 'Sel', 'Rab', 'Kam', 'Jum', 'Sab', 'Min']
    day_names = day_names_en if lang == 'en' else day_names_id
    
    if critical:
        report += f"### 🚨 Critical (Z-score > 3.0)\n\n"
        for a in critical:
            report += f"**{a['date']}** ({'Day' if lang == 'en' else 'Hari'}: {day_names[a['day_of_week']]})\n"
            if lang == 'en':
                report += f"• **{a['count']} events** (normal: {a['expected_range']})\n"
                report += f"• Deviation: **+{a['deviation']}%** from average\n"
                report += f"• Z-score: {a['zscore']}\n"
                if a['peak_hour'] is not None:
                    report += f"• Peak hour: {a['peak_hour']}:00\n"
            else:
                report += f"• **{a['count']} events** (normal: {a['expected_range']})\n"
                report += f"• Deviasi: **+{a['deviation']}%** dari rata-rata\n"
                report += f"• Z-score: {a['zscore']}\n"
                if a['peak_hour'] is not None:
                    report += f"• Peak hour: {a['peak_hour']}:00\n"
            report += f"\n"
    
    if high:
        report += f"### ⚠️ High (Z-score 2.5-3.0)\n\n"
        for a in high:
            report += f"**{a['date']}**: {a['count']} events (+{a['deviation']}%) | Peak: {a['peak_hour']}:00\n"
    
    if medium or low:
        report += f"\n### 📊 {'Medium/Low Anomalies' if lang == 'en' else 'Anomali Medium/Low'}\n\n"
        for a in (medium + low):
            report += f"• {a['date']}: {a['count']} events (+{a['deviation']}%)\n"
    
    # Insights & Recommendations
    report += f"\n---\n\n"
    report += f"## 💡 {t('insights', lang)} & {t('recommendation', lang)}\n\n"
    
    # Analyze patterns
    avg_deviation = sum([a['deviation'] for a in anomalies['anomalies']]) / len(anomalies['anomalies'])
    max_anomaly = max(anomalies['anomalies'], key=lambda x: x['count'])
    
    if lang == 'en':
        report += f"1. **Largest Spike:** {max_anomaly['date']} with {max_anomaly['count']} events (+{max_anomaly['deviation']}%)\n"
        report += f"2. **Average Deviation:** +{avg_deviation:.1f}% from normal\n"
        
        if len(critical) > 0:
            report += f"3. **Critical Alert:** {len(critical)} days with critical spikes (>3 sigma)\n"
        
        # Peak hour analysis
        peak_hours = [a['peak_hour'] for a in anomalies['anomalies'] if a['peak_hour'] is not None]
        if peak_hours:
            most_common_hour = max(set(peak_hours), key=peak_hours.count)
            report += f"4. **Most Common Hour:** Anomalies often occur around {most_common_hour}:00\n"
        
        report += f"\n**Recommendations:**\n"
        report += f"• Investigate root causes on critical dates\n"
        report += f"• Review maintenance schedule and operational patterns\n"
        if patterns.get('most_common_day'):
            report += f"• Pay special attention on {patterns['most_common_day']}\n"
        report += f"• Set up monitoring alerts to prevent similar anomalies\n"
    else:
        report += f"1. **Lonjakan Terbesar:** {max_anomaly['date']} dengan {max_anomaly['count']} events (+{max_anomaly['deviation']}%)\n"
        report += f"2. **Rata-rata Deviasi:** +{avg_deviation:.1f}% dari normal\n"
        
        if len(critical) > 0:
            report += f"3. **Critical Alert:** Terdapat {len(critical)} hari dengan lonjakan kritis (>3 sigma)\n"
        
        # Peak hour analysis
        peak_hours = [a['peak_hour'] for a in anomalies['anomalies'] if a['peak_hour'] is not None]
        if peak_hours:
            most_common_hour = max(set(peak_hours), key=peak_hours.count)
            report += f"4. **Jam Tersering:** Anomali sering terjadi sekitar jam {most_common_hour}:00\n"
        
        report += f"\n**Rekomendasi:**\n"
        report += f"• Investigasi penyebab lonjakan pada tanggal-tanggal critical\n"
        report += f"• Review maintenance schedule dan operational pattern\n"
        if patterns.get('most_common_day'):
            report += f"• Perhatikan khusus hari {patterns['most_common_day']}\n"
        report += f"• Set up monitoring alerts untuk mencegah anomali serupa\n"
    
    return report


def handle_anomaly_query(query, df, key_columns):
    """Dedicated handler for anomaly detection queries - BILINGUAL"""
    
    print("  🔍 ANOMALY DETECTION MODE")
    
    # Detect language
    lang = detect_language(query)
    
    # Import filter service
    from services.filter_service import extract_filters_from_query
    
    # Extract filters
    df_filtered, filters, filter_descriptions = extract_filters_from_query(query, df, key_columns)
    
    print(f"  📊 Filtered data: {len(df_filtered)} events")
    print(f"  🔧 Filters applied: {filters}")
    print(f"  🌐 Language: {lang}")
    
    date_col = key_columns.get('date')
    if not date_col:
        msg = "❌ Cannot perform anomaly analysis: date column not found" if lang == 'en' else "❌ Tidak dapat melakukan analisis anomali: kolom tanggal tidak ditemukan"
        return msg, 0, 0, None
    
    # Identify equipment if specified
    equipment_name = None
    if filters.get('identifier'):
        equipment_name = filters['identifier']
    elif filters.get('identifiers'):
        equipment_name = ', '.join(filters['identifiers'])
    
    # Build filter description
    filter_desc = ', '.join(filter_descriptions) if filter_descriptions else None
    
    print(f"  🎯 Equipment: {equipment_name if equipment_name else 'ALL'}")
    print(f"  📝 Filter desc: {filter_desc}")
    
    # Perform anomaly detection
    anomalies = detect_anomalies_enhanced(df_filtered, date_col, equipment_name, lang)
    
    # Build report with language support
    report = build_anomaly_report(anomalies, equipment_name, filter_desc, lang)
    
    # Generate visualization
    from services.graph_service import generate_xy_graph_data
    
    graph_data = None
    if len(df_filtered) > 0:
        title_parts = [t('anomaly_detected', lang).replace(' Detected', '').replace(' Terdeteksi', '')]
        if equipment_name:
            title_parts.append(f"- {equipment_name}")
        if filter_desc:
            title_parts.append(f"({filter_desc})")
        
        title = " ".join(title_parts)
        
        # Use simple graph generation
        graph_data = generate_xy_graph_data(df_filtered, date_col, title)
        
        # Add anomaly markers ONLY if anomalies detected
        if graph_data and anomalies and anomalies.get('detected') and anomalies.get('anomalies'):
            anomaly_dates = [a['date'] for a in anomalies['anomalies']]
            graph_data['anomaly_dates'] = anomaly_dates
            graph_data['anomaly_info'] = {
                'count': len(anomaly_dates),
                'severity_distribution': {
                    'critical': len([a for a in anomalies['anomalies'] if a['severity'] == 'critical']),
                    'high': len([a for a in anomalies['anomalies'] if a['severity'] == 'high']),
                    'medium': len([a for a in anomalies['anomalies'] if a['severity'] == 'medium']),
                    'low': len([a for a in anomalies['anomalies'] if a['severity'] == 'low'])
                }
            }
            print(f"  ⚠️ Added {len(anomaly_dates)} anomaly markers to graph")
    
    # Calculate confidence
    confidence = 100 if anomalies and anomalies.get('detected') else 90
    if len(df_filtered) < 30:
        confidence -= 20
    
    print(f"  ✅ Anomaly analysis complete. Detected: {anomalies.get('detected', False)}")
    
    return report, len(df_filtered), confidence, graph_data