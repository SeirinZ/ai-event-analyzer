"""
Anomaly Detection Service - WITH RECOMMENDATIONS
"""
import pandas as pd
from utils.translations import t

class AnomalyService:
    """Handles anomaly detection"""
    
    def __init__(self, key_columns):
        self.key_columns = key_columns
    
    def detect_anomalies(self, df_filtered, lang='en'):
        """Detect anomalies in time series data"""
        
        date_col = self.key_columns.get('date')
        if not date_col or date_col not in df_filtered.columns:
            return {
                'detected': False, 
                'reason': 'no_date_column',
                'message': 'Date column not found' if lang == 'en' else 'Kolom tanggal tidak ditemukan'
            }
        
        df_valid = df_filtered[df_filtered[date_col].notna()].copy()
        if len(df_valid) < 7:
            return {
                'detected': False, 
                'reason': 'insufficient_data',
                'message': 'Need at least 7 days of data' if lang == 'en' else 'Membutuhkan minimal 7 hari data'
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
                'message': 'No anomalies detected' if lang == 'en' else 'Tidak ada anomali terdeteksi',
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
            }
        }
    
    def build_anomaly_report(self, anomalies, equipment_name=None, filter_desc=None, lang='en'):
        """Build comprehensive anomaly report WITH RECOMMENDATIONS"""
        
        title = f"{t('anomaly_detected', lang)}"
        if equipment_name:
            title += f" - {equipment_name}"
        if filter_desc:
            title += f" ({filter_desc})"
        
        if not anomalies or not anomalies.get('detected'):
            report = f"# ✅ {title}\n\n"
            
            if anomalies and anomalies.get('reason') == 'insufficient_data':
                report += f"⚠️ **{anomalies['message']}**\n\n"
            else:
                report += f"✅ **{anomalies.get('message', 'No anomalies detected')}**\n\n"
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
        
        report = f"# ⚠️ {title}\n\n"
        
        stats = anomalies['statistics']
        
        # Executive Summary
        report += f"## 📊 {t('executive_summary', lang)}\n\n"
        
        if lang == 'en':
            report += f"**Total anomalies detected:** {anomalies['count']} days\n\n"
            report += f"**Normal Statistics:**\n"
            report += f"• Daily average: {stats['mean']} events\n"
            report += f"• Median: {stats['median']} events\n"
            report += f"• Standard deviation: {stats['std']}\n"
            report += f"• Anomaly threshold: {stats['threshold_zscore']} events\n\n"
        else:
            report += f"**Total anomali terdeteksi:** {anomalies['count']} hari\n\n"
            report += f"**Statistik Normal:**\n"
            report += f"• Rata-rata harian: {stats['mean']} events\n"
            report += f"• Median: {stats['median']} events\n"
            report += f"• Standar deviasi: {stats['std']}\n"
            report += f"• Threshold anomali: {stats['threshold_zscore']} events\n\n"
        
        report += f"---\n\n"
        
        # Detailed Anomaly List
        report += f"## 🔴 {'Detailed Anomaly List' if lang == 'en' else 'Daftar Anomali Detail'}\n\n"
        
        # Group by severity
        critical = [a for a in anomalies['anomalies'] if a['severity'] == 'critical']
        high = [a for a in anomalies['anomalies'] if a['severity'] == 'high']
        medium = [a for a in anomalies['anomalies'] if a['severity'] == 'medium']
        low = [a for a in anomalies['anomalies'] if a['severity'] == 'low']
        
        from utils.helpers import get_day_name
        
        if critical:
            report += f"### 🚨 Critical (Z-score > 3.0)\n\n"
            for a in critical:
                day_name = get_day_name(a['day_of_week'], lang)
                report += f"**{a['date']}** ({'Day' if lang == 'en' else 'Hari'}: {day_name})\n"
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
                report += f"**{a['date']}**: {a['count']} events (+{a['deviation']}%)"
                if a['peak_hour']:
                    report += f" | Peak: {a['peak_hour']}:00"
                report += "\n"
        
        if medium or low:
            report += f"\n### 📊 {'Medium/Low Anomalies' if lang == 'en' else 'Anomali Medium/Low'}\n\n"
            for a in (medium + low):
                report += f"• {a['date']}: {a['count']} events (+{a['deviation']}%)\n"
        
        # Insights
        report += f"\n---\n\n"
        report += f"## 💡 {t('insights', lang)} & Analysis\n\n"
        
        max_anomaly = max(anomalies['anomalies'], key=lambda x: x['count'])
        avg_deviation = sum([a['deviation'] for a in anomalies['anomalies']]) / len(anomalies['anomalies'])
        
        if lang == 'en':
            report += f"1. **Largest Spike:** {max_anomaly['date']} with {max_anomaly['count']} events (+{max_anomaly['deviation']}%)\n"
            report += f"2. **Average Deviation:** +{avg_deviation:.1f}% from normal\n"
            
            if len(critical) > 0:
                report += f"3. **Critical Alert:** {len(critical)} days with critical spikes (>3 sigma) - **IMMEDIATE ACTION REQUIRED**\n"
            
            # Peak hour analysis
            peak_hours = [a['peak_hour'] for a in anomalies['anomalies'] if a['peak_hour'] is not None]
            if peak_hours:
                from collections import Counter
                most_common_hour = Counter(peak_hours).most_common(1)[0][0]
                report += f"4. **Pattern Detected:** Anomalies frequently occur around **{most_common_hour}:00** - possible operational pattern\n"
        else:
            report += f"1. **Lonjakan Terbesar:** {max_anomaly['date']} dengan {max_anomaly['count']} events (+{max_anomaly['deviation']}%)\n"
            report += f"2. **Rata-rata Deviasi:** +{avg_deviation:.1f}% dari normal\n"
            
            if len(critical) > 0:
                report += f"3. **Critical Alert:** Terdapat {len(critical)} hari dengan lonjakan kritis (>3 sigma) - **TINDAKAN SEGERA DIPERLUKAN**\n"
            
            # Peak hour analysis
            peak_hours = [a['peak_hour'] for a in anomalies['anomalies'] if a['peak_hour'] is not None]
            if peak_hours:
                from collections import Counter
                most_common_hour = Counter(peak_hours).most_common(1)[0][0]
                report += f"4. **Pola Terdeteksi:** Anomali sering terjadi sekitar jam **{most_common_hour}:00** - kemungkinan pola operasional\n"
        
        # RECOMMENDATIONS - ACTIONABLE & DETAILED
        report += f"\n---\n\n"
        report += f"## 📌 {t('corrective_actions', lang)}\n\n"
        
        if len(critical) > 0:
            if lang == 'en':
                report += f"### 🚨 URGENT ACTIONS (Critical Anomalies Detected)\n\n"
                report += f"**Immediate (Next 24 hours):**\n"
                report += f"1. **Emergency Investigation**\n"
                report += f"   - Form incident response team immediately\n"
                report += f"   - Review detailed logs for {critical[0]['date']}\n"
                report += f"   - Interview operators on duty during spikes\n"
                report += f"   - Check equipment condition and sensor status\n\n"
                
                report += f"2. **Temporary Controls**\n"
                report += f"   - Implement 24/7 monitoring on affected equipment\n"
                report += f"   - Set up immediate escalation procedures\n"
                report += f"   - Consider operational restrictions if safety risk\n"
                report += f"   - Document all anomaly occurrences in real-time\n\n"
                
                report += f"**Short-term (Next 7 days):**\n"
                report += f"3. **Root Cause Analysis**\n"
                report += f"   - Conduct formal RCA using 5-Why or Fishbone method\n"
                report += f"   - Analyze correlation with process parameters\n"
                report += f"   - Review maintenance records and recent changes\n"
                report += f"   - Test equipment under controlled conditions\n\n"
                
                report += f"4. **Corrective Measures**\n"
                report += f"   - Schedule emergency maintenance if hardware issue\n"
                report += f"   - Adjust process parameters if operational issue\n"
                report += f"   - Update alarm thresholds based on findings\n"
                report += f"   - Retrain operators on proper procedures\n\n"
                
                report += f"**Long-term (Next 30 days):**\n"
                report += f"5. **Preventive Strategy**\n"
                report += f"   - Implement predictive maintenance program\n"
                report += f"   - Enhance monitoring systems with advanced analytics\n"
                report += f"   - Update Standard Operating Procedures (SOPs)\n"
                report += f"   - Conduct lessons learned session with team\n\n"
                
                report += f"6. **Continuous Improvement**\n"
                report += f"   - Set up weekly anomaly review meetings\n"
                report += f"   - Create dashboards for real-time monitoring\n"
                report += f"   - Establish KPIs for anomaly reduction\n"
                report += f"   - Share findings with management and stakeholders\n"
            else:
                report += f"### 🚨 TINDAKAN MENDESAK (Anomali Critical Terdeteksi)\n\n"
                report += f"**Segera (24 jam ke depan):**\n"
                report += f"1. **Investigasi Darurat**\n"
                report += f"   - Bentuk tim respons insiden segera\n"
                report += f"   - Review log detail untuk {critical[0]['date']}\n"
                report += f"   - Wawancara operator yang bertugas saat lonjakan\n"
                report += f"   - Cek kondisi equipment dan status sensor\n\n"
                
                report += f"2. **Kontrol Sementara**\n"
                report += f"   - Implementasi monitoring 24/7 pada equipment terdampak\n"
                report += f"   - Setup prosedur eskalasi langsung\n"
                report += f"   - Pertimbangkan pembatasan operasional jika ada risiko safety\n"
                report += f"   - Dokumentasi semua kejadian anomali secara real-time\n\n"
                
                report += f"**Jangka Pendek (7 hari ke depan):**\n"
                report += f"3. **Root Cause Analysis**\n"
                report += f"   - Lakukan RCA formal menggunakan 5-Why atau Fishbone\n"
                report += f"   - Analisis korelasi dengan parameter proses\n"
                report += f"   - Review catatan maintenance dan perubahan terkini\n"
                report += f"   - Test equipment dalam kondisi terkontrol\n\n"
                
                report += f"4. **Tindakan Perbaikan**\n"
                report += f"   - Jadwalkan emergency maintenance jika ada masalah hardware\n"
                report += f"   - Sesuaikan parameter proses jika masalah operasional\n"
                report += f"   - Update threshold alarm berdasarkan temuan\n"
                report += f"   - Retrain operator tentang prosedur yang benar\n\n"
                
                report += f"**Jangka Panjang (30 hari ke depan):**\n"
                report += f"5. **Strategi Preventif**\n"
                report += f"   - Implementasi program predictive maintenance\n"
                report += f"   - Tingkatkan sistem monitoring dengan advanced analytics\n"
                report += f"   - Update Standard Operating Procedures (SOPs)\n"
                report += f"   - Lakukan sesi lessons learned dengan tim\n\n"
                
                report += f"6. **Continuous Improvement**\n"
                report += f"   - Setup weekly anomaly review meetings\n"
                report += f"   - Buat dashboards untuk real-time monitoring\n"
                report += f"   - Tetapkan KPI untuk pengurangan anomali\n"
                report += f"   - Share temuan dengan management dan stakeholders\n"
        
        elif len(high) > 0:
            if lang == 'en':
                report += f"### ⚠️ PRIORITY ACTIONS (High Anomalies Detected)\n\n"
                report += f"**This Week:**\n"
                report += f"1. Schedule detailed investigation of anomaly dates\n"
                report += f"2. Increase monitoring frequency temporarily\n"
                report += f"3. Review and document patterns observed\n"
                report += f"4. Plan preventive maintenance within 2 weeks\n\n"
                
                report += f"**This Month:**\n"
                report += f"5. Conduct equipment health assessment\n"
                report += f"6. Review and optimize alarm settings\n"
                report += f"7. Update operator training materials\n"
                report += f"8. Implement enhanced monitoring procedures\n"
            else:
                report += f"### ⚠️ TINDAKAN PRIORITAS (Anomali High Terdeteksi)\n\n"
                report += f"**Minggu Ini:**\n"
                report += f"1. Jadwalkan investigasi detail pada tanggal anomali\n"
                report += f"2. Tingkatkan frekuensi monitoring sementara\n"
                report += f"3. Review dan dokumentasi pola yang diamati\n"
                report += f"4. Rencanakan preventive maintenance dalam 2 minggu\n\n"
                
                report += f"**Bulan Ini:**\n"
                report += f"5. Lakukan health assessment equipment\n"
                report += f"6. Review dan optimasi setting alarm\n"
                report += f"7. Update material training operator\n"
                report += f"8. Implementasi prosedur monitoring yang lebih baik\n"
        else:
            if lang == 'en':
                report += f"### ✅ STANDARD MONITORING (Low/Medium Anomalies)\n\n"
                report += f"1. Continue regular monitoring schedule\n"
                report += f"2. Review anomaly trends in monthly reports\n"
                report += f"3. Document observations for future reference\n"
                report += f"4. Consider fine-tuning alarm thresholds\n"
                report += f"5. Share findings in routine team meetings\n"
            else:
                report += f"### ✅ MONITORING STANDAR (Anomali Low/Medium)\n\n"
                report += f"1. Lanjutkan jadwal monitoring regular\n"
                report += f"2. Review trend anomali dalam laporan bulanan\n"
                report += f"3. Dokumentasi observasi untuk referensi masa depan\n"
                report += f"4. Pertimbangkan fine-tuning threshold alarm\n"
                report += f"5. Share temuan dalam meeting tim rutin\n"
        
        return report