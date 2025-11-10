"""
Query Router - Routes queries to appropriate services
"""
from utils.translations import detect_language
from utils.helpers import detect_query_intent, extract_equipment_codes, calculate_confidence

class QueryRouter:
    """Routes queries to appropriate service handlers"""
    
    def __init__(self, df, key_columns, services):
        self.df = df
        self.key_columns = key_columns
        self.services = services
    
    def route(self, query):
        """Route query to appropriate handler"""
        
        q = query.lower()
        lang = detect_language(query)
        intents = detect_query_intent(query)
        
        print(f"  🎯 Intents: {intents}, Language: {lang}")
        
        # Priority 1: Temporal Analysis
        if self._is_temporal_query(q):
            print("  📊 Route: TEMPORAL_ANALYSIS")
            result = self.services['temporal'].handle_temporal_query(query, lang)
            if result:
                return {
                    "answer": result,
                    "method": "temporal_analysis",
                    "confidence": 95,
                    "graph_data": None,
                    "count": len(self.df)
                }
        
        # Priority 2: Anomaly Detection
        if 'anomaly' in intents:
            print("  ⚠️ Route: ANOMALY_DETECTION")
            from services.filter_service import FilterService
            filter_service = FilterService(self.df, self.key_columns)
            df_filtered, filters, filter_descriptions = filter_service.extract_filters_from_query(query)
            
            # Detect equipment name for report
            equipment_name = None
            if filters.get('identifier'):
                equipment_name = filters['identifier']
            
            filter_desc = ', '.join(filter_descriptions) if filter_descriptions else None
            
            anomalies = self.services['anomaly'].detect_anomalies(df_filtered, lang)
            report = self.services['anomaly'].build_anomaly_report(anomalies, equipment_name, filter_desc, lang)
            
            # Generate graph with anomaly markers
            graph_data = None
            if len(df_filtered) > 0:
                title_parts = ["Anomaly Detection"]
                if equipment_name:
                    title_parts.append(f"- {equipment_name}")
                if filter_desc:
                    title_parts.append(f"({filter_desc})")
                
                title = " ".join(title_parts)
                graph_data = self.services['graph'].generate_xy_graph_data(df_filtered, title)
                
                # Add anomaly markers if detected
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
            
            return {
                "answer": report,
                "method": "anomaly_detection",
                "confidence": 90,
                "graph_data": graph_data,
                "count": len(df_filtered)
            }
        
        # Priority 3: Comparison with Trend/Graph
        comparison_keywords = ['bandingkan', 'vs', 'versus', 'compare', 'perbandingan']
        has_trend_request = self.services['graph'].should_generate_graph(query)
        
        if any(kw in q for kw in comparison_keywords):
            print("  📊 Route: COMPARISON")
            from services.filter_service import FilterService
            filter_service = FilterService(self.df, self.key_columns)
            df_filtered, filters, filter_descriptions = filter_service.extract_filters_from_query(query)
            
            result = self.services['comparison'].handle_comparison_query(query, filters)
            
            if result:
                graph_data = None
                
                # Generate comparison graph if trend requested
                if has_trend_request:
                    print("  📈 Generating comparison graph...")
                    found_equipments = extract_equipment_codes(query)
                    
                    if len(found_equipments) >= 2:
                        graph_data = self.services['graph'].generate_comparison_graph_data(
                            self.df, 
                            found_equipments, 
                            filters
                        )
                        
                        if graph_data:
                            print(f"  ✅ Comparison graph generated: {graph_data['title']}")
                
                return {
                    "answer": result,
                    "method": "comparison_with_trend" if graph_data else "comparison",
                    "confidence": 90,
                    "graph_data": graph_data,
                    "count": len(self.df)
                }
        
        # Priority 4: Identifier Search
        if self.services['identifier'].is_identifier_query(query):
            print("  🔍 Route: IDENTIFIER_SEARCH")
            from services.filter_service import FilterService
            filter_service = FilterService(self.df, self.key_columns)
            _, filters, _ = filter_service.extract_filters_from_query(query)
            
            data, explanation, confidence = self.services['identifier'].search_by_identifier(query, filters)
            
            if data is not None:
                # Generate graph if requested
                graph_data = None
                if has_trend_request and explanation:
                    # Extract equipment code from explanation
                    import re
                    match = re.search(r'`([A-Z]{2,}-?\d+[A-Z]*)`', explanation)
                    if match:
                        equipment_code = match.group(1)
                        from services.filter_service import FilterService
                        filter_service = FilterService(self.df, self.key_columns)
                        df_filtered, _, _ = filter_service.extract_filters_from_query(query)
                        
                        if len(df_filtered) > 0:
                            graph_data = self.services['graph'].generate_xy_graph_data(
                                df_filtered, 
                                f"Timeline {equipment_code}"
                            )
                
                return {
                    "answer": explanation,
                    "method": "identifier_search",
                    "confidence": confidence,
                    "graph_data": graph_data
                }
        
        # Priority 5: LLM Analysis (with trend support)
        if self._is_data_query(q):
            print("  🧠 Route: LLM_ANALYSIS")
            return self._handle_llm_analysis(query, lang)
        
        # Priority 6: General LLM
        print("  💬 Route: GENERAL_LLM")
        answer = self.services['llm'].answer_general_question(query, lang)
        return {
            "answer": answer,
            "method": "general_llm",
            "confidence": 100,
            "graph_data": None
        }
    
    def _is_temporal_query(self, q):
        """Check if temporal analysis query"""
        temporal_keywords = ['event terbanyak', 'event tersedikit', 'most events', 'least events']
        temporal_time = ['bulan apa', 'tanggal berapa', 'hari apa', 'which month', 'which date']
        return any(tk in q for tk in temporal_keywords) and any(tt in q for tt in temporal_time)
    
    def _is_data_query(self, q):
        """Check if data analysis query"""
        data_keywords = [
            'berapa', 'jumlah', 'rata-rata', 'paling', 'event', 'bulan',
            'how many', 'total', 'average', 'most', 'least', 'show', 'list'
        ]
        return any(kw in q for kw in data_keywords)
    
    def _handle_llm_analysis(self, query, lang):
        """Handle LLM analysis with graph support"""
        from services.filter_service import FilterService
        
        filter_service = FilterService(self.df, self.key_columns)
        df_filtered, filters, filter_descriptions = filter_service.extract_filters_from_query(query)
        
        # Build context
        context = filter_service.build_context(query, df_filtered, filters, filter_descriptions, lang)
        
        # Generate prompt and call LLM
        prompt = self.services['llm'].get_enhanced_llm_prompt(query, context, lang)
        answer = self.services['llm'].call_llm(prompt, temperature=0.2)
        
        # Clean up answer
        cleanup_phrases = {
            'en': ["Answer:", "ANSWER:", "Response:"],
            'id': ["Jawaban:", "JAWABAN:", "Respons:"]
        }
        
        for phrase in cleanup_phrases.get(lang, []):
            if answer.startswith(phrase):
                answer = answer[len(phrase):].strip()
        
        # Generate graph if needed
        graph_data = None
        if self.services['graph'].should_generate_graph(query) and len(df_filtered) > 0:
            # Build title based on filters
            title_parts = ["Event Timeline"]
            if filters.get('identifier'):
                title_parts.append(f"- {filters['identifier']}")
            if filters.get('months'):
                from utils.helpers import get_month_name
                months_str = ", ".join([get_month_name(m) for m in filters['months']])
                title_parts.append(f"({months_str})")
            if filters.get('date_range'):
                title_parts.append(f"(Date {filters['date_range'][0]}-{filters['date_range'][1]})")
            
            title = " ".join(title_parts)
            graph_data = self.services['graph'].generate_xy_graph_data(df_filtered, title)
        
        # Calculate confidence
        confidence, _ = calculate_confidence(df_filtered, filters, query, "llm_analysis")
        
        return {
            "answer": answer,
            "method": "llm_analysis",
            "confidence": confidence,
            "graph_data": graph_data,
            "count": len(df_filtered)
        }