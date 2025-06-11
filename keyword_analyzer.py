from collections import defaultdict, Counter
from settings import ALL_KEYWORDS, TARGET_KEYWORDS


class CrossChannelKeywordAnalyzer:
    def __init__(self):
        self.target_keywords = list(ALL_KEYWORDS.keys())

    def analyze_keyword_distribution(self, comments_data):
        """Analyze keyword distribution across all channels."""
        cross_channel_analysis = {}

        # Initialize structure for each target keyword
        for keyword in self.target_keywords:
            cross_channel_analysis[keyword] = {
                'total_mentions': 0,
                'total_likes': 0,
                'channel_stats': {},
                'top_channels': [],
                'sentiment_breakdown': defaultdict(int),
                'sample_comments': []
            }

        # Process each channel's data
        for channel_id, channel_data in comments_data.items():
            for video_id, video_data in channel_data.items():
                if video_data.get('keyword_segmentation'):
                    channel_name = video_data['video_info'].get('title', 'Unknown')

                    for keyword, keyword_data in video_data['keyword_segmentation'].items():
                        if keyword in cross_channel_analysis:
                            # Update cross-channel stats
                            cross_channel_analysis[keyword]['total_mentions'] += keyword_data['total_mentions']
                            cross_channel_analysis[keyword]['total_likes'] += keyword_data['total_likes']

                            # Channel-specific stats
                            if channel_name not in cross_channel_analysis[keyword]['channel_stats']:
                                cross_channel_analysis[keyword]['channel_stats'][channel_name] = {
                                    'mentions': 0,
                                    'likes': 0,
                                    'videos_count': 0
                                }

                            cross_channel_analysis[keyword]['channel_stats'][channel_name]['mentions'] += keyword_data[
                                'total_mentions']
                            cross_channel_analysis[keyword]['channel_stats'][channel_name]['likes'] += keyword_data[
                                'total_likes']
                            cross_channel_analysis[keyword]['channel_stats'][channel_name]['videos_count'] += 1

                            # Sentiment breakdown
                            for sentiment, count in keyword_data['sentiment_distribution'].items():
                                cross_channel_analysis[keyword]['sentiment_breakdown'][sentiment] += count

                            # Collect sample comments
                            cross_channel_analysis[keyword]['sample_comments'].extend(
                                keyword_data['top_comments'][:2]  # Top 2 comments per video
                            )

        # Post-process: rank channels and sort sample comments
        for keyword, data in cross_channel_analysis.items():
            # Rank channels by mentions
            channel_rankings = sorted(
                data['channel_stats'].items(),
                key=lambda x: x[1]['mentions'],
                reverse=True
            )
            data['top_channels'] = channel_rankings

            # Sort sample comments by likes and limit
            data['sample_comments'].sort(key=lambda x: x['likes'], reverse=True)
            data['sample_comments'] = data['sample_comments'][:10]

            # Convert sentiment breakdown to regular dict
            data['sentiment_breakdown'] = dict(data['sentiment_breakdown'])

        return cross_channel_analysis

    def generate_keyword_insights(self, cross_channel_data):
        """Generate insights from cross-channel keyword analysis."""
        insights = {
            'most_popular_keywords': [],
            'channel_specializations': {},
            'sentiment_patterns': {},
            'engagement_analysis': {},
            'keyword_correlations': []
        }

        # Most popular keywords
        keyword_popularity = [(k, v['total_mentions']) for k, v in cross_channel_data.items()]
        insights['most_popular_keywords'] = sorted(keyword_popularity, key=lambda x: x[1], reverse=True)

        # Channel specializations (which channel dominates which keyword)
        for keyword, data in cross_channel_data.items():
            if data['top_channels']:
                dominant_channel = data['top_channels'][0]
                if dominant_channel[0] not in insights['channel_specializations']:
                    insights['channel_specializations'][dominant_channel[0]] = []

                insights['channel_specializations'][dominant_channel[0]].append({
                    'keyword': keyword,
                    'mentions': dominant_channel[1]['mentions'],
                    'dominance_percentage': (dominant_channel[1]['mentions'] / data['total_mentions'] * 100) if data[
                                                                                                                    'total_mentions'] > 0 else 0
                })

        # Sentiment patterns
        for keyword, data in cross_channel_data.items():
            total_sentiment_mentions = sum(data['sentiment_breakdown'].values())
            if total_sentiment_mentions > 0:
                insights['sentiment_patterns'][keyword] = {
                    'dominant_sentiment': max(data['sentiment_breakdown'], key=data['sentiment_breakdown'].get),
                    'sentiment_percentages': {
                        sentiment: (count / total_sentiment_mentions * 100)
                        for sentiment, count in data['sentiment_breakdown'].items()
                    }
                }

        # Engagement analysis (likes per mention)
        for keyword, data in cross_channel_data.items():
            if data['total_mentions'] > 0:
                insights['engagement_analysis'][keyword] = {
                    'average_likes_per_mention': data['total_likes'] / data['total_mentions'],
                    'total_engagement': data['total_likes']
                }

        return insights

    def create_keyword_report(self, cross_channel_data, insights):
        """Create a comprehensive keyword analysis report."""
        report = {
            'executive_summary': self._create_executive_summary(cross_channel_data, insights),
            'detailed_analysis': cross_channel_data,
            'insights': insights,
            'recommendations': self._generate_recommendations(insights)
        }

        return report

    def _create_executive_summary(self, cross_channel_data, insights):
        """Create executive summary of keyword analysis."""
        total_mentions = sum(data['total_mentions'] for data in cross_channel_data.values())
        total_likes = sum(data['total_likes'] for data in cross_channel_data.values())

        most_mentioned = insights['most_popular_keywords'][0] if insights['most_popular_keywords'] else None
        most_engaging = max(insights['engagement_analysis'].items(), key=lambda x: x[1]['average_likes_per_mention']) if \
        insights['engagement_analysis'] else None

        return {
            'total_keyword_mentions': total_mentions,
            'total_keyword_engagement': total_likes,
            'most_mentioned_keyword': most_mentioned,
            'most_engaging_keyword': most_engaging,
            'channel_count_analyzed': len(set(
                channel for data in cross_channel_data.values()
                for channel in data['channel_stats'].keys()
            )),
            'keywords_analyzed': len(cross_channel_data)
        }

    def _generate_recommendations(self, insights):
        """Generate actionable recommendations based on analysis."""
        recommendations = []

        # Keyword strategy recommendations
        if insights['most_popular_keywords']:
            top_keyword = insights['most_popular_keywords'][0]
            recommendations.append(
                f"Focus on '{top_keyword[0]}' keyword - it has {top_keyword[1]} mentions across channels")

        # Channel strategy recommendations
        for channel, specializations in insights['channel_specializations'].items():
            if specializations:
                top_specialization = max(specializations, key=lambda x: x['dominance_percentage'])
                if top_specialization['dominance_percentage'] > 50:
                    recommendations.append(
                        f"{channel} dominates '{top_specialization['keyword']}' with {top_specialization['dominance_percentage']:.1f}% of mentions")

        # Sentiment recommendations
        for keyword, sentiment_data in insights['sentiment_patterns'].items():
            if sentiment_data['dominant_sentiment'] == 'negative':
                recommendations.append(f"Address negative sentiment around '{keyword}' keyword")
            elif sentiment_data['dominant_sentiment'] == 'positive':
                recommendations.append(f"Leverage positive sentiment around '{keyword}' for marketing")

        return recommendations
