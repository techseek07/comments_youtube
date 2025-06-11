#!/usr/bin/env python3
"""
Enhanced YouTube NEET Channels Keyword Analysis System with New Comments Only Logic
- Filters duplicate comments and saves only new ones
- Updates existing files instead of creating new timestamped files
- Specific keyword targeting: loved it, great, boring, explanation, explain, physics wallah, neetprep, neet
- Cross-channel keyword analysis
"""

import sys
from pathlib import Path

# Add current directory to path for imports
sys.path.append(str(Path(__file__).parent))

from comment_deduplicator import CommentDeduplicator
from auth import YouTubeAuthenticator
from quota_manager import QuotaManager
from channel_resolver import ChannelIDResolver
from video_fetcher import MultiChannelVideoFetcher
from comment_processor import CommentThreadProcessor
from data_saver import DataSaver
from keyword_analyzer import CrossChannelKeywordAnalyzer
from settings import TARGET_CHANNELS, ALL_KEYWORDS, QUOTA_LIMIT_PER_DAY


def create_complete_csv_database():
    """Create a single CSV file with all comments and YouTube tracking links."""
    from pathlib import Path
    import pandas as pd
    import json
    import gzip
    from datetime import datetime  # Add this import

    print("\n📊 Creating complete CSV database...")

    raw_data_dir = Path('/Users/sugamnema/Desktop/Python/PythonProject2/youtube/data/raw')

    # Find all channel files
    json_files = list(raw_data_dir.glob('comments_*.json'))
    gz_files = list(raw_data_dir.glob('comments_*.json.gz'))
    all_files = json_files + gz_files

    all_comments = []

    for file_path in all_files:
        try:
            # Load file
            if file_path.suffix == '.gz':
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    data = json.load(f)
            else:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

            # Extract channel info
            channel_info = data.get('channel_info', {})
            channel_name = channel_info.get('channel_name', 'Unknown')

            # Process all videos and comments
            videos = data.get('videos', {})
            for video_id, video_data in videos.items():
                video_info = video_data.get('video_info', {})
                comments = video_data.get('comments', [])

                for comment in comments:
                    # Create enhanced comment record
                    enhanced_comment = {
                        'comment_id': comment.get('comment_id', ''),
                        'video_id': comment.get('video_id', ''),
                        'channel_name': channel_name,
                        'video_title': video_info.get('title', 'Unknown'),
                        'author': comment.get('author', 'Unknown'),
                        'raw_text': comment.get('raw_text', ''),
                        'likes': comment.get('likes', 0),
                        'is_reply': comment.get('is_reply', False),
                        'publish_date': comment.get('publish_date', ''),
                        'sentiment_category': comment.get('sentiment_category', 'neutral'),
                        'detected_keywords': str(comment.get('detected_keywords', {})),

                        # YouTube tracking URLs
                        'youtube_video_url': f'https://www.youtube.com/watch?v={comment.get("video_id", "")}',
                        'youtube_comment_url': f'https://www.youtube.com/watch?v={comment.get("video_id", "")}&lc={comment.get("comment_id", "")}',

                        # Metadata
                        'source_file': file_path.name
                    }
                    all_comments.append(enhanced_comment)

        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            continue

    # Create DataFrame and save CSV
    if all_comments:
        df = pd.DataFrame(all_comments)

        # Sort by likes (most engaging first)
        df = df.sort_values('likes', ascending=False, na_position='last')

        # Save CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = raw_data_dir / f'NEET_Complete_Comments_Database_{timestamp}.csv'

        df.to_csv(csv_path, index=False, encoding='utf-8-sig')

        print(f"✅ Complete CSV created: {csv_path}")
        print(f"📊 Total comments: {len(all_comments):,}")
        print(f"🔗 Each comment has YouTube tracking URLs")

        return csv_path
    else:
        print("❌ No comments found to create CSV")
        return None


def main():
    """Main execution function with new-comments-only saving logic."""
    # Initialize variables at function scope to avoid reference errors
    videos_file = None
    comments_files = None
    processed_file = None
    keyword_analysis_file = None
    data_saver = None
    comments_data = None
    videos_data = None
    resolved_channels = None
    quota_manager = None
    new_comments_only = None
    filtering_stats = None

    print("Enhanced YouTube NEET Channels Keyword Analysis System")
    print("=" * 70)
    print("🔄 NEW: Only saves new comments (filters duplicates)")
    print("Target Keywords Analysis:")
    for i, (keyword, variations) in enumerate(ALL_KEYWORDS.items(), 1):
        print(f"  {i}. {keyword}: {variations[:3]}{'...' if len(variations) > 3 else ''}")
    print("=" * 70)

    try:
        # Initialize system
        print("\n🔑 Initializing YouTube API...")
        authenticator = YouTubeAuthenticator()

        if not authenticator.test_connection():
            print("❌ Failed to connect to YouTube API. Please check your API key.")
            return

        youtube_service = authenticator.get_service()
        quota_manager = QuotaManager()  # Initialize here so it's available in except block

        print(f"✔ API connected. Remaining quota: {quota_manager.get_remaining_quota()}")

        # Initialize data saver early so it's available in except blocks
        data_saver = DataSaver()

        # Initialize deduplication system FIRST
        print("\n🔍 Initializing comment deduplication...")
        deduplicator = CommentDeduplicator()
        print(f"✔ Comment history loaded: {len(deduplicator.previous_comments):,} known comments")

        # Resolve channels
        print("\n🔍 Resolving NEET channel IDs...")
        resolver = ChannelIDResolver(youtube_service, quota_manager)
        resolved_channels = resolver.resolve_all_channels(TARGET_CHANNELS)

        if not resolved_channels:
            print("❌ No channels could be resolved.")
            return

        print(f"✔ Resolved {len(resolved_channels)} channels")

        # Initialize processors
        video_fetcher = MultiChannelVideoFetcher(youtube_service, quota_manager)
        comment_processor = CommentThreadProcessor(youtube_service, quota_manager)
        keyword_analyzer = CrossChannelKeywordAnalyzer()

        # Fetch videos
        print("\n📹 Fetching videos from NEET channels...")
        videos_data = video_fetcher.fetch_all_channels(resolved_channels)

        total_videos = sum(len(channel.get('videos', [])) for channel in videos_data.values())
        if total_videos == 0:
            print("❌ No videos found. Exiting.")
            return

        print(f"✔ Found {total_videos} videos across {len(resolved_channels)} channels")

        # Fetch comments with keyword analysis
        print("\n💬 Analyzing comments for target keywords...")
        comments_data = comment_processor.process_all_videos(videos_data)

        # FILTER FOR NEW COMMENTS ONLY
        print("\n🔍 Filtering for new comments only...")
        new_comments_only, filtering_stats = deduplicator.filter_new_comments_only(comments_data)

        # Save ONLY new comments using new logic
        print("\n💾 Saving new comments only...")
        try:
            videos_file, comments_files = data_saver.save_raw_data(videos_data, comments_data, new_comments_only)
            print(f"✔ New comments saved successfully")

        except Exception as save_error:
            print(f"❌ Error saving new comments: {save_error}")
            # Create emergency backup with ALL comments
            try:
                backup_file = data_saver.create_backup(comments_data, 'emergency_all_comments')
                print(f"Emergency backup saved: {backup_file}")
            except:
                print("Failed to create emergency backup")

        # Save updated comment history
        deduplicator.save_comment_history()

        # Generate collection report from filtering stats
        collection_report = deduplicator.get_collection_report(filtering_stats)

        # Print enhanced deduplication stats
        print(f"\n📊 Collection Report (New Comments Only):")
        print(f"  Total processed: {collection_report['total_processed']:,}")
        print(f"  New comments: {collection_report['new_comments']:,}")
        print(f"  Duplicates filtered: {collection_report['duplicates']:,}")
        print(f"  Efficiency: {collection_report['efficiency']:.1f}%")
        print(f"  History source: {collection_report['history_source']}")

        # Perform cross-channel keyword analysis using ALL comments (for comprehensive analysis)
        print("\n🔍 Performing cross-channel keyword analysis...")
        cross_channel_keyword_data = keyword_analyzer.analyze_keyword_distribution(comments_data)
        keyword_insights = keyword_analyzer.generate_keyword_insights(cross_channel_keyword_data)
        keyword_report = keyword_analyzer.create_keyword_report(cross_channel_keyword_data, keyword_insights)

        # Save all processed data
        print("\n💾 Saving analysis results...")

        # Enhanced structured data with new comments tracking
        processed_data = {
            'metadata': {
                'processing_date': data_saver.timestamp,
                'resolved_channels': resolved_channels if resolved_channels else [],
                'target_keywords': ALL_KEYWORDS,
                'analysis_summary': keyword_report['executive_summary'],
                'quota_used': quota_manager.quota_used if quota_manager else 0,
                'collection_report': collection_report,
                'new_comments_only': {
                    'enabled': True,
                    'channels_with_new_comments': len(new_comments_only) if new_comments_only else 0,
                    'filtering_efficiency': collection_report['efficiency']
                }
            },
            'channels': resolved_channels if resolved_channels else [],
            'videos': videos_data if videos_data else {},
            'comments': comments_data if comments_data else {},
            'new_comments_summary': {
                'new_comments_by_channel': {
                    channel_id: sum(len(video_data.get('comments', [])) for video_data in channel_data.values())
                    for channel_id, channel_data in (new_comments_only or {}).items()
                },
                'filtering_stats': filtering_stats if filtering_stats else {}
            },
            'keyword_analysis': {
                'cross_channel_data': cross_channel_keyword_data,
                'insights': keyword_insights,
                'full_report': keyword_report
            }
        }

        # Save files
        processed_file = data_saver.save_processed_data(processed_data)
        keyword_analysis_file = data_saver.save_analysis_data(keyword_report)
        if comments_files:
            csv_database = create_complete_csv_database()
            if csv_database:
                print(f"📊 Complete CSV database: {csv_database}")

        # Print comprehensive results
        print("\n" + "=" * 70)
        print("KEYWORD ANALYSIS RESULTS")
        print("=" * 70)

        print_keyword_analysis_summary(keyword_report, resolved_channels, quota_manager, collection_report)

        print("\n" + "=" * 70)
        print("ANALYSIS COMPLETE!")
        print("=" * 70)

        # Safe file path printing
        if videos_file:
            print(f"📁 Raw videos: {videos_file}")
        if comments_files:
            if isinstance(comments_files, list):
                print(f"📁 Raw comments: {len(comments_files)} files updated/created")
                for cf in comments_files[:3]:  # Show first 3 files
                    print(f"   - {cf.name}")
                if len(comments_files) > 3:
                    print(f"   ... and {len(comments_files) - 3} more files")
            else:
                print(f"📁 Raw comments: {comments_files}")
        if processed_file:
            print(f"📁 Processed data: {processed_file}")
        if keyword_analysis_file:
            print(f"📁 Keyword analysis: {keyword_analysis_file}")

        if quota_manager:
            print(f"📊 Quota used: {quota_manager.quota_used}/{QUOTA_LIMIT_PER_DAY}")

        # New comments summary
        if new_comments_only:
            new_comments_count = sum(
                len(video_data.get('comments', []))
                for channel_data in new_comments_only.values()
                for video_data in channel_data.values()
            )
            print(f"🔄 New comments saved: {new_comments_count:,}")
            print(f"📈 Storage efficiency: {collection_report['efficiency']:.1f}% (only new data saved)")

    except KeyboardInterrupt:
        print("\n\n⚠️ Process interrupted by user.")
        # Try to save partial data - now all variables are safely accessible
        save_partial_data(data_saver, new_comments_only if new_comments_only else comments_data, "interrupted")

    except Exception as e:
        print(f"\n❌ Error occurred: {e}")
        import traceback
        traceback.print_exc()

        # Try to save partial data - now all variables are safely accessible
        save_partial_data(data_saver, new_comments_only if new_comments_only else comments_data, "error_recovery")


def save_partial_data(data_saver, comments_data, backup_type):
    """Safely save partial data if available."""
    try:
        if data_saver and comments_data:
            backup_file = data_saver.create_backup(comments_data, f'{backup_type}_comments')
            print(f"Partial data saved: {backup_file}")
        else:
            print("No data available to save as backup")
    except Exception as backup_error:
        print(f"Failed to save backup: {backup_error}")


def print_keyword_analysis_summary(keyword_report, resolved_channels, quota_manager, collection_report):
    """Print detailed keyword analysis summary with new comments info."""
    try:
        summary = keyword_report['executive_summary']
        insights = keyword_report['insights']
        detailed_analysis = keyword_report['detailed_analysis']

        print(f"📊 Executive Summary:")
        print(f"  Total keyword mentions: {summary['total_keyword_mentions']:,}")
        print(f"  Total engagement (likes): {summary['total_keyword_engagement']:,}")
        print(f"  Channels analyzed: {summary['channel_count_analyzed']}")
        print(f"  Keywords tracked: {summary['keywords_analyzed']}")

        # New comments efficiency info
        print(f"\n🔄 Collection Efficiency:")
        print(f"  New comments: {collection_report['new_comments']:,}")
        print(f"  Duplicates filtered: {collection_report['duplicates']:,}")
        print(f"  Efficiency: {collection_report['efficiency']:.1f}%")

        if summary.get('most_mentioned_keyword'):
            print(
                f"\n🏆 Most mentioned: '{summary['most_mentioned_keyword'][0]}' ({summary['most_mentioned_keyword'][1]} mentions)")

        if summary.get('most_engaging_keyword'):
            print(
                f"💫 Most engaging: '{summary['most_engaging_keyword'][0]}' ({summary['most_engaging_keyword'][1]['average_likes_per_mention']:.1f} avg likes)")

        print(f"\n🏆 Top Keywords by Mentions:")
        for i, (keyword, mentions) in enumerate(insights['most_popular_keywords'][:5], 1):
            percentage = (mentions / summary['total_keyword_mentions'] * 100) if summary[
                                                                                     'total_keyword_mentions'] > 0 else 0
            print(f"  {i}. '{keyword}': {mentions} mentions ({percentage:.1f}%)")

        print(f"\n📺 Channel Specializations:")
        for channel, specializations in insights['channel_specializations'].items():
            if specializations:
                top_spec = specializations[0]
                print(f"  {channel[:30]}...")
                print(f"    └─ Dominates '{top_spec['keyword']}' ({top_spec['dominance_percentage']:.1f}% of mentions)")

        print(f"\n😊 Sentiment Analysis:")
        for keyword, sentiment_data in list(insights['sentiment_patterns'].items())[:5]:
            dominant = sentiment_data['dominant_sentiment']
            percentage = sentiment_data['sentiment_percentages'].get(dominant, 0)
            print(f"  '{keyword}': {dominant} ({percentage:.1f}%)")

        print(f"\n💡 Key Findings:")

        # Find most active channel overall
        channel_activity = {}
        for keyword, data in detailed_analysis.items():
            for channel, stats in data['channel_stats'].items():
                if channel not in channel_activity:
                    channel_activity[channel] = 0
                channel_activity[channel] += stats['mentions']

        if channel_activity:
            most_active = max(channel_activity.items(), key=lambda x: x[1])
            print(f"  Most active channel: {most_active[0][:40]}... ({most_active[1]} total keyword mentions)")

        # Brand mention analysis
        brand_keywords = ['physics_wallah', 'neetprep', 'neet']
        brand_mentions = sum(detailed_analysis.get(kw, {}).get('total_mentions', 0) for kw in brand_keywords)
        print(f"  Brand mentions total: {brand_mentions}")

        # Educational content analysis
        edu_keywords = ['explanation', 'explain']
        edu_mentions = sum(detailed_analysis.get(kw, {}).get('total_mentions', 0) for kw in edu_keywords)
        print(f"  Educational content mentions: {edu_mentions}")

        print(f"\n📋 Recommendations:")
        for i, rec in enumerate(keyword_report['recommendations'][:5], 1):
            print(f"  {i}. {rec}")

    except Exception as e:
        print(f"Error in summary printing: {e}")


if __name__ == "__main__":
    main()
