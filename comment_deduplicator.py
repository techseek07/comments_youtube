import json
import gzip
from datetime import datetime
from pathlib import Path
from settings import RAW_DATA_DIR


class CommentDeduplicator:
    def __init__(self):
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.comment_history_file = RAW_DATA_DIR / 'comment_history.json'
        self.previous_comments = self.load_comment_history()

    def load_comment_history(self):
        """Load previously collected comments with fallback to previous runs."""
        try:
            if self.comment_history_file.exists():
                with open(self.comment_history_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                # FALLBACK: Try to rebuild from previous raw files
                print("No comment history found. Attempting to rebuild from previous runs...")
                return self._rebuild_from_previous_runs()
        except Exception as e:
            print(f"Error loading comment history: {e}")
            return {}

    def _rebuild_from_previous_runs(self):
        """Rebuild comment history from previous raw comment files."""
        rebuilt_history = {}

        try:
            # Look for previous raw comment files (both .json and .json.gz)
            json_files = list(RAW_DATA_DIR.glob('comments_*.json'))
            gz_files = list(RAW_DATA_DIR.glob('comments_*.json.gz'))
            raw_files = json_files + gz_files

            if raw_files:
                print(f"Found {len(raw_files)} previous comment files. Rebuilding history...")

                for raw_file in raw_files:
                    try:
                        # Handle both compressed and regular files
                        if raw_file.suffix == '.gz':
                            with gzip.open(raw_file, 'rt', encoding='utf-8') as f:
                                previous_data = json.load(f)
                        else:
                            with open(raw_file, 'r', encoding='utf-8') as f:
                                previous_data = json.load(f)

                        # Extract timestamp from filename
                        file_timestamp = raw_file.stem.replace('comments_', '').replace('.json', '')

                        # Handle different file structures
                        if 'videos' in previous_data:
                            # New file format with channel_info and videos
                            videos = previous_data.get('videos', {})
                        else:
                            # Old format - direct channel data
                            videos = previous_data

                        # Process all comments from this file
                        for video_id, video_data in videos.items():
                            for comment in video_data.get('comments', []):
                                comment_id = comment.get('comment_id')
                                if not comment_id:
                                    continue

                                if comment_id not in rebuilt_history:
                                    rebuilt_history[comment_id] = {
                                        'first_collected': comment.get('publish_date', ''),
                                        'last_collected': file_timestamp,
                                        'collection_history': [file_timestamp],
                                        'author': comment.get('author', 'Unknown'),
                                        'video_id': comment.get('video_id', '')
                                    }
                                else:
                                    # Update if this file is newer
                                    if file_timestamp not in rebuilt_history[comment_id]['collection_history']:
                                        rebuilt_history[comment_id]['collection_history'].append(file_timestamp)
                                        rebuilt_history[comment_id]['last_collected'] = file_timestamp

                    except Exception as file_error:
                        print(f"Error processing {raw_file}: {file_error}")
                        continue

                print(f"Rebuilt history with {len(rebuilt_history)} comments from previous runs")

                # Save the rebuilt history
                self.save_rebuilt_history(rebuilt_history)

            return rebuilt_history

        except Exception as e:
            print(f"Error rebuilding from previous runs: {e}")
            return {}

    def save_rebuilt_history(self, rebuilt_history):
        """Save rebuilt comment history."""
        try:
            with open(self.comment_history_file, 'w', encoding='utf-8') as f:
                json.dump(rebuilt_history, f, indent=2, ensure_ascii=False)
            print(f"✓ Rebuilt comment history saved to {self.comment_history_file}")
        except Exception as e:
            print(f"Error saving rebuilt history: {e}")

    def filter_new_comments_only(self, all_comments_data):
        """MAIN METHOD: Filter and return only genuinely new comments."""
        new_comments_only = {}
        stats = {
            'total_processed': 0,
            'new_comments': 0,
            'duplicates_filtered': 0
        }

        print("\n🔍 Filtering for new comments only...")

        for channel_id, channel_data in all_comments_data.items():
            new_comments_only[channel_id] = {}

            for video_id, video_data in channel_data.items():
                comments = video_data.get('comments', [])
                new_comments_for_video = []

                for comment in comments:
                    comment_id = comment.get('comment_id')
                    stats['total_processed'] += 1

                    if comment_id and comment_id not in self.previous_comments:
                        # This is a genuinely new comment
                        new_comments_for_video.append(comment)
                        stats['new_comments'] += 1

                        # Add to history immediately
                        self.previous_comments[comment_id] = {
                            'first_collected': datetime.now().isoformat(),
                            'last_collected': datetime.now().isoformat(),
                            'collection_history': [self.run_id],
                            'author': comment.get('author', 'Unknown'),
                            'video_id': comment.get('video_id', '')
                        }
                    else:
                        # This is a duplicate
                        stats['duplicates_filtered'] += 1
                        if comment_id and comment_id in self.previous_comments:
                            # Update existing comment history
                            if self.run_id not in self.previous_comments[comment_id]['collection_history']:
                                self.previous_comments[comment_id]['collection_history'].append(self.run_id)
                                self.previous_comments[comment_id]['last_collected'] = datetime.now().isoformat()

                # Only include videos that have new comments
                if new_comments_for_video:
                    new_comments_only[channel_id][video_id] = {
                        'video_info': video_data.get('video_info'),
                        'comments': new_comments_for_video
                    }

            # Remove channels with no new comments
            if not new_comments_only[channel_id]:
                del new_comments_only[channel_id]

        # Print filtering results
        efficiency = (stats['new_comments'] / stats['total_processed'] * 100) if stats['total_processed'] > 0 else 0
        print(f"📊 Filtering Results:")
        print(f"   Total processed: {stats['total_processed']:,}")
        print(f"   New comments: {stats['new_comments']:,}")
        print(f"   Duplicates filtered: {stats['duplicates_filtered']:,}")
        print(f"   Efficiency: {efficiency:.1f}%")

        return new_comments_only, stats

    def check_comment_status(self, comment_data):
        """Check if comment was previously collected with enhanced detection."""
        comment_id = comment_data['comment_id']

        if comment_id in self.previous_comments:
            return {
                'is_duplicate_collection': True,
                'first_collected_date': self.previous_comments[comment_id]['first_collected'],
                'collection_count': len(self.previous_comments[comment_id]['collection_history']),
                'previous_collections': self.previous_comments[comment_id]['collection_history']
            }
        else:
            return {
                'is_duplicate_collection': False,
                'first_collected_date': datetime.now().isoformat(),
                'collection_count': 1,
                'previous_collections': []
            }

    def save_comment_history(self):
        """Save updated comment collection history."""
        try:
            with open(self.comment_history_file, 'w', encoding='utf-8') as f:
                json.dump(self.previous_comments, f, indent=2, ensure_ascii=False)
            print(f"✓ Comment history updated: {len(self.previous_comments):,} total comments tracked")
        except Exception as e:
            print(f"Error saving comment history: {e}")

    def get_collection_report(self, stats):
        """Generate collection statistics from filtering stats."""
        return {
            'total_processed': stats['total_processed'],
            'new_comments': stats['new_comments'],
            'duplicates': stats['duplicates_filtered'],
            'efficiency': (stats['new_comments'] / stats['total_processed'] * 100) if stats[
                                                                                          'total_processed'] > 0 else 0,
            'history_source': 'existing_history' if self.comment_history_file.exists() else 'rebuilt_from_files'
        }

    def get_collection_report_legacy(self, all_comments):
        """Generate collection statistics with better reporting (legacy method for backward compatibility)."""
        total_comments = sum(
            len(video_data.get('comments', []))
            for channel_data in all_comments.values()
            for video_data in channel_data.values()
        )

        new_comments = sum(
            1 for channel_data in all_comments.values()
            for video_data in channel_data.values()
            for comment in video_data.get('comments', [])
            if comment.get('comment_id') and comment['comment_id'] not in self.previous_comments
        )

        return {
            'total_processed': total_comments,
            'new_comments': new_comments,
            'duplicates': total_comments - new_comments,
            'efficiency': (new_comments / total_comments * 100) if total_comments > 0 else 0,
            'history_source': 'rebuilt_from_files' if len(
                self.previous_comments) > 0 and not self.comment_history_file.exists() else 'existing_history'
        }

    def add_comment_to_history(self, comment_id, comment_data):
        """Add a single comment to history (utility method)."""
        if comment_id not in self.previous_comments:
            self.previous_comments[comment_id] = {
                'first_collected': datetime.now().isoformat(),
                'last_collected': datetime.now().isoformat(),
                'collection_history': [self.run_id],
                'author': comment_data.get('author', 'Unknown'),
                'video_id': comment_data.get('video_id', '')
            }
        else:
            # Update existing comment
            if self.run_id not in self.previous_comments[comment_id]['collection_history']:
                self.previous_comments[comment_id]['collection_history'].append(self.run_id)
                self.previous_comments[comment_id]['last_collected'] = datetime.now().isoformat()

    def is_duplicate(self, comment_id):
        """Simple duplicate check method."""
        return comment_id in self.previous_comments

    def mark_as_processed(self, comment_id):
        """Mark comment as processed (for manual tracking)."""
        if comment_id not in self.previous_comments:
            self.previous_comments[comment_id] = {
                'first_collected': datetime.now().isoformat(),
                'last_collected': datetime.now().isoformat(),
                'collection_history': [self.run_id],
                'author': 'Unknown',
                'video_id': 'Unknown'
            }

    def get_duplicate_statistics(self):
        """Get detailed duplicate statistics."""
        total_tracked = len(self.previous_comments)

        # Count comments by collection frequency
        collection_counts = {}
        for comment_data in self.previous_comments.values():
            count = len(comment_data['collection_history'])
            collection_counts[count] = collection_counts.get(count, 0) + 1

        # Find most frequently collected comments
        frequent_collections = [
            (comment_id, len(data['collection_history']))
            for comment_id, data in self.previous_comments.items()
            if len(data['collection_history']) > 1
        ]
        frequent_collections.sort(key=lambda x: x[1], reverse=True)

        return {
            'total_comments_tracked': total_tracked,
            'collection_frequency_distribution': collection_counts,
            'most_frequently_collected': frequent_collections[:10],
            'unique_comments': collection_counts.get(1, 0),
            'duplicate_comments': total_tracked - collection_counts.get(1, 0)
        }

    def cleanup_old_history(self, days_threshold=30):
        """Clean up old comment history entries (utility method)."""
        from datetime import timedelta

        cutoff_date = datetime.now() - timedelta(days=days_threshold)

        cleaned_count = 0
        comments_to_remove = []

        for comment_id, comment_data in self.previous_comments.items():
            try:
                last_collected = datetime.fromisoformat(comment_data['last_collected'])
                if last_collected < cutoff_date:
                    comments_to_remove.append(comment_id)
                    cleaned_count += 1
            except (ValueError, KeyError):
                # Invalid date format or missing date, keep the comment
                continue

        # Remove old comments
        for comment_id in comments_to_remove:
            del self.previous_comments[comment_id]

        if cleaned_count > 0:
            # Save updated history
            self.save_comment_history()
            print(f"✓ Cleaned up {cleaned_count} old comment entries (older than {days_threshold} days)")

        return cleaned_count

    def export_history_summary(self, export_path=None):
        """Export comment history summary to JSON file."""
        if export_path is None:
            export_path = RAW_DATA_DIR / f'comment_history_summary_{self.run_id}.json'

        summary = {
            'export_timestamp': datetime.now().isoformat(),
            'total_comments_tracked': len(self.previous_comments),
            'duplicate_statistics': self.get_duplicate_statistics(),
            'recent_collections': [
                {
                    'comment_id': comment_id,
                    'author': data['author'],
                    'first_collected': data['first_collected'],
                    'collection_count': len(data['collection_history'])
                }
                for comment_id, data in list(self.previous_comments.items())[:100]
            ]
        }

        try:
            with open(export_path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            print(f"✓ History summary exported to: {export_path}")
            return export_path
        except Exception as e:
            print(f"Error exporting history summary: {e}")
            return None
