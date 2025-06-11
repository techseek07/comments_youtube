import json
import gzip
from datetime import datetime
from pathlib import Path
import re
from settings import (
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR,
    ANALYSIS_DATA_DIR,
    SPLIT_FILES_BY_CHANNEL,
    MAX_FILE_SIZE_MB,
    COMPRESS_LARGE_FILES
)


class DataSaver:
    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.raw_data_dir = RAW_DATA_DIR
        self.processed_data_dir = PROCESSED_DATA_DIR
        self.analysis_data_dir = ANALYSIS_DATA_DIR

    def find_existing_channel_files(self):
        """Find existing channel files (without timestamps)."""
        existing_files = {}

        # Look for existing channel files
        json_files = list(self.raw_data_dir.glob('comments_*.json'))
        gz_files = list(self.raw_data_dir.glob('comments_*.json.gz'))
        all_files = json_files + gz_files

        for file_path in all_files:
            # Extract channel name from filename (remove timestamp if present)
            filename = file_path.stem.replace('.gz', '') if file_path.suffix == '.gz' else file_path.stem

            # Remove 'comments_' prefix
            channel_part = filename.replace('comments_', '')

            # Remove timestamp suffix if present (pattern: _YYYYMMDD_HHMMSS)
            channel_name = re.sub(r'_\d{8}_\d{6}$', '', channel_part)

            existing_files[channel_name] = file_path

        return existing_files

    def load_existing_channel_data(self, file_path):
        """Load existing channel data from file."""
        try:
            if file_path.suffix == '.gz':
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    return json.load(f)
            else:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            return None

    def merge_new_comments_with_existing(self, existing_data, new_channel_data):
        """Merge new comments with existing channel data."""
        if not existing_data:
            return new_channel_data

        # Update channel info
        existing_data['channel_info']['total_comments'] += sum(
            len(video_data.get('comments', []))
            for video_data in new_channel_data.values()
        )
        existing_data['comments_summary']['collection_timestamp'] = self.timestamp

        # Merge videos and comments
        existing_videos = existing_data.get('videos', {})

        for video_id, new_video_data in new_channel_data.items():
            if video_id in existing_videos:
                # Append new comments to existing video
                existing_comments = existing_videos[video_id].get('comments', [])
                new_comments = new_video_data.get('comments', [])
                existing_videos[video_id]['comments'] = existing_comments + new_comments
            else:
                # Add new video completely
                existing_videos[video_id] = new_video_data

        existing_data['videos'] = existing_videos
        return existing_data

    def save_or_update_channel_files(self, new_comments_only, videos_data=None):
        """Save new comments by either updating existing files or creating new ones."""
        if not new_comments_only:
            print("📁 No new comments to save.")
            return []

        saved_files = []
        existing_files = self.find_existing_channel_files()

        print(f"\n📁 Saving new comments only:")
        print(f"   Found {len(existing_files)} existing channel files")

        for channel_id, new_channel_data in new_comments_only.items():
            if not new_channel_data:
                continue

            # Get channel info
            channel_name = self._extract_channel_name(channel_id, new_channel_data, videos_data)
            safe_channel_name = self._clean_filename(channel_name)

            # Count new comments
            new_comment_count = sum(
                len(video_data.get('comments', []))
                for video_data in new_channel_data.values()
            )

            print(f"   📺 {channel_name}: {new_comment_count:,} new comments")

            # Check if file exists
            existing_file_path = existing_files.get(safe_channel_name)

            if existing_file_path and existing_file_path.exists():
                # UPDATE EXISTING FILE
                print(f"      🔄 Updating existing file: {existing_file_path.name}")

                existing_data = self.load_existing_channel_data(existing_file_path)
                if existing_data:
                    merged_data = self.merge_new_comments_with_existing(existing_data, new_channel_data)

                    # Save back to same file
                    self._save_channel_file(existing_file_path, merged_data, channel_name, is_update=True)
                    saved_files.append(existing_file_path)
                else:
                    print(f"      ❌ Could not load existing file, creating new one")
                    new_file_path = self._create_new_channel_file(channel_id, new_channel_data, videos_data,
                                                                  channel_name)
                    saved_files.append(new_file_path)
            else:
                # CREATE NEW FILE (First time scenario)
                print(f"      ✨ Creating new file for {channel_name}")
                new_file_path = self._create_new_channel_file(channel_id, new_channel_data, videos_data, channel_name)
                saved_files.append(new_file_path)

        print(f"\n📊 Save Summary:")
        print(f"   Files updated/created: {len(saved_files)}")
        total_new_comments = sum(
            len(video_data.get('comments', []))
            for channel_data in new_comments_only.values()
            for video_data in channel_data.values()
        )
        print(f"   Total new comments saved: {total_new_comments:,}")

        return saved_files

    def _create_new_channel_file(self, channel_id, channel_data, videos_data, channel_name):
        """Create a new channel file (first time scenario)."""
        safe_channel_name = self._clean_filename(channel_name)

        # Count comments
        comment_count = sum(
            len(video_data.get('comments', []))
            for video_data in channel_data.values()
        )

        # Create channel file data
        channel_file_data = {
            'channel_info': {
                'channel_id': channel_id,
                'channel_name': channel_name,
                'total_videos': len(channel_data),
                'total_comments': comment_count
            },
            'videos': channel_data,
            'comments_summary': {
                'total_comments': comment_count,
                'videos_with_comments': len([v for v in channel_data.values() if v.get('comments')]),
                'collection_timestamp': self.timestamp
            }
        }

        # Create file path (without timestamp for consistency)
        filename = f"comments_{safe_channel_name}.json"
        filepath = self.raw_data_dir / filename

        self._save_channel_file(filepath, channel_file_data, channel_name, is_update=False)
        return filepath

    def _save_channel_file(self, filepath, channel_data, channel_name, is_update=False):
        """Save channel data to file with compression if needed."""
        # Check if compression needed
        json_str = json.dumps(channel_data, indent=2, ensure_ascii=False)
        estimated_size = len(json_str.encode('utf-8')) / (1024 * 1024)

        if COMPRESS_LARGE_FILES and estimated_size > MAX_FILE_SIZE_MB:
            if not filepath.name.endswith('.gz'):
                filepath = filepath.with_suffix('.json.gz')

            with gzip.open(filepath, 'wt', encoding='utf-8') as f:
                f.write(json_str)
            compression_note = " (compressed)"
        else:
            # Ensure .json extension
            if filepath.suffix == '.gz':
                filepath = filepath.with_suffix('')
            if not filepath.name.endswith('.json'):
                filepath = filepath.with_suffix('.json')

            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(json_str)
            compression_note = ""

        file_size = self._get_file_size_mb(filepath)
        action = "Updated" if is_update else "Created"
        total_comments = channel_data.get('channel_info', {}).get('total_comments', 0)

        print(f"      ✅ {action}: {filepath.name} ({file_size:.2f} MB){compression_note}")
        print(f"         Total comments in file: {total_comments:,}")

    # Update your existing save_raw_data method
    def save_raw_data(self, videos_data, comments_data, new_comments_only=None):
        """Save raw data with support for new-comments-only mode."""
        videos_file = self._save_videos_data(videos_data)

        if new_comments_only is not None:
            # Use new comments-only saving logic
            comments_files = self.save_or_update_channel_files(new_comments_only, videos_data)
            return videos_file, comments_files
        elif SPLIT_FILES_BY_CHANNEL:
            # Fallback to original logic
            comments_files = self._save_comments_by_channel(comments_data, videos_data)
            return videos_file, comments_files
        else:
            comments_file = self._save_comments_data(comments_data)
            return videos_file, comments_file

    # Keep all your existing methods
    def _save_videos_data(self, videos_data):
        """Save videos data."""
        filename = f"videos_{self.timestamp}.json"
        filepath = self.raw_data_dir / filename

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(videos_data, f, indent=2, ensure_ascii=False)

        file_size = self._get_file_size_mb(filepath)
        print(f"📁 Videos saved: {filename} ({file_size:.2f} MB)")
        return filepath

    def _save_comments_data(self, comments_data):
        """Save all comments in single file."""
        filename = f"comments_{self.timestamp}.json"
        filepath = self.raw_data_dir / filename

        # Check if compression needed
        json_str = json.dumps(comments_data, indent=2, ensure_ascii=False)
        estimated_size = len(json_str.encode('utf-8')) / (1024 * 1024)

        if COMPRESS_LARGE_FILES and estimated_size > MAX_FILE_SIZE_MB:
            filepath = filepath.with_suffix('.json.gz')
            with gzip.open(filepath, 'wt', encoding='utf-8') as f:
                f.write(json_str)
            print(f"📁 Comments saved (compressed): {filepath.name} ({self._get_file_size_mb(filepath):.2f} MB)")
        else:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(json_str)
            print(f"📁 Comments saved: {filepath.name} ({self._get_file_size_mb(filepath):.2f} MB)")

        return filepath

    def _save_comments_by_channel(self, comments_data, videos_data=None):
        """Save comments split by channel with proper channel name extraction."""
        saved_files = []
        total_comments = 0

        print(f"\n📁 Saving comments by channel:")

        for channel_id, channel_data in comments_data.items():
            if not channel_data:
                continue

            # Enhanced channel name extraction
            channel_name = self._extract_channel_name(channel_id, channel_data, videos_data)
            safe_channel_name = self._clean_filename(channel_name)

            # Count comments for this channel
            channel_comments = []
            channel_comment_count = 0

            for video_id, video_data in channel_data.items():
                comments = video_data.get('comments', [])
                channel_comments.extend(comments)
                channel_comment_count += len(comments)

            if channel_comment_count == 0:
                print(f"  ⚠️ {channel_name}: No comments - skipping")
                continue

            # Create channel-specific data structure
            channel_file_data = {
                'channel_info': {
                    'channel_id': channel_id,
                    'channel_name': channel_name,
                    'total_videos': len(channel_data),
                    'total_comments': channel_comment_count
                },
                'videos': channel_data,
                'comments_summary': {
                    'total_comments': channel_comment_count,
                    'videos_with_comments': len([v for v in channel_data.values() if v.get('comments')]),
                    'collection_timestamp': self.timestamp
                }
            }

            # Save channel file
            filename = f"comments_{safe_channel_name}_{self.timestamp}.json"
            filepath = self.raw_data_dir / filename

            # Check if compression needed
            json_str = json.dumps(channel_file_data, indent=2, ensure_ascii=False)
            estimated_size = len(json_str.encode('utf-8')) / (1024 * 1024)

            if COMPRESS_LARGE_FILES and estimated_size > MAX_FILE_SIZE_MB:
                filepath = filepath.with_suffix('.json.gz')
                with gzip.open(filepath, 'wt', encoding='utf-8') as f:
                    f.write(json_str)
                compression_note = " (compressed)"
            else:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(json_str)
                compression_note = ""

            file_size = self._get_file_size_mb(filepath)
            print(
                f"  ✅ {channel_name}: {channel_comment_count:,} comments → {filepath.name} ({file_size:.2f} MB){compression_note}")

            saved_files.append(filepath)
            total_comments += channel_comment_count

        print(f"\n📊 Channel splitting summary:")
        print(f"  Total files: {len(saved_files)}")
        print(f"  Total comments: {total_comments:,}")
        print(f"  Average per file: {total_comments / len(saved_files):,.0f}" if saved_files else "  No files created")

        return saved_files

    def _extract_channel_name(self, channel_id, channel_data, videos_data=None):
        """Enhanced channel name extraction using multiple fallback methods."""
        channel_name = 'Unknown_Channel'

        # Method 1: From videos_data (most reliable)
        if videos_data and channel_id in videos_data:
            video_channel_info = videos_data[channel_id].get('channel_info', {})
            if 'title' in video_channel_info:
                channel_name = video_channel_info['title']
                return channel_name

        # Method 2: From any video's video_info in channel_data
        for video_id, video_data in channel_data.items():
            video_info = video_data.get('video_info', {})

            # Try multiple possible keys
            for key in ['channel_title', 'channel_name', 'title']:
                if key in video_info and video_info[key]:
                    channel_name = video_info[key]
                    return channel_name

        # Method 3: From comments source_channel field
        for video_id, video_data in channel_data.items():
            comments = video_data.get('comments', [])
            if comments:
                for comment in comments[:5]:  # Check first 5 comments
                    if 'source_channel' in comment and comment['source_channel'] != 'Unknown':
                        channel_name = comment['source_channel']
                        return channel_name

                    if 'channel_title' in comment and comment['channel_title'] != 'Unknown':
                        channel_name = comment['channel_title']
                        return channel_name

        # Method 4: Channel ID mapping (manual fallback for known channels)
        channel_id_mapping = {
            'UC4BjX3BqigeWAOp0kLkKSIA': 'NEETprep_Course',
            'UC3l5WaEQL9MT9ufz4V9Dr0w': 'NEETspot',
            'UCD16eo98AXl-9T61Xd711kQ': 'Competition_Wallah',
            'UC9v_KhFxEX6D4f3oDd9_LIQ': 'NEET_Adda247',
            'UCdQwYksctqqiRwqp3PiJMWA': 'Unacademy_NEET',
            'UCYlXotCWGrgc51VzRBW83Tg': 'Motion_NEET',
            'UCySvBtI4jMLXp0BT9osvASw': 'ALLEN_NEET',
            'UCANHq6LL7-oehsVds9-2ccQ': 'Aakash_NEET'
        }

        if channel_id in channel_id_mapping:
            channel_name = channel_id_mapping[channel_id]
            return channel_name

        # Method 5: Use channel ID as fallback
        if channel_id:
            channel_name = f"Channel_{channel_id[:12]}"

        return channel_name

    def _clean_filename(self, name):
        """Clean channel name for safe filename."""
        # Remove/replace problematic characters
        safe_name = name.replace(' ', '_').replace('/', '_').replace('\\', '_')
        safe_name = safe_name.replace('(', '').replace(')', '').replace(':', '_')
        safe_name = safe_name.replace('[', '').replace(']', '').replace('|', '_')
        safe_name = ''.join(c for c in safe_name if c.isalnum() or c in ['_', '-'])
        return safe_name[:50]  # Limit length

    def save_processed_data(self, processed_data):
        """Save processed data."""
        filename = f"processed_{self.timestamp}.json"
        filepath = self.processed_data_dir / filename

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, indent=2, ensure_ascii=False)

        file_size = self._get_file_size_mb(filepath)
        print(f"📁 Processed data saved: {filename} ({file_size:.2f} MB)")
        return filepath

    def save_analysis_data(self, analysis_data):
        """Save analysis data."""
        filename = f"analysis_{self.timestamp}.json"
        filepath = self.analysis_data_dir / filename

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(analysis_data, f, indent=2, ensure_ascii=False)

        file_size = self._get_file_size_mb(filepath)
        print(f"📁 Analysis data saved: {filename} ({file_size:.2f} MB)")
        return filepath

    def create_backup(self, data, prefix="backup"):
        """Create backup file."""
        filename = f"{prefix}_{self.timestamp}.json"
        filepath = self.raw_data_dir / 'backups' / filename
        filepath.parent.mkdir(exist_ok=True)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        return filepath

    def _get_file_size_mb(self, filepath):
        """Get file size in MB."""
        return filepath.stat().st_size / (1024 * 1024)
