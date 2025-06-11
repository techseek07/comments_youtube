import re
import time
from googleapiclient.errors import HttpError
from settings import MAX_RETRIES, BACKOFF_FACTOR, RETRY_STATUS_CODES


class ChannelIDResolver:
    def __init__(self, youtube_service, quota_manager):
        self.youtube = youtube_service
        self.quota_manager = quota_manager
        self.channel_cache = {}

    def extract_handle_from_url(self, url):
        """Extract handle from YouTube URL."""
        patterns = [
            r'youtube\.com/@([^/?]+)',
            r'youtube\.com/c/([^/?]+)',
            r'youtube\.com/user/([^/?]+)',
            r'youtube\.com/channel/([^/?]+)'
        ]

        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None

    def resolve_channel_id(self, url_or_handle):
        """Convert YouTube URL/handle to canonical channel ID."""
        if url_or_handle in self.channel_cache:
            return self.channel_cache[url_or_handle]

        try:
            # If it's already a channel ID (starts with UC), return as is
            if url_or_handle.startswith('UC'):
                self.channel_cache[url_or_handle] = {
                    'channel_id': url_or_handle,
                    'title': 'Unknown',
                    'handle': None
                }
                return self.channel_cache[url_or_handle]

            # Extract handle from URL
            handle = self.extract_handle_from_url(url_or_handle)
            if not handle:
                handle = url_or_handle.replace('@', '')  # Remove @ if present

            # Try different search methods
            channel_info = self._search_by_handle(handle) or self._search_by_username(handle)

            if channel_info:
                self.channel_cache[url_or_handle] = channel_info
                return channel_info
            else:
                print(f"Warning: Could not resolve channel ID for {url_or_handle}")
                return None

        except Exception as e:
            print(f"Error resolving channel ID for {url_or_handle}: {e}")
            return None

    def _search_by_handle(self, handle):
        """Search channel by handle using search API."""
        try:
            if not self.quota_manager.check_quota('search'):
                raise Exception("Quota limit reached")

            request = self.youtube.search().list(
                part='snippet',
                q=f"@{handle}",
                type='channel',
                maxResults=1
            )

            response = self._execute_with_retry(request)
            self.quota_manager.use_quota('search', description=f'Search for handle {handle}')

            if response['items']:
                item = response['items'][0]
                channel_id = item['snippet']['channelId']

                # Get detailed channel info
                return self._get_channel_details(channel_id)

            return None

        except Exception as e:
            print(f"Error searching by handle {handle}: {e}")
            return None

    def _search_by_username(self, username):
        """Search channel by legacy username."""
        try:
            if not self.quota_manager.check_quota('channel_list'):
                raise Exception("Quota limit reached")

            request = self.youtube.channels().list(
                part='snippet',
                forUsername=username
            )

            response = self._execute_with_retry(request)
            self.quota_manager.use_quota('channel_list', description=f'Search for username {username}')

            if response['items']:
                item = response['items'][0]
                return {
                    'channel_id': item['id'],
                    'title': item['snippet']['title'],
                    'handle': f"@{username}",
                    'description': item['snippet']['description']
                }

            return None

        except Exception as e:
            print(f"Error searching by username {username}: {e}")
            return None

    def _get_channel_details(self, channel_id):
        """Get detailed channel information."""
        try:
            if not self.quota_manager.check_quota('channel_list'):
                raise Exception("Quota limit reached")

            request = self.youtube.channels().list(
                part='snippet,statistics',
                id=channel_id
            )

            response = self._execute_with_retry(request)
            self.quota_manager.use_quota('channel_list', description=f'Details for channel {channel_id}')

            if response['items']:
                item = response['items'][0]
                return {
                    'channel_id': item['id'],
                    'title': item['snippet']['title'],
                    'handle': item['snippet'].get('customUrl', ''),
                    'description': item['snippet']['description'],
                    'subscriber_count': int(item['statistics'].get('subscriberCount', 0)),
                    'video_count': int(item['statistics'].get('videoCount', 0))
                }

            return None

        except Exception as e:
            print(f"Error getting channel details for {channel_id}: {e}")
            return None

    def _execute_with_retry(self, request):
        """Execute request with exponential backoff retry."""
        for attempt in range(MAX_RETRIES):
            try:
                return request.execute()
            except HttpError as e:
                if e.resp.status in RETRY_STATUS_CODES:
                    wait_time = BACKOFF_FACTOR ** attempt
                    print(f"Rate limit hit. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise e

        raise Exception(f"Failed after {MAX_RETRIES} attempts")

    def resolve_all_channels(self, channel_urls):
        """Resolve all channel URLs to channel IDs."""
        resolved_channels = []

        for url in channel_urls:
            print(f"Resolving channel: {url}")
            channel_info = self.resolve_channel_id(url)

            if channel_info:
                resolved_channels.append(channel_info)
                print(f"✓ Resolved: {channel_info['title']} ({channel_info['channel_id']})")
            else:
                print(f"✗ Failed to resolve: {url}")

        return resolved_channels
