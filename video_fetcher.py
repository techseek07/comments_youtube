import time
from googleapiclient.errors import HttpError
from settings import MAX_VIDEOS_PER_CHANNEL, MAX_RETRIES, BACKOFF_FACTOR, RETRY_STATUS_CODES


class MultiChannelVideoFetcher:
    def __init__(self, youtube_service, quota_manager):
        self.youtube = youtube_service
        self.quota_manager = quota_manager

    def fetch_channel_videos(self, channel_id, max_videos=MAX_VIDEOS_PER_CHANNEL):
        """Fetch videos using UPLOADS PLAYLIST - gets ALL videos chronologically."""
        try:
            # Method 1: Get ALL videos from uploads playlist (RELIABLE)
            uploads_playlist_id = self._get_uploads_playlist_id(channel_id)
            if uploads_playlist_id:
                print(f"Using uploads playlist method for {channel_id}")
                videos = self._fetch_from_uploads_playlist(uploads_playlist_id, channel_id, max_videos)
                if videos:
                    return videos

            # Method 2: Fallback to search (if playlist method fails)
            print(f"Fallback to search method for {channel_id}")
            return self._fetch_via_search(channel_id, max_videos)

        except Exception as e:
            print(f"Error in fetch_channel_videos for {channel_id}: {e}")
            return []

    def _get_uploads_playlist_id(self, channel_id):
        """Get the uploads playlist ID for a channel."""
        try:
            if not self.quota_manager.check_quota('channel_list'):
                return None

            request = self.youtube.channels().list(
                part='contentDetails',
                id=channel_id
            )

            response = self._execute_with_retry(request)
            self.quota_manager.use_quota('channel_list', description=f'Get uploads playlist for {channel_id}')

            if response['items']:
                uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                print(f"Found uploads playlist: {uploads_playlist_id}")
                return uploads_playlist_id

            return None

        except Exception as e:
            print(f"Error getting uploads playlist for {channel_id}: {e}")
            return None

    def _fetch_from_uploads_playlist(self, playlist_id, channel_id, max_videos):
        """Fetch ALL videos from uploads playlist - GETS EVERYTHING."""
        videos = []
        page_token = None

        try:
            while len(videos) < max_videos:
                if not self.quota_manager.check_quota('videos_list'):
                    print(f"Quota limit reached. Got {len(videos)} videos so far.")
                    break

                # Get playlist items (ALL videos in chronological order)
                request = self.youtube.playlistItems().list(
                    part='snippet',
                    playlistId=playlist_id,
                    maxResults=min(50, max_videos - len(videos)),
                    pageToken=page_token
                )

                response = self._execute_with_retry(request)
                self.quota_manager.use_quota('videos_list', description=f'Playlist items from {playlist_id}')

                if not response['items']:
                    break

                # Extract video IDs
                video_ids = []
                for item in response['items']:
                    try:
                        video_id = item['snippet']['resourceId']['videoId']
                        video_ids.append(video_id)
                    except KeyError:
                        continue  # Skip deleted/private videos

                if video_ids:
                    # Get detailed video information
                    videos_request = self.youtube.videos().list(
                        part='snippet,statistics',
                        id=','.join(video_ids)
                    )
                    videos_response = self._execute_with_retry(videos_request)
                    self.quota_manager.use_quota('videos_list',
                                                 description=f'Video details for {len(video_ids)} videos')

                    # Process each video
                    for video in videos_response['items']:
                        try:
                            video_data = {
                                'video_id': video['id'],
                                'channel_id': channel_id,
                                'title': video['snippet']['title'],
                                'description': video['snippet']['description'],
                                'publish_date': video['snippet']['publishedAt'],
                                'view_count': int(video['statistics'].get('viewCount', 0)),
                                'comment_count': int(video['statistics'].get('commentCount', 0)),
                                'like_count': int(video['statistics'].get('likeCount', 0)),
                                'thumbnail_url': video['snippet']['thumbnails']['default']['url']
                            }
                            videos.append(video_data)
                        except Exception as e:
                            print(f"Error processing video {video.get('id', 'unknown')}: {e}")
                            continue

                # Check for next page
                page_token = response.get('nextPageToken')
                if not page_token:
                    break

        except Exception as e:
            print(f'Error fetching from uploads playlist: {e}')

        print(f"Found {len(videos)} videos using uploads playlist method")
        return videos

    def _fetch_via_search(self, channel_id, max_videos):
        """Fallback search method - less reliable but better than nothing."""
        videos = []
        page_token = None

        try:
            while len(videos) < max_videos:
                if not self.quota_manager.check_quota('search'):
                    print(f"Quota limit reached. Got {len(videos)} videos via search.")
                    break

                request = self.youtube.search().list(
                    part='id,snippet',
                    channelId=channel_id,
                    maxResults=min(50, max_videos - len(videos)),
                    order='date',  # Still using date order as fallback
                    type='video',
                    pageToken=page_token
                )

                response = self._execute_with_retry(request)
                self.quota_manager.use_quota('search', description=f'Video search fallback for {channel_id}')

                video_ids = [item['id']['videoId'] for item in response['items']]

                if video_ids:
                    videos_request = self.youtube.videos().list(
                        part='snippet,statistics',
                        id=','.join(video_ids)
                    )
                    videos_response = self._execute_with_retry(videos_request)
                    self.quota_manager.use_quota('videos_list')

                    for video in videos_response['items']:
                        try:
                            video_data = {
                                'video_id': video['id'],
                                'channel_id': channel_id,
                                'title': video['snippet']['title'],
                                'description': video['snippet']['description'],
                                'publish_date': video['snippet']['publishedAt'],
                                'view_count': int(video['statistics'].get('viewCount', 0)),
                                'comment_count': int(video['statistics'].get('commentCount', 0)),
                                'like_count': int(video['statistics'].get('likeCount', 0)),
                                'thumbnail_url': video['snippet']['thumbnails']['default']['url']
                            }
                            videos.append(video_data)
                        except Exception as e:
                            print(f"Error processing search video: {e}")
                            continue

                page_token = response.get('nextPageToken')
                if not page_token:
                    break

        except Exception as e:
            print(f'Error in search fallback: {e}')

        print(f"Found {len(videos)} videos using search fallback method")
        return videos

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
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    wait_time = BACKOFF_FACTOR ** attempt
                    print(f"Network error. Retry {attempt + 1}/{MAX_RETRIES} in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise e

        raise Exception(f"Failed after {MAX_RETRIES} attempts")

    def fetch_all_channels(self, channel_configs):
        """Fetch videos from all configured channels using improved method."""
        all_videos = {}

        for channel_config in channel_configs:
            channel_id = channel_config['channel_id']
            channel_name = channel_config['title']

            print(f"\n{'=' * 50}")
            print(f"Fetching ALL videos for: {channel_name}")
            print(f"Channel ID: {channel_id}")
            print(f"{'=' * 50}")

            videos = self.fetch_channel_videos(channel_id)

            all_videos[channel_id] = {
                'channel_info': channel_config,
                'videos': videos,
                'total_videos': len(videos)
            }

            print(f"✓ Successfully fetched {len(videos)} videos from {channel_name}")
            if len(videos) == MAX_VIDEOS_PER_CHANNEL:
                print(f"  (Hit the {MAX_VIDEOS_PER_CHANNEL} video limit - channel may have more)")

        return all_videos
