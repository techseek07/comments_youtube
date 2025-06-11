import time
import math
from collections import defaultdict
from googleapiclient.errors import HttpError
from text_cleaner import TextCleaner
from settings import (
    MAX_COMMENTS_PER_REQUEST, MAX_RETRIES, BACKOFF_FACTOR,
    RETRY_STATUS_CODES, TOP_COMMENTS_COUNT, REPLIES_PER_TOP_COMMENT,
    LIKE_WEIGHT, REPLY_WEIGHT, MIN_COMMENTS_THRESHOLD, RATE_LIMIT_DELAY
)


class CommentThreadProcessor:
    def __init__(self, youtube_service, quota_manager):
        self.youtube = youtube_service
        self.quota_manager = quota_manager
        self.text_cleaner = TextCleaner()

        # OPTIMIZED SETTINGS BASED ON YOUR REQUIREMENTS
        self.absolute_quota_reserve = 10

        # PRIORITY CHANNELS: Limited for efficiency
        self.unlimited_priority_channels = ['NEETprep', 'NEETspot']

        # ENHANCED COLLECTION SETTINGS - YOUR REQUESTED VALUES
        self.priority_max_pages = 200  # Prevents single video domination
        self.basic_max_pages = 100  # For balanced channels
        self.basic_min_comments = 5
        self.max_pages_per_round = 80  # INCREASED from 40 to 80
        self.max_rounds = 10
        self.videos_per_round = 10 # INCREASED from 5 to 8

        # REPLY OPTIMIZATION SETTINGS - YOUR REQUESTED CHANGES
        self.max_top_replies_priority = 5  # Top 5 for priority channels
        self.max_top_replies_balanced = 2  # TOP 2 REPLIES for balanced channels (changed from 0)

        self.video_processing_state = {}

    def calculate_comment_score(self, likes, reply_count):
        """Calculate weighted score for comment ranking using likes and replies."""
        like_score = math.log(likes + 1)
        reply_score = math.log(reply_count + 1)
        weighted_score = (LIKE_WEIGHT * like_score) + (REPLY_WEIGHT * reply_score)
        return weighted_score

    def is_unlimited_priority_channel(self, channel_name):
        """Check if channel gets unlimited collection first."""
        return any(priority in channel_name for priority in self.unlimited_priority_channels)

    def fetch_video_comments_unlimited(self, video_id, video_info, channel_info):
        """Fetch comments from priority channels with TOP 5 MOST LIKED replies and 200 page limit."""
        try:
            comment_count = video_info.get('comment_count', 0)
            if comment_count == 0:
                return self._get_empty_comment_result('No comments')

            all_comments = []
            page_token = None
            page_count = 0

            print(
                f"🚀 PRIORITY collection: {video_id} (estimated: {comment_count} comments, max {self.priority_max_pages} pages)")

            while page_count < self.priority_max_pages:  # 200 PAGE LIMIT
                remaining_quota = self.quota_manager.get_remaining_quota()
                if remaining_quota < self.absolute_quota_reserve:
                    print(f"🔥 Hit quota reserve ({remaining_quota} remaining)")
                    break

                if not self.quota_manager.check_quota('comment_threads'):
                    break

                try:
                    request = self.youtube.commentThreads().list(
                        part='snippet,replies',
                        videoId=video_id,
                        maxResults=MAX_COMMENTS_PER_REQUEST,
                        pageToken=page_token,
                        textFormat='plainText',
                        order='relevance'
                    )

                    response = self._execute_with_retry(request)
                    self.quota_manager.use_quota('comment_threads',
                                                 description=f'PRIORITY - Video {video_id} page {page_count + 1}')

                    items_in_page = len(response.get('items', []))
                    if items_in_page == 0:
                        break

                    for item in response['items']:
                        # Always collect top-level comment
                        top_comment = self._process_comment_with_keywords(
                            item['snippet']['topLevelComment'],
                            video_id, False, None, channel_info
                        )
                        all_comments.append(top_comment)

                        # COLLECT TOP 5 MOST LIKED REPLIES FOR PRIORITY
                        reply_count = item['snippet']['totalReplyCount']
                        if reply_count > 0:
                            all_replies = []

                            # Get replies that come with the comment thread
                            if 'replies' in item:
                                for reply in item['replies']['comments']:
                                    reply_comment = self._process_comment_with_keywords(
                                        reply, video_id, True, item['id'], channel_info
                                    )
                                    all_replies.append(reply_comment)

                            # FIXED: Fetch additional replies WITHOUT order parameter
                            if reply_count > len(all_replies) and remaining_quota > 30:
                                try:
                                    additional_replies_request = self.youtube.comments().list(
                                        part='snippet',
                                        parentId=item['id'],
                                        maxResults=100,
                                        textFormat='plainText'
                                        # REMOVED: order='relevance' - not supported by comments.list()
                                    )
                                    additional_replies_response = self._execute_with_retry(additional_replies_request)
                                    self.quota_manager.use_quota('comments_list',
                                                                 description=f'Additional replies for {item["id"]}')

                                    for additional_reply in additional_replies_response.get('items', []):
                                        reply_comment = self._process_comment_with_keywords(
                                            additional_reply, video_id, True, item['id'], channel_info
                                        )
                                        all_replies.append(reply_comment)
                                except Exception as e:
                                    print(f"Error fetching additional replies: {e}")

                            # MANUAL SORTING BY LIKES - This gives you the relevance you want!
                            if all_replies:
                                sorted_replies = sorted(all_replies, key=lambda x: x.get('likes', 0), reverse=True)
                                top_replies = sorted_replies[:self.max_top_replies_priority]
                                all_comments.extend(top_replies)

                    page_token = response.get('nextPageToken')
                    page_count += 1

                    # Ultra fast processing
                    time.sleep(0.02)

                    if not page_token:
                        break

                    # Progress reporting every 30 pages
                    if page_count % 30 == 0:
                        top_level_count = len([c for c in all_comments if not c['is_reply']])
                        reply_count = len([c for c in all_comments if c['is_reply']])
                        print(
                            f"    📈 Page {page_count}/{self.priority_max_pages}: {len(all_comments)} total ({top_level_count} comments + {reply_count} replies)")

                except HttpError as e:
                    if e.resp.status == 403 and 'commentsDisabled' in str(e):
                        return self._get_empty_comment_result('Comments disabled')
                    else:
                        raise e

            analysis_result = self._analyze_comments_with_keywords(video_id, all_comments, channel_info)

            return {
                'all_comments': all_comments,
                'top_comments_analysis': analysis_result['top_comments'],
                'keyword_segmentation': analysis_result['keyword_analysis'],
                'total_comments': len(all_comments),
                'pages_processed': page_count,
                'collection_mode': 'priority_top5_replies_limited',
                'analysis_skipped': False,
                'has_sufficient_data': len(all_comments) >= MIN_COMMENTS_THRESHOLD
            }

        except Exception as e:
            print(f'Error fetching priority comments for video {video_id}: {e}')
            return self._get_empty_comment_result(f'Error: {str(e)}')

    def init_video_state(self, video_id, estimated_comments):
        """Initialize tracking state for balanced collection."""
        if video_id not in self.video_processing_state:
            self.video_processing_state[video_id] = {
                'pages_processed': 0,
                'total_comments_collected': 0,
                'last_page_token': None,
                'is_complete': False,
                'estimated_comments': estimated_comments,
                'collected_comment_ids': set()
            }

    def fetch_video_comments_balanced(self, video_id, video_info, channel_info, max_pages_this_round=None):
        """Fetch comments with TOP 2 REPLIES for balanced coverage."""
        try:
            comment_count = video_info.get('comment_count', 0)

            if comment_count < self.basic_min_comments:
                return self._get_empty_comment_result(f'Too few comments ({comment_count})')

            # Initialize video state
            self.init_video_state(video_id, comment_count)
            video_state = self.video_processing_state[video_id]

            if video_state['is_complete']:
                return self._get_empty_comment_result('Video already complete')

            if max_pages_this_round is None:
                max_pages_this_round = self.max_pages_per_round  # Now 80 pages per round

            pages_remaining = self.basic_max_pages - video_state['pages_processed']
            pages_this_round = min(max_pages_this_round, pages_remaining)

            if pages_this_round <= 0:
                video_state['is_complete'] = True
                return self._get_empty_comment_result('Page limit reached')

            all_comments = []
            page_token = video_state['last_page_token']
            page_count = 0

            print(f"⚖️ BALANCED collection: {video_id} (TOP 2 REPLIES)")
            print(
                f"    📊 Pages {video_state['pages_processed'] + 1}-{video_state['pages_processed'] + pages_this_round} of {self.basic_max_pages}")

            while page_count < pages_this_round:
                remaining_quota = self.quota_manager.get_remaining_quota()
                if remaining_quota < self.absolute_quota_reserve:
                    break

                if not self.quota_manager.check_quota('comment_threads'):
                    break

                try:
                    request = self.youtube.commentThreads().list(
                        part='snippet,replies',
                        videoId=video_id,
                        maxResults=MAX_COMMENTS_PER_REQUEST,
                        pageToken=page_token,
                        textFormat='plainText',
                        order='relevance'
                    )

                    response = self._execute_with_retry(request)
                    self.quota_manager.use_quota('comment_threads',
                                                 description=f'BALANCED - Video {video_id} page {video_state["pages_processed"] + page_count + 1}')

                    items_in_page = len(response.get('items', []))
                    if items_in_page == 0:
                        video_state['is_complete'] = True
                        break

                    for item in response['items']:
                        # Collect top-level comment
                        top_comment = self._process_comment_with_keywords(
                            item['snippet']['topLevelComment'],
                            video_id, False, None, channel_info
                        )

                        comment_id = top_comment['comment_id']
                        if comment_id not in video_state['collected_comment_ids']:
                            all_comments.append(top_comment)
                            video_state['collected_comment_ids'].add(comment_id)

                        # COLLECT TOP 2 REPLIES FOR BALANCED CHANNELS
                        reply_count = item['snippet']['totalReplyCount']
                        if reply_count > 0 and 'replies' in item:
                            # Get available replies
                            available_replies = []
                            for reply in item['replies']['comments']:
                                reply_comment = self._process_comment_with_keywords(
                                    reply, video_id, True, item['id'], channel_info
                                )
                                available_replies.append(reply_comment)

                            # Sort by likes and take top 2
                            if available_replies:
                                sorted_replies = sorted(available_replies, key=lambda x: x.get('likes', 0),
                                                        reverse=True)
                                top_2_replies = sorted_replies[:self.max_top_replies_balanced]  # Top 2 only

                                for reply in top_2_replies:
                                    reply_id = reply['comment_id']
                                    if reply_id not in video_state['collected_comment_ids']:
                                        all_comments.append(reply)
                                        video_state['collected_comment_ids'].add(reply_id)

                    page_token = response.get('nextPageToken')
                    page_count += 1

                    video_state['pages_processed'] += 1
                    video_state['total_comments_collected'] += len(all_comments)
                    video_state['last_page_token'] = page_token

                    # Fast processing
                    time.sleep(0.05)

                    if not page_token:
                        video_state['is_complete'] = True
                        break

                except HttpError as e:
                    if e.resp.status == 403 and 'commentsDisabled' in str(e):
                        video_state['is_complete'] = True
                        return self._get_empty_comment_result('Comments disabled')
                    else:
                        raise e

            analysis_result = self._analyze_comments_with_keywords(video_id, all_comments, channel_info)

            return {
                'all_comments': all_comments,
                'top_comments_analysis': analysis_result['top_comments'],
                'keyword_segmentation': analysis_result['keyword_analysis'],
                'total_comments': len(all_comments),
                'pages_processed_this_round': page_count,
                'total_pages_processed': video_state['pages_processed'],
                'is_complete': video_state['is_complete'],
                'collection_mode': 'balanced_top2_replies',
                'analysis_skipped': False,
                'has_sufficient_data': len(all_comments) >= MIN_COMMENTS_THRESHOLD
            }

        except Exception as e:
            print(f'Error fetching balanced comments for video {video_id}: {e}')
            return self._get_empty_comment_result(f'Error: {str(e)}')

    def _process_comment_with_keywords(self, comment_data, video_id, is_reply=False, parent_id=None, channel_info=None):
        """Process individual comment data with keyword detection."""
        snippet = comment_data['snippet']

        raw_text = snippet.get('textDisplay', '') or snippet.get('textOriginal', '')
        cleaned_text = self.text_cleaner.clean_text(raw_text)
        detected_keywords = self.text_cleaner.detect_target_keywords(cleaned_text)
        sentiment_category = self.text_cleaner.categorize_comment_sentiment(detected_keywords)

        return {
            'comment_id': comment_data['id'],
            'video_id': video_id,
            'parent_id': parent_id,
            'is_reply': is_reply,
            'author': snippet.get('authorDisplayName', 'Unknown'),
            'author_channel_id': snippet.get('authorChannelId', {}).get('value', ''),
            'raw_text': raw_text,
            'cleaned_text': cleaned_text,
            'likes': snippet.get('likeCount', 0),
            'publish_date': snippet.get('publishedAt', ''),
            'updated_date': snippet.get('updatedAt', ''),
            'reply_count': 0 if is_reply else snippet.get('totalReplyCount', 0),
            'detected_keywords': detected_keywords,
            'sentiment_category': sentiment_category,
            'source_channel': channel_info.get('title') if channel_info else 'Unknown',
            'channel_title': channel_info.get('title') if channel_info else 'Unknown'  # ADDED: For proper file naming
        }

    def _analyze_comments_with_keywords(self, video_id, all_comments, channel_info):
        """Analyze comments with enhanced keyword segmentation."""
        top_level_comments = [c for c in all_comments if not c['is_reply']]

        for comment in top_level_comments:
            comment['weighted_score'] = self.calculate_comment_score(
                comment['likes'], comment['reply_count']
            )

        top_comments = sorted(top_level_comments, key=lambda x: x['weighted_score'], reverse=True)[:TOP_COMMENTS_COUNT]

        top_comments_with_replies = []
        for top_comment in top_comments:
            comment_thread_id = top_comment['comment_id']
            existing_replies = [c for c in all_comments if c.get('parent_id') == comment_thread_id]
            limited_replies = existing_replies[:REPLIES_PER_TOP_COMMENT]

            top_comments_with_replies.append({
                'top_comment': top_comment,
                'replies': limited_replies,
                'total_replies_available': top_comment['reply_count'],
                'weighted_score': top_comment['weighted_score'],
                'score_breakdown': {
                    'likes': top_comment['likes'],
                    'reply_count': top_comment['reply_count'],
                    'like_contribution': LIKE_WEIGHT * math.log(top_comment['likes'] + 1),
                    'reply_contribution': REPLY_WEIGHT * math.log(top_comment['reply_count'] + 1)
                }
            })

        keyword_analysis = self._perform_keyword_segmentation(all_comments, channel_info)

        return {
            'top_comments': top_comments_with_replies,
            'keyword_analysis': keyword_analysis
        }

    def _perform_keyword_segmentation(self, comments, channel_info):
        """Perform detailed keyword segmentation analysis."""
        keyword_stats = defaultdict(lambda: {
            'comments': [],
            'total_count': 0,
            'total_likes': 0,
            'channels': defaultdict(int),
            'sentiment_distribution': defaultdict(int)
        })

        for comment in comments:
            detected_keywords = comment.get('detected_keywords', {})
            sentiment = comment.get('sentiment_category', 'neutral')
            channel_name = comment.get('source_channel', 'Unknown')

            for main_keyword, variations in detected_keywords.items():
                keyword_stats[main_keyword]['comments'].append({
                    'comment_id': comment['comment_id'],
                    'text_preview': comment['cleaned_text'][:100] + '...' if len(comment['cleaned_text']) > 100 else
                    comment['cleaned_text'],
                    'full_text': comment['cleaned_text'],
                    'likes': comment['likes'],
                    'author': comment['author'],
                    'variations_found': variations,
                    'sentiment': sentiment,
                    'channel': channel_name,
                    'video_id': comment['video_id']
                })

                keyword_stats[main_keyword]['total_count'] += 1
                keyword_stats[main_keyword]['total_likes'] += comment['likes']
                keyword_stats[main_keyword]['channels'][channel_name] += 1
                keyword_stats[main_keyword]['sentiment_distribution'][sentiment] += 1

        result = {}
        for keyword, stats in keyword_stats.items():
            stats['comments'].sort(key=lambda x: x['likes'], reverse=True)

            if stats['channels']:
                most_common_channel = max(stats['channels'].items(), key=lambda x: x[1])
            else:
                most_common_channel = ('Unknown', 0)

            result[keyword] = {
                'total_mentions': stats['total_count'],
                'total_likes': stats['total_likes'],
                'average_likes': stats['total_likes'] / stats['total_count'] if stats['total_count'] > 0 else 0,
                'most_common_channel': {
                    'name': most_common_channel[0],
                    'count': most_common_channel[1]
                },
                'channel_distribution': dict(stats['channels']),
                'sentiment_distribution': dict(stats['sentiment_distribution']),
                'top_comments': stats['comments'][:10],
                'sample_variations': list(set([
                    var for comment in stats['comments'][:5]
                    for var in comment.get('variations_found', [])
                ]))[:5]
            }

        return result

    def _get_empty_comment_result(self, reason):
        """Return empty comment result structure."""
        return {
            'all_comments': [],
            'top_comments_analysis': [],
            'keyword_segmentation': {},
            'total_comments': 0,
            'analysis_skipped': True,
            'skip_reason': reason
        }

    def _execute_with_retry(self, request):
        """Execute request with exponential backoff retry for transient errors."""
        for attempt in range(MAX_RETRIES):
            try:
                return request.execute()
            except HttpError as e:
                if e.resp.status in RETRY_STATUS_CODES:
                    wait_time = BACKOFF_FACTOR ** attempt
                    print(f"Transient error {e.resp.status}. Retry {attempt + 1}/{MAX_RETRIES} in {wait_time}s...")
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

    def process_all_videos(self, videos_data):
        """OPTIMIZED processing with your requested settings."""
        all_comments = {}
        total_quota_start = self.quota_manager.quota_used

        print(f"\n🔥 OPTIMIZED COMMENT COLLECTION")
        print(f"🚀 Priority: Top 5 replies, 200 pages max per video")
        print(f"⚖️ Balanced: Top 2 replies, 100 pages max per video")
        print(f"🎯 Enhanced: {self.max_pages_per_round} pages/round, {self.videos_per_round} videos/round")

        # PHASE 1: PRIORITY COLLECTION WITH 200 PAGE LIMIT
        print(f"\n{'=' * 80}")
        print(f"PHASE 1: PRIORITY COLLECTION (200 PAGE LIMIT)")
        print(f"{'=' * 80}")

        priority_channels_data = {}
        for channel_id, channel_data in videos_data.items():
            channel_info = channel_data['channel_info']
            if self.is_unlimited_priority_channel(channel_info['title']):
                priority_channels_data[channel_id] = channel_data

        for channel_id, channel_data in priority_channels_data.items():
            channel_comments = {}
            channel_info = channel_data['channel_info']
            channel_quota_start = self.quota_manager.quota_used

            print(f"\n🚀 PRIORITY processing: {channel_info['title']}")
            print(f"💰 Quota remaining: {self.quota_manager.get_remaining_quota()}")
            print(f"📝 Limits: Max {self.priority_max_pages} pages, top {self.max_top_replies_priority} replies")

            videos = channel_data.get('videos', [])
            if not videos:
                all_comments[channel_id] = {}
                continue

            # Sort by comment count (highest first)
            videos = sorted(videos, key=lambda v: v.get('comment_count', 0), reverse=True)

            for i, video in enumerate(videos, 1):
                video_id = video['video_id']
                estimated_comments = video.get('comment_count', 0)

                print(f"\n[{i}/{len(videos)}] {video['title'][:60]}...")
                print(f"    📊 Estimated: {estimated_comments} comments")

                remaining = self.quota_manager.get_remaining_quota()
                if remaining < 30:
                    print(f"🔥 PHASE 1 STOPPED: Low quota ({remaining} remaining)")
                    break

                try:
                    comment_data = self.fetch_video_comments_unlimited(
                        video_id, video, channel_info
                    )

                    channel_comments[video_id] = {
                        'video_info': video,
                        'comments': comment_data['all_comments'],
                        'top_comments_analysis': comment_data['top_comments_analysis'],
                        'keyword_segmentation': comment_data['keyword_segmentation'],
                        'total_comments': comment_data['total_comments'],
                        'pages_processed': comment_data.get('pages_processed', 0),
                        'collection_mode': comment_data.get('collection_mode', 'priority'),
                        'analysis_metadata': {
                            'skipped': comment_data.get('analysis_skipped', False),
                            'skip_reason': comment_data.get('skip_reason', ''),
                            'processed_at': time.time()
                        }
                    }

                    if comment_data.get('analysis_skipped'):
                        print(f"    ⚠️ Skipped: {comment_data.get('skip_reason')}")
                    else:
                        pages = comment_data.get('pages_processed', 1)
                        top_level = len([c for c in comment_data['all_comments'] if not c['is_reply']])
                        replies = len([c for c in comment_data['all_comments'] if c['is_reply']])
                        print(f"    ✅ {comment_data['total_comments']} comments ({pages} pages)")
                        print(f"        📊 {top_level} original + {replies} top replies")

                except Exception as e:
                    print(f"    ❌ Error: {str(e)}")
                    channel_comments[video_id] = {
                        'video_info': video,
                        'comments': [],
                        'analysis_metadata': {'skipped': True, 'skip_reason': f'Error: {str(e)}'}
                    }

            # Phase 1 channel summary
            channel_quota_used = self.quota_manager.quota_used - channel_quota_start
            channel_comments_total = sum(len(v.get('comments', [])) for v in channel_comments.values())

            print(f"\n🏁 PHASE 1 - {channel_info['title']} Complete:")
            print(f"    💬 Comments: {channel_comments_total}")
            print(f"    💰 Quota used: {channel_quota_used}")

            all_comments[channel_id] = channel_comments

        # PHASE 2: BALANCED COLLECTION WITH TOP 2 REPLIES
        remaining_quota = self.quota_manager.get_remaining_quota()
        if remaining_quota > 100:
            print(f"\n{'=' * 80}")
            print(f"PHASE 2: BALANCED COLLECTION (TOP 2 REPLIES)")
            print(f"Quota available: {remaining_quota} units")
            print(f"{'=' * 80}")

            # Get non-priority channels
            remaining_channels_data = {}
            for channel_id, channel_data in videos_data.items():
                channel_info = channel_data['channel_info']
                if not self.is_unlimited_priority_channel(channel_info['title']):
                    remaining_channels_data[channel_id] = channel_data

            # ENHANCED Multi-round processing with 80 pages per round, 8 videos per round
            round_number = 1

            while round_number <= self.max_rounds and self.quota_manager.get_remaining_quota() > 30:
                print(f"\n--- ENHANCED ROUND {round_number}/{self.max_rounds} ---")
                print(f"📊 Processing {self.max_pages_per_round} pages/video, {self.videos_per_round} videos/channel")
                progress_made = False

                for channel_id, channel_data in remaining_channels_data.items():
                    channel_info = channel_data['channel_info']

                    print(f"\n⚖️ Round {round_number} - {channel_info['title']} (TOP 2 REPLIES)")

                    if channel_id not in all_comments:
                        all_comments[channel_id] = {}

                    videos = channel_data.get('videos', [])
                    if not videos:
                        continue

                    # Sort by engagement potential
                    videos = sorted(videos, key=lambda v: v.get('comment_count', 0), reverse=True)

                    videos_processed = 0
                    for video in videos:
                        remaining = self.quota_manager.get_remaining_quota()
                        if remaining < 15:
                            break

                        video_id = video['video_id']

                        try:
                            comment_data = self.fetch_video_comments_balanced(
                                video_id, video, channel_info, max_pages_this_round=self.max_pages_per_round
                            )

                            if video_id not in all_comments[channel_id]:
                                all_comments[channel_id][video_id] = {
                                    'video_info': video,
                                    'comments': [],
                                    'total_comments': 0
                                }

                            # Safe comment processing
                            video_entry = all_comments[channel_id][video_id]
                            existing_comments = video_entry.get('comments', [])
                            existing_ids = {c.get('comment_id') for c in existing_comments if c.get('comment_id')}

                            new_comments = [
                                c for c in comment_data.get('all_comments', [])
                                if c.get('comment_id') and c.get('comment_id') not in existing_ids
                            ]

                            # Update video entry
                            video_entry['comments'].extend(new_comments)
                            video_entry['total_comments'] = len(video_entry['comments'])

                            if len(new_comments) > 0:
                                progress_made = True
                                videos_processed += 1
                                top_level_new = len([c for c in new_comments if not c['is_reply']])
                                replies_new = len([c for c in new_comments if c['is_reply']])
                                print(
                                    f"    ✅ {video['title'][:40]}... (+{len(new_comments)} comments: {top_level_new} original + {replies_new} replies)")

                        except Exception as e:
                            print(f"    ❌ Error: {str(e)}")

                        if videos_processed >= self.videos_per_round:  # Process 8 videos per round
                            break

                if not progress_made:
                    print(f"🏁 No progress in round {round_number} - stopping")
                    break

                round_number += 1

        # FINAL OPTIMIZED SUMMARY
        total_quota_used = self.quota_manager.quota_used - total_quota_start
        total_comments = sum(
            len(v.get('comments', []))
            for channel_data in all_comments.values()
            for v in channel_data.values()
        )

        # Calculate metrics
        total_top_level = sum(
            len([c for c in v.get('comments', []) if not c.get('is_reply', False)])
            for channel_data in all_comments.values()
            for v in channel_data.values()
        )
        total_replies = total_comments - total_top_level
        reply_ratio = (total_replies / total_comments * 100) if total_comments > 0 else 0

        quota_percentage = (self.quota_manager.quota_used / 10000) * 100

        print(f"\n🔥 OPTIMIZED COLLECTION COMPLETE!")
        print(f"💬 Total comments: {total_comments:,}")
        print(f"📊 Top-level comments: {total_top_level:,} ({100 - reply_ratio:.1f}%)")
        print(f"📝 Quality replies: {total_replies:,} ({reply_ratio:.1f}%)")
        print(f"💰 Total quota used: {self.quota_manager.quota_used}/10,000 ({quota_percentage:.1f}%)")
        print(
            f"⚡ Collection efficiency: {total_comments / total_quota_used:.1f} comments/quota unit" if total_quota_used > 0 else "⚡ Collection efficiency: N/A")

        # Phase-wise breakdown
        priority_comments = 0
        balanced_comments = 0

        for channel_id, channel_data in all_comments.items():
            channel_info = videos_data[channel_id]['channel_info']
            channel_comments = sum(len(v.get('comments', [])) for v in channel_data.values())

            if self.is_unlimited_priority_channel(channel_info['title']):
                priority_comments += channel_comments
                print(f"🚀 PRIORITY (Top 5): {channel_info['title']}: {channel_comments:,} comments")
            else:
                balanced_comments += channel_comments
                print(f"⚖️ BALANCED (Top 2): {channel_info['title']}: {channel_comments:,} comments")

        print(f"\n📊 Optimized Results:")
        print(f"🚀 Phase 1 (Top 5 Replies): {priority_comments:,} comments")
        print(f"⚖️ Phase 2 (Top 2 Replies): {balanced_comments:,} comments")
        print(f"🎯 Benefits: 200 page limit prevents video domination")
        print(f"📈 Enhanced: {self.max_pages_per_round} pages/round, {self.videos_per_round} videos/round")

        return all_comments
