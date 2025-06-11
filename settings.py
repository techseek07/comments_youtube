import os
from pathlib import Path

# Project paths - UPDATED for better file management
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / 'data'

# OPTION: Use custom folder for large files (uncomment if needed)
# DATA_DIR = Path.home() / "Documents" / "NEET_Comments_Analysis"

RAW_DATA_DIR = DATA_DIR / 'raw'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'
ANALYSIS_DATA_DIR = DATA_DIR / 'analysis'
CREDENTIALS_PATH = PROJECT_ROOT / 'credentials' / 'api_key.txt'

# Create directories
for dir_path in [RAW_DATA_DIR, PROCESSED_DATA_DIR, ANALYSIS_DATA_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# YouTube API settings
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'

# Target channels for NEET analysis
TARGET_CHANNELS = [
    "https://www.youtube.com/@NEETprep",
    "https://www.youtube.com/@NEETspot",
    "https://www.youtube.com/@PW-NEETWallah",
    "https://www.youtube.com/@neetadda247",
    "https://www.youtube.com/@UnacademyNEET",
    "https://www.youtube.com/@MotionNEET",
    "https://www.youtube.com/@ALLENNEET",
    "https://www.youtube.com/@Aakash_NEET",
     "https://www.youtube.com/@Siwanidusad",
    "https://www.youtube.com/@vaibhavdeshmukhneet",
    "https://www.youtube.com/@DoctorSiblings",
    "https://www.youtube.com/@RASHMIAIIMSDELHI1722",
    "https://www.youtube.com/@ishitakhurana20",
    "https://www.youtube.com/@AnujPachhel",
    "https://www.youtube.com/@shivamrajaiims",
    "https://www.youtube.com/@aiimspioneer",
    "https://www.youtube.com/@DRSKSINGHMBBS",
    "https://www.youtube.com/@mitaliunfiltered", 
    "https://www.youtube.com/@qualityspeaks",
    "https://www.youtube.com/@harjassinghaiims",
    "https://www.youtube.com/@DoctorSiblings"
]

# Enhanced keyword segmentation
TARGET_KEYWORDS = {
    'positive_feedback': {
        'loved_it': ['loved it', 'love it', 'loved this', 'love this'],
        'great': ['great', 'amazing', 'awesome', 'excellent', 'fantastic', 'wonderful'],
    },
    'negative_feedback': {
        'boring': ['boring', 'bored', 'dull', 'bekar', 'tedious'],
    },
    'educational_content': {
        'explanation': ['explanation', 'explanations', 'explained', 'explaining'],
        'explain': ['explain', 'explains', 'clarify', 'clarification', 'understand'],
    },
    'brand_mentions': {
        'physics_wallah': ['physics wallah', 'pw', 'physicswallah', 'physics wala'],
        'neetprep': ['neetprep', 'neet prep', 'neet preparation'],
        'neet': ['neet', 'NEET', 'neet exam', 'NEET exam', 'neet 2025', 'NEET 2025'],
    }
}

# Flatten keywords for easy searching
ALL_KEYWORDS = {}
for category, keywords in TARGET_KEYWORDS.items():
    for main_keyword, variations in keywords.items():
        ALL_KEYWORDS[main_keyword] = variations

# OPTIMIZED Rate limiting and quota - AGGRESSIVE SETTINGS
MAX_VIDEOS_PER_CHANNEL = 150
MAX_COMMENTS_PER_REQUEST = 100
QUOTA_LIMIT_PER_DAY = 10000
RATE_LIMIT_DELAY = 0.1  # Reduced from 0.5

# OPTIMIZED Retry settings
MAX_RETRIES = 5
BACKOFF_FACTOR = 2
RETRY_STATUS_CODES = [403, 429, 500, 502, 503, 504]

# OPTIMIZED Top comments analysis
TOP_COMMENTS_COUNT = 10
REPLIES_PER_TOP_COMMENT = 3
LIKE_WEIGHT = 0.7
REPLY_WEIGHT = 0.3
MIN_COMMENTS_THRESHOLD = 5

# Text processing
ENCODING = 'utf-8'

# FILE SPLITTING SETTINGS - NEW
SPLIT_FILES_BY_CHANNEL = True  # Enable channel-specific files
MAX_FILE_SIZE_MB = 5  # Split if file exceeds 5MB
COMPRESS_LARGE_FILES = True  # Enable compression for large files

# Quota costs
QUOTA_COSTS = {
    'search': 100,
    'videos_list': 1,
    'comment_threads': 1,
    'comments_list': 1,
    'channel_list': 1
}
