from googleapiclient.discovery import build
from settings import CREDENTIALS_PATH, YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION

class YouTubeAuthenticator:
    def __init__(self):
        self.youtube_service = None
        self.api_key = self._load_api_key()

    def _load_api_key(self):
        """Load API key from file."""
        try:
            with open(CREDENTIALS_PATH, 'r') as f:
                api_key = f.read().strip()
                if not api_key:
                    raise ValueError("API key is empty")
                return api_key
        except FileNotFoundError:
            raise FileNotFoundError(f"API key file not found at {CREDENTIALS_PATH}")
        except Exception as e:
            raise Exception(f"Error loading API key: {e}")

    def get_service(self):
        """Get authenticated YouTube service using API key."""
        if not self.youtube_service:
            try:
                self.youtube_service = build(
                    YOUTUBE_API_SERVICE_NAME,
                    YOUTUBE_API_VERSION,
                    developerKey=self.api_key
                )
                print("✔ YouTube API service initialized with API key")
            except Exception as e:
                raise Exception(f"Failed to initialize YouTube service: {e}")

        return self.youtube_service

    def test_connection(self):
        """Test API connection."""
        try:
            service = self.get_service()
            # Test with a simple API call
            response = service.videos().list(
                part='snippet',
                id='dQw4w9WgXcQ',  # Rick Roll video ID for testing
                maxResults=1
            ).execute()

            if response.get('items'):
                print("✔ API connection test successful")
                return True
            else:
                print("❌ API connection test failed - no response items")
                return False

        except Exception as e:
            print(f"❌ API connection test failed: {e}")
            return False
