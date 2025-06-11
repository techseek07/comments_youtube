import time
import json
from datetime import datetime
from settings import QUOTA_LIMIT_PER_DAY, RATE_LIMIT_DELAY, QUOTA_COSTS

class QuotaManager:
    def __init__(self):
        self.quota_file = 'quota_usage.json'
        self.quota_used = 0
        self.last_reset = datetime.now().date()
        self.operation_log = []
        self.load_quota_state()

    def load_quota_state(self):
        """Load quota usage from file."""
        try:
            with open(self.quota_file, 'r') as f:
                data = json.load(f)
                self.quota_used = data.get('quota_used', 0)
                last_reset_str = data.get('last_reset', str(datetime.now().date()))
                self.last_reset = datetime.strptime(last_reset_str, '%Y-%m-%d').date()
                self.operation_log = data.get('operation_log', [])

            # Reset quota if it's a new day
            if self.last_reset < datetime.now().date():
                self.quota_used = 0
                self.operation_log = []
                self.last_reset = datetime.now().date()
                self.save_quota_state()

        except FileNotFoundError:
            self.save_quota_state()

    def save_quota_state(self):
        """Save quota usage to file."""
        data = {
            'quota_used': self.quota_used,
            'last_reset': str(self.last_reset),
            'operation_log': self.operation_log[-100:]  # Keep last 100 operations
        }
        with open(self.quota_file, 'w') as f:
            json.dump(data, f, indent=2)  # type: ignore

    def check_quota(self, operation_type='generic', cost=1):
        """Check if quota allows for operation."""
        actual_cost = QUOTA_COSTS.get(operation_type, cost)
        return self.quota_used + actual_cost <= QUOTA_LIMIT_PER_DAY

    def use_quota(self, operation_type='generic', cost=1, description=''):
        """Use quota and apply rate limiting."""
        actual_cost = QUOTA_COSTS.get(operation_type, cost)

        if not self.check_quota(operation_type, cost):
            raise Exception(f"Daily quota limit exceeded. Used: {self.quota_used}/{QUOTA_LIMIT_PER_DAY}")

        self.quota_used += actual_cost

        # Log the operation
        self.operation_log.append({
            'timestamp': datetime.now().isoformat(),
            'operation': operation_type,
            'cost': actual_cost,
            'description': description,
            'total_used': self.quota_used
        })

        self.save_quota_state()
        time.sleep(RATE_LIMIT_DELAY)  # Rate limiting

    def get_remaining_quota(self):
        """Get remaining quota for today."""
        return QUOTA_LIMIT_PER_DAY - self.quota_used

    def estimate_channel_quota(self, video_count=50, avg_comments_per_video=20):
        """Estimate quota needed for a channel analysis."""
        estimated_quota = (
                QUOTA_COSTS['search'] +  # Channel resolution
                QUOTA_COSTS['search'] +  # Video search
                QUOTA_COSTS['videos_list'] * (video_count // 50 + 1) +  # Video details
                QUOTA_COSTS['comment_threads'] * video_count +  # Comment threads
                QUOTA_COSTS['comments_list'] * 10  # Additional replies for top comments
        )
        return estimated_quota

    def get_quota_summary(self):
        """Get detailed quota usage summary."""
        operations_summary = {}
        for log in self.operation_log:
            op_type = log['operation']
            if op_type not in operations_summary:
                operations_summary[op_type] = {'count': 0, 'total_cost': 0}
            operations_summary[op_type]['count'] += 1
            operations_summary[op_type]['total_cost'] += log['cost']

        return {
            'total_used': self.quota_used,
            'remaining': self.get_remaining_quota(),
            'percentage_used': (self.quota_used / QUOTA_LIMIT_PER_DAY) * 100,
            'operations_summary': operations_summary,
            'estimated_channels_remaining': self.get_remaining_quota() // self.estimate_channel_quota()
        }
