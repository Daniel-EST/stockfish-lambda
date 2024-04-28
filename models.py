class S3Record:
    """Instantiated record from an S3 Event"""
    def __init__(self, event: dict):
        self.event_name = event["Records"][0]["eventName"]
        self.bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        self.object = event["Records"][0]["s3"]["object"]["key"]
