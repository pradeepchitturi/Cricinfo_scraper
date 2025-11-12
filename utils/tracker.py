import json
import os

class MatchTracker:
    def __init__(self, filename="downloaded_matches.json"):
        self.filename = filename
        self.downloaded = set()
        self._load()

    def _load(self):
        if os.path.exists(self.filename):
            if os.stat(self.filename).st_size == 0:
                self.downloaded = set()
            else:
                with open(self.filename, "r") as f:
                    self.downloaded = set(json.load(f))
        else:
            self.downloaded = set()

    def add(self, match_id):
        self.downloaded.add(match_id)
        self._save()

    def exists(self, match_id):
        return match_id in self.downloaded

    def _save(self):
        with open(self.filename, "w") as f:
            json.dump(list(self.downloaded), f, indent=4)
