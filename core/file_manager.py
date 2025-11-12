import os
import json

class FileManager:
    @staticmethod
    def save_html(content, path):
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

    @staticmethod
    def save_json(data, path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

    @staticmethod
    def save_csv(df, path):
        df.to_csv(path, index=False)

    @staticmethod
    def make_folder(path):
        os.makedirs(path, exist_ok=True)
