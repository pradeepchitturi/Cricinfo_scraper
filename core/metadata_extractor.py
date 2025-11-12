from bs4 import BeautifulSoup

class MetadataExtractor:
    @staticmethod
    def extract_metadata(html_content,match_id):
        soup = BeautifulSoup(html_content, "html.parser")
        metadata = {}
        match_details_table = soup.find("table", class_="ds-w-full ds-table ds-table-sm ds-table-auto")
        if not match_details_table:
            return metadata
        rows = match_details_table.find_all("tr")
        for row in rows:
            columns = row.find_all("td")
            if len(columns) == 2:
                key = columns[0].get_text(strip=True)
                value = columns[1].get_text(separator=" ", strip=True)
                metadata[key] = value
            elif len(columns) == 1:
                metadata["Venue"] = columns[0].get_text(strip=True)
        metadata["MatchID"] = match_id
        return metadata
