from bs4 import BeautifulSoup
import pandas as pd

class CommentaryParser:
    @staticmethod
    def parse_commentary(html_content):
        soup = BeautifulSoup(html_content, "html.parser")
        data = []
        blocks = soup.find_all("div", class_="ds-text-tight-m ds-font-regular ds-flex ds-px-3 ds-py-2 "
                                             "lg:ds-px-4 lg:ds-py-[10px] ds-items-start ds-select-none lg:ds-select-auto")
        for block in blocks:
            span_texts = [span.get_text(strip=True) for span in block.find_all("span")]
            p_texts = [p.get_text(strip=True) for p in block.find_all("p")]
            all_text = span_texts + p_texts
            data.append(all_text)
        return data

    @staticmethod
    def extract_bowler_batsman(event):
        try:
            parts = event.split(' to ')
            bowler = parts[0].strip() if len(parts) > 1 else None
            batsman_part = parts[1].split(',')[0].strip() if len(parts) > 1 else None
            return pd.Series([bowler, batsman_part])
        except Exception:
            return pd.Series([None, None])

    @staticmethod
    def to_dataframe(parsed_data):
        df = pd.DataFrame(parsed_data)
        if df.shape[1] < 7:
            return pd.DataFrame()

        df[5] = df.apply(lambda row: row[6] if pd.isna(row[5]) or row[5] == "" else row[5], axis=1)
        df = df.drop([1, 4, 6], axis=1)
        df.columns = ["Ball", "Event", "Score", "Commentary"]

        # ✅ Use static method here
        df[['Bowler', 'Batsman']] = df['Event'].apply(CommentaryParser.extract_bowler_batsman)

        return df
