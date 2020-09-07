import pandas as pd


class TopicCounter:

    def __init__(self, filepath):
        self.filepath = filepath

    def get_raw_text(self):
        with open(self.filepath, 'r') as f:
            return f.read()

    def get_counters(self):
        counts = dict()
        counts_table = {'word': list(), 'count': list()}
        words = filter(lambda item: (item[0].isupper() and len(item) > 3), self.get_raw_text().split())

        for word in words:
            if word in counts:
                counts[word] += 1
            else:
                counts[word] = 1

        for (word, count) in zip(counts.keys(), counts.values()):
            counts_table["word"].append(word)
            counts_table["count"].append(count)

        counters_df = pd.DataFrame.from_dict(counts_table)
        counters_df = counters_df.sort_values(by="count", ascending=False)

        return counters_df
