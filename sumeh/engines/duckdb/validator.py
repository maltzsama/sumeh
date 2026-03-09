
class ValidatedDuckDBDataFrame:
    def __init__(self, good_df, bad_df):
        self._good = good_df
        self._bad = bad_df

    def split_by_errors(self, error_column=None):
        return self._good, self._bad

    @property
    def columns(self):
        return self._good.columns

    @property
    def shape(self):
        return (len(self._good) + len(self._bad), len(self._good.columns))

    def __len__(self):
        return len(self._good) + len(self._bad)