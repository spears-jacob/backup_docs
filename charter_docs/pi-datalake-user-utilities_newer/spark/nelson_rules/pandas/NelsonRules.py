from functools import partial
import pandas as pd
import numpy as np
from yaml import load, FullLoader


class NelsonRules:
    def __init__(self, path) -> None:
        conf = load(open(path, "r", encoding="utf-8").read(), Loader=FullLoader)
        print("Config:",conf, sep="/t")
        if conf.get("rules") is None:
            self.__rules = conf.get("defaults").get("rules")
            self.__window =  conf.get("defaults").get("window")

    def compute_nelson_rules(self, df: pd.DataFrame, date_col: str, value_col: str) -> pd.DataFrame:
        # if df.shape[0] < 10:
        #     return None
        # if ["R4", "R7"] in self.__rules.keys():
        #     if rules.get("R7").get("param") and df.shape[0] < rules.get("R7").get("param"):
        #         rules["R7"]["param"] = df.shape[0]
        #     if rules.get("R4").get("param") and df.shape[0] < rules.get("R4").get("param"):
        #         rules["R4"]["param"] = df.shape[0]

        self.date_col = date_col
        self.value_col = value_col
        
        resultDF = (
            df[[value_col, date_col]]
            .reset_index()
            .pipe(self.set_time_stats)
        )
        
        ruler = {
            "R1": partial(self.set_R1, 
                          std=self.__rules.get("R1").get("std"), 
                          recent_days=self.__rules.get("R1").get("recent_days")),
            "R2": partial(self.set_R2,
                          R2_param=self.__rules.get("R2").get("param"),
                          recent_days=self.__rules.get("R2").get("recent_days"),
                          continuous=self.__rules.get("R2").get("continous")),
            "R4": partial(self.set_R4,
                          R4_param=self.__rules.get("R4").get("param"),
                          recent_days=self.__rules.get("R4").get("recent_days"),
                          continuous=self.__rules.get("R4").get("continous")),
            "R5": partial(self.set_R5,
                          R5_param=self.__rules.get("R5").get("param"),
                          recent_days=self.__rules.get("R5").get("recent_days"),
                          continuous=self.__rules.get("R5").get("continous")),
            "R6": partial(self.set_R6,
                          R6_param=self.__rules.get("R6").get("param"),
                          recent_days=self.__rules.get("R6").get("recent_days"),
                          continuous=self.__rules.get("R6").get("continous")),
            "R7": partial(self.set_R7,
                          R7_param=self.__rules.get("R7").get("param"),
                          recent_days=self.__rules.get("R7").get("recent_days"),
                          continuous=self.__rules.get("R7").get("continous")),
            "R8": partial(self.set_R8,
                          R8_param=self.__rules.get("R8").get("param"),
                          recent_days=self.__rules.get("R8").get("recent_days"),
                          continuous=self.__rules.get("R8").get("continous")),
            "R9": partial(self.set_R9,
                          R9_param=self.__rules.get("R9").get("param"),
                          recent_days=self.__rules.get("R9").get("recent_days"),
                          continuous=self.__rules.get("R9").get("continous")),
        }
        
        for rule in self.__rules.keys():
            if rule == "R3":
                continue
            resultDF = resultDF.pipe(ruler[rule])
        
        if "R3" in self.__rules.keys():
            r3df = df.sort_values(date_col)
            r3df = (
                r3df.set_index(df[date_col])
                .rolling(window=self.__rules.get("R3").get("rolldays", "3D"))
                .agg({value_col: "mean"})
                .reset_index()
                .pipe(
                    self.set_R3,
                    self.__rules.get("R3").get("param", 6),
                    self.__rules.get("R3").get("recent_days", 6),
                    self.__rules.get("R3").get("continous", True)
                )
            )

            resultDF = resultDF.merge(r3df[[date_col, "R3"]], on=date_col, how="left")

        keep = [
            *[
                date_col,
                value_col,
                "mean",
                "std",
                "LStd1",
                "LStd2",
                "LStd3",
                "UStd1",
                "UStd2",
                "UStd3",
            ],
            *self.__rules.keys(),
        ]
        return resultDF[keep]


    def set_time_stats(self, df: pd.DataFrame):
        dfG = df[self.value_col].agg(["mean", "std"])
        df = df.assign(mean=dfG["mean"], std=dfG.loc["std"])
        df = df.assign(
            StdD=df[self.value_col] / df["std"],
            LStd1=df["mean"] - df["std"],
            LStd2=df["mean"] - 2 * df["std"],
            LStd3=df["mean"] - 3 * df["std"],
            UStd1=df["mean"] + df["std"],
            UStd2=df["mean"] + 2 * df["std"],
            UStd3=df["mean"] + 3 * df["std"],
        )
        return df


    def set_R1(
        self,
        df: pd.DataFrame,
        std: float,
        recent_days: int,
    ):
        """
        First Nelson Rule: One point is more than 3 standard deviations from the mean.
        """
        df["R1"] = None
        LStd = df["mean"] - std * df["std"]
        UStd = df["mean"] + std * df["std"]
        df.loc[
            (df[self.date_col] > df[self.date_col].max() - pd.DateOffset(days=recent_days))
            & ((df[self.value_col] >= UStd) | (df[self.value_col] <= LStd)),
            "R1",
        ] = df[self.value_col]
        return df


    def set_R2(
        self,
        df: pd.DataFrame,
        R2_param: int,
        recent_days: int,
        continuous: bool,
    ):
        """
        Second Nelson Rule: Nine (or more) points in a row are on the same side of the mean.
        """
        df["R2"] = None
        df["sign"] = np.sign(df[self.value_col] - df["mean"])
        df["MnV"] = df["sign"] == df["sign"].shift(-1)
        df.loc[df.index[-1], "MnV"] = df.loc[df.index[-2], "MnV"]
        filter_arr = self.recent_events(
            (df[self.date_col] > df[self.date_col].max() - pd.DateOffset(days=recent_days)),
            (self.rolling_same_sign(df["MnV"], R2_param, True, "backward")),
            R2_param, continuous)
        df.loc[
            filter_arr,
            "R2",
        ] = df[self.value_col]
        return df


    def set_R3(
        self,
        df: pd.DataFrame,
        R3_param: int,
        recent_days: int,
        continuous: bool,
    ):
        """
        
        Third Nelson Rule: Six (or more) points in a row are continually increasing (or decreasing).
        """
        df["R3"] = None
        df = df.assign(
            lw=df[self.value_col] < df[self.value_col].shift(-1),
            gt=df[self.value_col] > df[self.value_col].shift(-1),
        )
        df.loc[df.index[-1], ["lw", "gt"]] = df.loc[df.index[-2], ["lw", "gt"]]
        df = df.assign(
            Rlw=self.rolling_same_sign(df["lw"], R3_param, True, "backward"),
            Rgt=self.rolling_same_sign(df["gt"], R3_param, True, "backward"),
        )
        df = df.assign(trend=df["Rlw"] | df["Rgt"])
        filter_arr = self.recent_events(
            df[self.date_col] > df[self.date_col].max() - pd.DateOffset(days=recent_days),
            df["trend"],
            R3_param, continuous)

        df.loc[
            filter_arr,
            "R3"
        ] = df[self.value_col]
        return df


    def set_R4(
        self,
        df: pd.DataFrame,
        R4_param: int,
        recent_days: int,
        continuous: bool,
    ):
        """
        Fourth Nelson Rule: Fourteen (or more) points in a row alternate in direction,
        increasing then decreasing.
        """
        df["R4"] = None
        df["VDiff"] = np.sign(df[self.value_col].shift(-1) - df[self.value_col])
        df["VDiff"] = df["VDiff"].fillna(method="bfill")
        df["Alt"] = df["VDiff"] != df["VDiff"].shift(1)
        df["Alt"] = np.where(df["Alt"].isna(), df["Alt"].shift(2), df["Alt"])
        filter_arr = self.recent_events(
            df[self.date_col] > df[self.date_col].max() - pd.DateOffset(days=recent_days),
            self.rolling_same_sign(df["Alt"], R4_param, True, "backward"),
            R4_param, continuous)
        df.loc[
            filter_arr,
            "R4",
        ] = df[self.value_col]
        return df


    def set_R5(
        self,
        df: pd.DataFrame,
        R5_param: int,
        recent_days: int,
        continuous: bool,
    ):
        """
        Fifth Nelson Rule: Two (or three) out of three points in a row are more than 2
        standard deviations from the mean in the same direction.
        """
        df["R5"] = None
        df = df.assign(
            gt=df[self.value_col] >= df["UStd2"],
            lw=df[self.value_col] <= df["LStd2"],
        )
        df = df.assign(
            gtrolled=self.rolling_same_sign(df["gt"], R5_param, False),
            lwrolled=self.rolling_same_sign(df["lw"], R5_param, False),
        )
        df = df.assign(
            preR5=(df["gtrolled"] | df["lwrolled"])
            & self.rolling_same_sign(df["MnV"], R5_param, False)
        )
        filter_arr = self.recent_events(
            df[self.date_col] > df[self.date_col].max() - pd.DateOffset(days=recent_days),
            df["preR5"],
            R5_param, continuous)
        df.loc[
            filter_arr,
            "R5",
        ] = df[self.value_col]
        return df


    def set_R6(
        self,
        df: pd.DataFrame,
        R6_param: int,
        recent_days: int,
        continuous: bool,
    ):
        """
        Sixth Nelson Rule: Four (or five) out of five points in a row are more than 1 Std
        from the mean in the same direction.
        """
        df["R6"] = None
        df = df.assign(
            gt=df[self.value_col] >= df["UStd1"],
            lw=df[self.value_col] <= df["LStd1"],
        )
        df = df.assign(
            gtrolled=self.rolling_same_sign(df["gt"], R6_param, False),
            lwrolled=self.rolling_same_sign(df["lw"], R6_param, False),
        )
        df = df.assign(
            preR6=(df["gtrolled"] | df["lwrolled"])
            & self.rolling_same_sign(df["MnV"], R6_param, True, "backward")
        )
        
        filter_arr = self.recent_events(
            df[self.date_col] > df[self.date_col].max() - pd.DateOffset(days=recent_days),
            df["preR6"],
            R6_param, continuous)
        df.loc[
            filter_arr,
            "R6",
        ] = df[self.value_col]
        return df


    def set_R7(
        self,
        df: pd.DataFrame,
        R7_param: int,
        recent_days: int,
        continuous: bool,
    ):
        """
        Seventh Nelson Rule: Fifteen points in a row are all within 1 standard deviation
        of the mean on either side of the mean.
        """
        df["R7"] = None
        df = df.assign(
            in_range=(df[self.value_col] >= df["LStd1"]) & (df[self.value_col] <= df["UStd1"]),
        )
        df["preR7"] = None
        df.loc[df["in_range"], "preR7"] = True
        df["R7Sum"] = df["preR7"].rolling(R7_param).sum()
        filter_arr = self.recent_events(
            df[self.date_col] > df[self.date_col].max() - pd.DateOffset(days=recent_days),
            self.completeRolling(df["R7Sum"], R7_param),
            R7_param, continuous)
        df.loc[
            filter_arr,
            "R7",
        ] = df[self.value_col]
        return df


    def set_R8(
        self,
        df: pd.DataFrame,
        R8_param: int,
        recent_days: int,
        continuous: bool,
    ):
        """
        Eighth Nelson Rule: Eight points in a row exist, but none within 1
        standard deviation
        """
        df["R8"] = None
        df = df.assign(
            in_range=(df[self.value_col] < df["LStd1"]) | (df[self.value_col] > df["UStd1"]),
        )
        df["preR8"] = None
        df.loc[df["in_range"], "preR8"] = True
        df["R8Sum"] = df["preR8"].rolling(R8_param).sum()
        filter_arr = self.recent_events(
            df[self.date_col] > df[self.date_col].max() - pd.DateOffset(days=recent_days),
            self.completeRolling(df["R8Sum"], R8_param),
            R8_param, continuous)
        df.loc[
            filter_arr, 
            "R8"] = True
        return df

    def set_R9(
        self,
        df: pd.DataFrame,
        R9_param: int,
        recent_days: int,
        continuous: bool,
    ):
        """
        Eighth Nelson Rule: Eight points in a row exist, but none within 1
        standard deviation
        """
        df["R9"] = None
        df = df.assign(
            R9=(df[self.value_col].isna()) | (df[self.value_col] == 0),
        )
        filter_arr = self.recent_events(
            df[self.date_col] > df[self.date_col].max() - pd.DateOffset(days=recent_days),
            df["R9"],
            R9_param, continuous)
        df.loc[
            filter_arr, 
            "R9"] = True
        return df


    def completeRolling(self, values, threshold):
        result = [False] * len(values)
        for i in range(len(values)):
            if not pd.isna(values[i]) and values[i]:
                for v in range(threshold):
                    result[i - v] = True
        return result


    def rolling_same_sign(self, values, threshold, lagged=True, direction="forward"):
        count = 0
        start_index = None
        indices = []
        values.fillna(False, inplace=True)
        # Create a new array of the same length as the series full with False
        new_array = np.full(len(values), False)
        # Iterate over the series
        for i, value in values.items():
            # If the value is True, increment the count and remember the start index
            if value == True:
                if start_index is None:
                    start_index = i
                count += 1
            # If the value is not True or we reached the end of the series,
            # check if the count surpasses the threshold
            if value != True or i == len(values) - 1:
                if start_index is not None:
                    if count >= threshold:
                        # If the count surpasses the threshold, add the indices to the list
                        indices.append((start_index, i - 1))
                        new_array[start_index:i] = True
                    # Reset the count and the start index
                    count = 0
                    start_index = None

        if lagged:
            return self.turn_true_lagged(new_array, direction)
        else:
            return new_array


    def turn_true_lagged(self, values, direction):
        if direction == "forward":
            for i in range(len(values) - 1):
                if values[i + 1] == True:
                    values[i] = True
            return values
        else:  # direction == "backward"
            for i in range(len(values) - 1, 0, -1):
                if values[i - 1] == True:
                    values[i] = True
            return values


    def create_bool_array(self, indices, size):
        bool_arr = np.full(size, False, dtype=bool)
        for index_range in indices:
            start, end = index_range
            bool_arr[start:end] = True
        return bool_arr


    def agg_nelson_results(self, df: pd.DataFrame, **rules):
        return (
            df.agg(
                {
                    self.date_col: "max",
                    "R1": lambda x: rules.get("rvalue", 5) if x.any() else 1,
                    "R2": lambda x: rules.get("rvalue", 3) if x.any() else 1,
                    "R3": lambda x: rules.get("rvalue", 4) if x.any() else 1,
                    "R4": lambda x: rules.get("rvalue", 2) if x.any() else 1,
                    "R5": lambda x: rules.get("rvalue", 4) if x.any() else 1,
                    "R6": lambda x: rules.get("rvalue", 4) if x.any() else 1,
                    "R7": lambda x: rules.get("rvalue", 3) if x.any() else 1,
                    "R8": lambda x: rules.get("rvalue", 2) if x.any() else 1,
                    "R9": lambda x: rules.get("rvalue", 5) if x.any() else 1,
                }
            )
            .to_frame()
            .T
        )


    def recent_events(self, x: np.ndarray, y: np.ndarray, threshold, continuous=False):
        if continuous:
            if (x & y).sum() >= threshold:
                return (x) & (y)
            else:
                return np.full(len(x), False)
        else:
            return (x) & (y)

    def rolling_alerts(self, df: pd.DataFrame, alert_columns, date_col, threshold):
        def reset_cumsum(series: pd.Series):
            cumsum = series.cumsum()
            reset_points = (series == 0).cumsum()
            cumsum_reset = cumsum - cumsum.groupby(reset_points).transform('first')
            return cumsum_reset
        
        def cut_cases(series: pd.Series):
            return series.apply(lambda x: 0 if x > threshold else x)
        
        df[alert_columns] = df[alert_columns].apply(lambda x: x > 1)
        cumsum_df = df[alert_columns].apply(reset_cumsum)
        cumsum_df = cumsum_df.apply(cut_cases)
        result_df = cumsum_df.apply(lambda x: x > 0)
        result_df = pd.concat([df[date_col], result_df], axis=1)
        return result_df