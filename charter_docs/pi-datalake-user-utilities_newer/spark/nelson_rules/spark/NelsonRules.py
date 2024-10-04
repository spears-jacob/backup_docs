from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from yaml import FullLoader, load

class NelsonRules:
    def __init__(self, path) -> None:
        conf = load(open(path, "r", encoding="utf-8").read(), Loader=FullLoader)
        if conf.get("rules") is None:
            self.__rules = conf.get("defaults").get("rules")
            self.__window =  conf.default("defaults").get("window")

    def compute_nelson_rules(self, df: DataFrame, date_col: str, value_col: str):
        # if df.count() < 10:
        #     return None
        # if ["R4", "R7"] in self.__rules.keys():
        #     if self.__rules.get("R7_param") and df.count() < self.__rules.get("R7_param"):
        #         self.__rules["R7_param"] = df.count()
        #     if self.__rules.get("R4_param") and df.count() < self.__rules.get("R4_param"):
        #         self.__rules["R4_param"] = df.count()

        self.date_col = date_col
        self.value_col = value_col

        resultDF = (
            df.select(value_col, date_col)
            .transform(self.set_time_stats, value_col)
        )

        for rule in self.__rules.keys():
            match rule:
                case "R1":
                    resultDF = resultDF.transform(
                        self.set_R1,
                        value_col,
                        date_col,
                        self.__rules.get("R1_std", 3),
                        self.__rules.get("R1_recent_days", 3),
                    )
                case "R2":
                    resultDF = resultDF.transform(
                        self.set_R2,
                        value_col,
                        date_col,
                        self.__rules.get("R2_param", 9),
                        self.__rules.get("R2_recent_days", 10),
                        self.__rules.get("R2_continous", False)
                    )
                case "R4":
                    resultDF = resultDF.transform(
                        self.set_R4,
                        value_col,
                        date_col,
                        self.__rules.get("R4_param", 14),
                        self.__rules.get("R4_recent_days", 20),
                        self.__rules.get("R4_continous", False)
                    )
                case "R5":
                    resultDF = resultDF.transform(
                        self.set_R5,
                        value_col,
                        date_col,
                        self.__rules.get("R5_param", 2),
                        self.__rules.get("R5_recent_days", 4),
                        self.__rules.get("R5_continous", True)
                    )
                case "R6":
                    resultDF = resultDF.transform(
                        self.set_R6,
                        value_col,
                        date_col,
                        self.__rules.get("R6_param", 4),
                        self.__rules.get("R6_recent_days", 6),
                        self.__rules.get("R6_continous", True)
                    )
                case "R7":
                    resultDF = resultDF.transform(
                        self.set_R7,
                        value_col,
                        date_col,
                        self.__rules.get("R7_param", 15),
                        self.__rules.get("R7_recent_days", 20),
                        self.__rules.get("R7_continous", False)
                    )
                case "R8":
                    resultDF = resultDF.transform(
                        self.set_R8,
                        value_col,
                        date_col,
                        self.__rules.get("R8_param", 8),
                        self.__rules.get("R8_recent_days", 12),
                        self.__rules.get("R8_continous", False)
                    )
                case "R9":
                    resultDF = resultDF.transform(
                        self.set_R9,
                        value_col,
                        date_col,
                        self.__rules.get("R9_param", 1),
                        self.__rules.get("R9_recent_days", 2),
                        self.__rules.get("R9_continous", False)
                    )

        if "R3" in self.__rules.keys():
            r3df = (df
                .select(value_col, date_col)
                .transform(self.set_time_stats, value_col)
                .sort(date_col))
            window = Window.orderBy(date_col).rowsBetween(-1, 1)
            r3df = (
                r3df
                .withColumn(value_col, F.mean(value_col).over(window))
                .transform(
                    self.set_R3,
                    value_col,
                    date_col,
                    self.__rules.get("R3_param", 6),
                    self.__rules.get("R3_recent_days", 6),
                    self.__rules.get("R3_continous", True)
                )
            )
            resultDF = resultDF.join(r3df.select(date_col, "R3"), on=date_col, how="left")

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
        return resultDF.select(keep)


    def set_time_stats(self, df: DataFrame, value_col: str):
        dfG = df.select(F.mean(value_col).alias("mean"), F.stddev(value_col).alias("std"))
        mean = dfG.select("mean").first()[0]
        std = dfG.select("std").first()[0]
        df = df.withColumns({
            "mean": F.lit(mean),
            "std": F.lit(std),
            'index': F.row_number().over(Window.orderBy("date"))
            })
        df = df.withColumns({
            "StdD": df[value_col] / df["std"],
            "LStd1": df["mean"] - df["std"],
            "LStd2": df["mean"] - 2 * df["std"],
            "LStd3": df["mean"] - 3 * df["std"],
            "UStd1": df["mean"] + df["std"],
            "UStd2": df["mean"] + 2 * df["std"],
            "UStd3": df["mean"] + 3 * df["std"],
            "MSign": F.signum(df[value_col] - df["mean"]),
        }).withColumn("MLag", F.col("MSign") == F.lag("MSign", -1).over(Window.orderBy("date")))
        return df


    def set_R1(
        self,
        df: DataFrame,
        value_col: str,
        date_col: str,
        std: float,
        recent_days: int,
    ):
        """
        First Nelson Rule: One point is more than 3 standard deviations from the mean.
        """
        LStd = df["mean"] - std * df["std"]
        UStd = df["mean"] + std * df["std"]
        max_date = df.select(F.date_sub(F.max(date_col), recent_days)).first()[0]
        df = df.withColumn("R1",
                           F.when(
                               (F.col(date_col) > max_date)
                           & ((F.col(value_col) >= UStd) | (F.col(value_col) <= LStd)),
                           F.col(value_col)).otherwise(F.lit(None)))
        return df


    def set_R2(
        self,
        df: DataFrame,
        value_col: str,
        date_col: str,
        R2_param: int,
        recent_days: int,
        continuous: bool,
    ):
        """
        Second Nelson Rule: Nine (or more) points in a row are on the same side of the mean.
        """
        window = Window.orderBy(date_col)
        max_date = df.select(F.date_sub(F.max(date_col), recent_days)).first()[0]
        df = df.withColumn("recent_days", F.col(date_col) > max_date)
        df = df.transform(self.rolling_same_sign, "MLag", R2_param)
        df = df.transform(self.recent_events, R2_param, continuous)
        df = df.withColumn("R2", F.when(F.col("FinalCheck") == True, F.col(value_col)).otherwise(F.lit(None)))
        return df.drop("recent_days", "count", "surpass", "FinalCheck")


    def set_R3(
        self,
        df: DataFrame,
        value_col: str,
        date_col: str,
        R3_param: int,
        recent_days: int,
        continuous: bool,
    ):
        """
        Third Nelson Rule: Six (or more) points in a row are continually increasing (or decreasing).
        """
        w = Window.orderBy(date_col)
        max_date = df.select(F.date_sub(F.max(date_col), recent_days)).first()[0]
        df = (df.withColumns({
            "lw": F.col(value_col) < F.lag(value_col, -1).over(w),
            "gt": F.col(value_col) > F.lag(value_col, -1).over(w),
        })
        .transform(self.rolling_same_sign, "lw", R3_param, "Rlw")
        .transform(self.rolling_same_sign, "gt", R3_param, "Rgt")
        .withColumn("trend", F.col("Rlw") | F.col("Rgt"))
        .withColumn("recent_days", F.col(date_col) > max_date)
        .transform(self.recent_events, R3_param, continuous, "trend")
        .withColumn("R3", F.when(F.col("FinalCheck") == True, F.col(value_col)).otherwise(F.lit(None)))
        )
        return df


    def set_R4(
        self,
        df: DataFrame,
        value_col: str,
        date_col: str,
        R4_param: int,
        recent_days: int,
        continuous: bool,
    ):
        """
        Fourth Nelson Rule: Fourteen (or more) points in a row alternate in direction,
        increasing then decreasing.
        """
        w = Window.orderBy(date_col)
        max_date = df.select(F.date_sub(F.max(date_col), recent_days)).first()[0]
        df = (
            df
            .withColumn("VDiff", F.signum(F.lag(F.col(value_col), -1).over(w) - F.col(value_col)))
            .withColumn("Alt", F.col("VDiff") != F.lag(F.col("VDiff"), 1).over(w))
            .withColumn("Alt", F.when(F.col("Alt").isNull(), F.lag(F.col("Alt"), 2).over(w)).otherwise(F.col("Alt")))
            .transform(self.rolling_same_sign, "Alt", R4_param)
            .withColumn("recent_days", F.col(date_col) > max_date)
            .transform(self.recent_events, R4_param, continuous)
            .withColumn("R4", F.when(F.col("FinalCheck") == True, F.col(value_col)).otherwise(F.lit(None)))
        )
        return df.drop("VDiff", "Alt", "count", "surpass", "recent_days", "FinalCheck")

    def set_R5(
        self,
        df: DataFrame,
        value_col: str,
        date_col: str,
        R5_param: int,
        recent_days: int,
        continuous: bool,
    ):
        """
        Fifth Nelson Rule: Two (or three) out of three points in a row are more than 2
        standard deviations from the mean in the same direction.
        """
        max_date = df.select(F.date_sub(F.max(date_col), recent_days)).first()[0]
        df = (
            df.withColumns({
                "gt": F.col(value_col) >= F.col("UStd2"),
                "lw": F.col(value_col) <= F.col("LStd2"),
            })
            .transform(self.rolling_same_sign, "gt", R5_param, "gtrolled")
            .transform(self.rolling_same_sign, "lw", R5_param, "lwrolled")
            .withColumn("recent_days", F.col(date_col) > max_date)
            .transform(self.rolling_same_sign, "MLag", R5_param)
            .withColumn("preR5", (F.col("gtrolled") | F.col("lwrolled")) & F.col("surpass"))
            .transform(self.recent_events, R5_param, continuous, "preR5")
            .withColumn("R5", F.when(F.col("FinalCheck") == True, F.col(value_col)).otherwise(F.lit(None)))
        )
        return df.drop("gt", "lw", "gtrolled", "lwrolled", "preR5", "count", "surpass", "recent_days", "FinalCheck")


    def set_R6(
        self,
        df: DataFrame,
        value_col: str,
        date_col: str,
        R6_param: int,
        recent_days: int,
        continuous: bool,
    ):
        """
        Sixth Nelson Rule: Four (or five) out of five points in a row are more than 1 Std
        from the mean in the same direction.
        """
        max_date = df.select(F.date_sub(F.max(date_col), recent_days)).first()[0]
        df = (
            df.withColumns({
                "gt": F.col(value_col) >= F.col("UStd1"),
                "lw": F.col(value_col) <= F.col("LStd1"),
            }).transform(self.rolling_same_sign, "gt", R6_param, "gtrolled")
            .transform(self.rolling_same_sign, "lw", R6_param, "lwrolled")
            .withColumn("recent_days", F.col(date_col) > max_date)
            .transform(self.rolling_same_sign, "MLag", R6_param)
            .withColumn("preR6", (F.col("gtrolled") | F.col("lwrolled")) & F.col("surpass"))
            .transform(self.recent_events, R6_param, continuous, "preR6")
            .withColumn("R6", F.when(F.col("FinalCheck") == True, F.col(value_col)).otherwise(F.lit(None)))
            )
        return df.drop("gt", "lw", "gtrolled", "lwrolled", "preR6", "count", "surpass", "recent_days", "FinalCheck")


    def set_R7(
        self,
        df: DataFrame,
        value_col: str,
        date_col: str,
        R7_param: int,
        recent_days: int,
        continuous: bool,
    ):
        """
        Seventh Nelson Rule: Fifteen points in a row are all within 1 standard deviation
        of the mean on either side of the mean.
        """
        max_date = df.select(F.date_sub(F.max(date_col), recent_days)).first()[0]
        df = (
            df.withColumn("in_range", 
                          (F.col(value_col) >= F.col("LStd1")) & (F.col(value_col) <= F.col("UStd1")),
            ).withColumn("preR7", F.when(F.col("in_range") == True, F.lit(1)).otherwise(F.lit(None)))
            .withColumn("R7Sum", F.sum(F.col("preR7")).over(Window.orderBy(F.col(date_col)).rowsBetween(-R7_param, 0)))
            .withColumns({
                "recent_days": F.col(date_col) > max_date,
                "preR7": F.col("R7Sum") >= R7_param
                })
            .transform(self.recent_events, R7_param, continuous, "preR7")
            .withColumn("R7", F.when(F.col("FinalCheck") == True, F.col(value_col)).otherwise(F.lit(None)))
        )
        return df.drop("in_range", "preR7", "R7Sum", "recent_days", "FinalCheck")


    def set_R8(
        self,
        df: DataFrame,
        value_col: str,
        date_col: str,
        R8_param: int,
        recent_days: int,
        continuous: bool,
    ):
        """
        Eighth Nelson Rule: Eight points in a row exist, but none within 1
        standard deviation
        """
        max_date = df.select(F.date_sub(F.max(date_col), recent_days)).first()[0]
        df = (
            df.withColumn("in_range",
                          F.col(value_col) < F.col("LStd1") | F.col(value_col) > F.col("UStd1")
            ).withColumn("preR8", F.when(F.col("in_range") == True, F.lit(1)).otherwise(F.lit(None)))
            .withColumn("R8Sum", F.sum(F.col("preR8")).over(Window.orderBy(F.col(date_col)).rowsBetween(-R8_param, 0)))
            .withColumns({
                "recent_days": F.col(date_col) > max_date,
                "preR8": F.col("R8Sum") >= R8_param
            })
            .transform(self.recent_events, R8_param, continuous, "preR8")
            .withColumn("R8", F.when(F.col("FinalCheck") == True, F.col(value_col)).otherwise(F.lit(None)))
        )
        return df.drop("in_range", "preR8", "R8Sum", "recent_days", "FinalCheck")

    def set_R9(
        self,
        df: DataFrame,
        value_col: str,
        date_col: str,
        R9_param: int,
        recent_days: int,
        continuous: bool,
    ):
        """
        Eighth Nelson Rule: Eight points in a row exist, but none within 1
        standard deviation
        """
        max_date = df.select(F.date_sub(F.max(date_col), recent_days)).first()[0]
        df = (
            df.withColumns({
                "R9": F.when(F.col(value_col).isNull() | (F.col(value_col) == 0), True).otherwise(False),
                "recent_days": F.col(date_col) > max_date,
            }).transform(self.recent_events, R9_param, continuous, "R9")
            .withColumn("R9", F.when(F.col("FinalCheck") == True, F.col(value_col)).otherwise(F.lit(None)))
        )
        return df.drop("recent_days", "FinalCheck")


    def completeRolling(self, df, value_col, threshold, sum_col_name = "rolling_sum"):
        w = Window.orderBy("index").rowsBetween(1 - threshold, 0)
        rolling_sum = F.sum(value_col).over(w).alias(sum_col_name)
        return df.withColumn(sum_col_name, rolling_sum)


    def rolling_same_sign(self, mdf, column, threshold, column_name="surpass"):
        mdf = mdf.withColumns({
            column: F.col(column).cast("int")
            })
        window_spec = Window.orderBy("index")
        # Create a column that increases when the value changes
        change = (F.col(column) != F.lag(F.col(column), 1).over(window_spec)).cast("int")
        mdf = mdf.withColumn("change", change)
        group = F.sum(change).over(window_spec).alias("group")
        mdf = mdf.withColumn("group", group)
        # Count the number of true values in each group
        count = F.sum(F.col(column)).over(Window.partitionBy("group").orderBy("index"))
        mdf = mdf.withColumn("count", count).withColumn(column_name, F.col("count") >= threshold)
        if mdf.agg(F.max(F.col(column_name))).first()[0] == True:
            idx = mdf.filter(column_name).select("index").first()[0]
            return (mdf
                    .withColumn(column_name, F.when(F.col("index") > idx - threshold, True).otherwise(False))
                .drop("change", "group"))
        else: return mdf.drop("change", "group")


    def recent_events(self, df, threshold, continuous=False, surpass_column="surpass"):
        if continuous:
            x = df.select(F.sum((F.col("recent_days") & F.col(surpass_column)).cast("int"))).first()[0]
            if x >= threshold:
                return df.withColumn("FinalCheck", F.col("recent_days") & F.col(surpass_column))
            else:
                return df.withColumn("FinalCheck", F.lit(False))
        else:
            return df.withColumn("FinalCheck", F.col("recent_days") & F.col(surpass_column))
