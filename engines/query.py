import pandas as pd
import pyodbc
import numpy as np
import datetime


class QueryEngine:

    @property
    def researchScienceConnectionString(self):
        return self.__researchScienceConnectionString

    @property
    def bazookaAnalyticsConnString(self):
        return self.__bazookaAnalyticsConnString

    @property
    def bazookaReplConnString(self):
        return self.__bazookaReplConnString

    def __init__(self, researchScienceConnectionString, bazookaAnalyticsConnString, bazookaReplConnString):
        self.__researchScienceConnectionString = researchScienceConnectionString
        self.__bazookaAnalyticsConnString = bazookaAnalyticsConnString
        self.__bazookaReplConnString = bazookaReplConnString

    def update_hist_load(self):
        cn = pyodbc.connect(self.__researchScienceConnectionString)
        # sql = "{call Research.sp_Delay_weeklyupdate_setting(?,?)}"
        # df = pd.read_sql(sql = sql, con = cn, params=(startDate,endDate,))
        sql_query = "{call Research.dbo.sp_Delay_weeklyupdate_setting}"
        df = pd.read_sql(sql=sql_query, con=cn)
        return df

    def get_hist_load(self):
        # if __name__ == "__main__":
        cn = pyodbc.connect(self.__researchScienceConnectionString)
        sql_query = """
        declare @holiday_start_2017 as date='2017-11-19'
        declare @holiday_start_2018 as date='2018-11-18'
        declare @holiday_end_2017 as date='2018-01-02'
        declare @holiday_end_2018 as date='2019-01-02'

        select top 20000
        loadid,stopsequence,ontime,loadtype,numstops,carrier_rate,facility_rate,customer_rate,prestop_ontime,prestop_duration,distance,appttype
        ,case when datediff(minute, booktime_utc, appt)<=0 then 1 else 0 end 'latebooking',
         case when datediff(minute,dispatch_time_adjusted,appt) <= 0 then 1 else 0 end 'latedispatch' 
        , case when (convert(date,appt) between @holiday_start_2017 and @holiday_end_2017) or (convert(date,appt) between @holiday_start_2018 and @holiday_end_2018) then 1 else 0 end 'holiday_season'
        , datepart(WEEKDAY,appt) 'day_of_week'
        from ResearchScience.dbo.delay_histload_train
        order by loadid, stopsequence
        """
        df_hist_load = pd.read_sql(sql=sql_query, con=cn)  # 30 seconds
        return (df_hist_load)

    def get_dynamic_load(self):
        # if __name__ == "__main__":
        cn = pyodbc.connect(self.__researchScienceConnectionString)
        sql_query = """
        declare @holiday_start_2017 as date='2017-11-19'
        declare @holiday_start_2018 as date='2018-11-18'
        declare @holiday_end_2017 as date='2018-01-02'
        declare @holiday_end_2018 as date='2019-01-02'

        select top 100
        loadid,stopsequence,ontime,loadtype,progresstype,numstops,carrier_rate,facility_rate,customer_rate,prestop_ontime,prestop_duration,distance,appttype
        ,case when datediff(minute, booktime_utc, appt)<=0 then 1 else 0 end 'latebooking',
         case when datediff(minute,dispatch_time_adjusted,appt) <= 0 then 1 else 0 end 'latedispatch'
        , case when (convert(date,appt) between @holiday_start_2017 and @holiday_end_2017) or (convert(date,appt) between @holiday_start_2018 and @holiday_end_2018) then 1 else 0 end 'holiday_season'
        , datepart(WEEKDAY,appt) 'day_of_week'
        from ResearchScience.dbo.Delay_DailyLoad 
        order by loadid, stopsequence   
        """
        df_daily_load = pd.read_sql(sql=sql_query, con=cn)
        return (df_daily_load)

    def get_dynamic_load_1(self):
        # might be no difference between iloc and loc, as we have order by in sql query. but need to be verified
        # use iloc will guarantee that the sequence of load and stopsequence that we want and we arranged.
        cn = pyodbc.connect(self.__bazookaAnalyticsConnString)
        sql_query = "{call Research.dbo.sp_Delay_dailyload}"

        df_daily_load = pd.read_sql(sql=sql_query, con=cn)
        # duration in minute
        return df_daily_load

    def get_newload_oldversion(self, startDate, endDate):
        cn = pyodbc.connect(self.__bazookaReplConnString)
        sql = "{call Research.spLoad_GetNonUPSActiveLoadsForResearchMatching(?,?)}"
        df = pd.read_sql(sql=sql, con=cn, params=(startDate, endDate,))
        return df