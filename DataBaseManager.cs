using System;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;
using System.Globalization;
using System.IO;
using System.Text;
using Serilog;
using ControlPanelSpace;
using APIManagerSpace;

namespace DatabaseManagerSpace{
    public static class DataBaseManager{
        //===The path to the SQLite file
        private const string dataBaseFile = "../Data/historical_data.db";

        //Constructor for the Database class
        static DataBaseManager(){
            //===Check if DB file exists, else create it
            if(!File.Exists(dataBaseFile)){
                using(FileStream fs = File.Create(dataBaseFile)){}
            }

            //===Connect to the DB
            using var connection = new SqliteConnection($"Data Source={dataBaseFile};");
            connection.Open();

            //===Initialise Table command
            string tableInitCommand = @"
                CREATE TABLE IF NOT EXISTS historical_prices (
                    timestamp TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume INTEGER NOT NULL,
                    average REAL NOT NULL,
                    total INTEGER NOT NULL,
                    PRIMARY KEY (timestamp, symbol)
                );";

            //===Run command
            using var command = new SqliteCommand(tableInitCommand, connection);
            command.ExecuteNonQuery();
        }

        //===Allows execution of SQL queries by any class
        public static object ExecuteCommand(string query){
            Log.Information($"SQLite Command Execution {query}");
            //===Connect to the DB
            using var connection = new SqliteConnection($"Data Source={dataBaseFile};");
            connection.Open();

            //===Create SQLite command
            using var command = new SqliteCommand(query, connection);

            //===If the command is a SELECT, we'll be returning the result, else return affected row count
            if(query.TrimStart().StartsWith("SELECT")){
                //===Create list to store resulting rows
                var rows = new List<Dictionary<string, object>>();

                //===reader executes the command
                using var reader = command.ExecuteReader();

                //===Loop through each row in result
                while(reader.Read()){
                    //===Create dict for current row
                    var row = new Dictionary<string, object>();

                    //Loop through columns and map values to dict
                    for(int i = 0; i < reader.FieldCount; i++){
                        row[reader.GetName(i)] = reader.GetValue(i);
                    }

                    //Add dict to rows list
                    rows.Add(row);
                }

                //return rows if more than 0 in list, else null
                return rows.Count > 0 ? rows : null;
            }else{
                //Execute command and return affected row count as int
                return command.ExecuteNonQuery();
            }
        }

        public static double GetMinutesBetween(DateTime start, DateTime end){
            return (end - start).TotalMinutes;
        }

        //===Tells us what percentage of the data is filled for each symbol or all if empty list
        public static List<string> DataStatus(List<string> symbols){
            //===Final list of stats to return
            List<string> finalStats = new List<string>();

            //===First we get our time range
            var unparsedStartDate = ControlPanel.GetSetting("DataStart");
            DateTime parsedStartDate = DateTime.ParseExact((string)unparsedStartDate, "yyyy-MM-dd HH:mm", null);
            
            //===Get current time
            DateTime latestTimeSnapshot = ControlPanel.GetCurrentTimeCEST();

            //===Get all market uptimes for the data range
            List<(DateTime start, DateTime end)> totalGaps = TrimMarketDowntime(parsedStartDate, latestTimeSnapshot);

            //===Create counter for minutes expected
            double expectedMinutes = 0;

            //===Fill expected minutes
            foreach((DateTime start, DateTime end) gap in totalGaps){
                expectedMinutes += GetMinutesBetween(gap.start, gap.end);
            }
            


            Log.Information($"expectedMinutes = {expectedMinutes}");

            //===Create a list for the symbols (beacause if param list is empty we need this to fill anyway)
            List<string> symbolsToCheck = new List<string>();

            //===Check if the list is empty
            if(symbols.Count == 0){
                //===Add the full range of symbols to the list
                foreach(string s in ControlPanel.GetTopSymbols((int)ControlPanel.GetSetting("Range"))){
                    symbolsToCheck.Add(s);
                }
            }else{
                //===Shove all those from the param list into the list
                foreach(string s in symbols){
                    symbolsToCheck.Add(s);
                }
            }

            //===Now loop through the final list of symbols
            foreach(string symbol in symbolsToCheck){
                //===Get the gaps for that symbol
                List<(DateTime start, DateTime end)> gaps = GapFinder(symbol);

                //===Create a double to store the amount of missing minutes
                double missingMinutes = 0;

                //===Total up all the missing minutes for that symbol
                foreach((DateTime start, DateTime end) gap in gaps){
                    missingMinutes += GetMinutesBetween(gap.start, gap.end);
                }
                Log.Information($"Missing minutes for {symbol}: {missingMinutes}");

                //===Calculate percentage of data filled  
                double percentageFilled = (expectedMinutes - missingMinutes) / (expectedMinutes / 100);

                //===Add answer to list
                finalStats.Add($"{symbol} percentage complete: {percentageFilled}");             

            }

            return finalStats;
        }

        //===Fills the gaps in the database for a given symbol
        public static async Task UpdateSymbolDataset(string symbol){
            Log.Information($"Updating {symbol}");

            var gapWatch = System.Diagnostics.Stopwatch.StartNew();
            //===Get the gaps for that symbol
            List<(DateTime start, DateTime end)> gapsInData = GapFinder(symbol);

            gapWatch.Stop();
            Log.Information($"Found {gapsInData.Count} gaps in {gapWatch.Elapsed}");

            //===Create list to store retrieved data
            List<(string timestamp, decimal open, decimal high, decimal low, decimal close, long volume, double average, int total)> fullData 
                = new List<(string, decimal, decimal, decimal, decimal, long, double, int)>();

            var fetchWatch = System.Diagnostics.Stopwatch.StartNew();

            //===Fetch all missing data first
            foreach((DateTime start, DateTime end) gap in gapsInData){
                Log.Information($"{gap.start} to {gap.end}");
                var bars = await LaunchDataCall(symbol, gap.start, gap.end, "1Min");
                fullData.AddRange(bars);
            }

            fetchWatch.Stop();

            Log.Information($"Fetched {fullData.Count} bars for {symbol} in {fetchWatch.Elapsed}, now inserting into DB");

            var InsertWatch = System.Diagnostics.Stopwatch.StartNew();
            //===Insert all data into DB in one pass
            using var connection = new SqliteConnection($"Data Source={dataBaseFile};");
            connection.Open();

            using var transaction = connection.BeginTransaction();
            using var command = connection.CreateCommand();

            foreach(var dataPoint in fullData){
                // Use INSERT OR IGNORE to prevent UNIQUE constraint errors
                command.CommandText = @$"
                    INSERT OR IGNORE INTO historical_prices VALUES
                    ('{dataPoint.timestamp}',
                    '{symbol}',
                    {dataPoint.open.ToString(CultureInfo.InvariantCulture)},
                    {dataPoint.high.ToString(CultureInfo.InvariantCulture)},
                    {dataPoint.low.ToString(CultureInfo.InvariantCulture)},
                    {dataPoint.close.ToString(CultureInfo.InvariantCulture)},
                    {dataPoint.volume},
                    {dataPoint.average.ToString(CultureInfo.InvariantCulture)},
                    {dataPoint.total});";
                command.ExecuteNonQuery();
            }

            transaction.Commit();
            InsertWatch.Stop();
            Log.Information($"Inserted data for {symbol} successfully in {InsertWatch.Elapsed}");
        }

        //===WARNING, THE FOLLOWING METHOD POLLUTES THE DATA IF MISUSED, USE ONLY AFTER UpdateSymbolDataset
        //===Artificially fill unfillable gaps (nothing was traded in those periods)
        public static void ArtificialFill(string symbol){
            Log.Information($"[ArtificialFill] Starting artificial gap fill for {symbol}");

            //===Get the gaps for that symbol
            List<(DateTime start, DateTime end)> gaps = GapFinder(symbol);

            //===Open SQLite connection
            using var connection = new SqliteConnection($"Data Source={dataBaseFile};");
            connection.Open();
            using var transaction = connection.BeginTransaction();
            using var insert = connection.CreateCommand();

            //===For each gap
            foreach ((DateTime start, DateTime end) gap in gaps){
                Log.Information($"[ArtificialFill] artificial-filling gap: {gap.start} -> {gap.end}");

                //===Get the edges of the gap
                var before = GetNearestBar(symbol, gap.start.AddMinutes(-1), connection);
                var after  = GetNearestBar(symbol, gap.end, connection);

                //===Make sure there ARE edges, not an open end
                if (before is null || after is null)
                    continue; // cannot interpolate

                //===Current timestamp (used to loop through the minutes in a gap)
                DateTime currentTime = gap.start;

                //===Size of the gap
                int totalMinutes = (int)(gap.end - gap.start).TotalMinutes;

                //===Cook up a fake bar and put it into the DB
                for (int i = 0; i < totalMinutes; i++){
                    double t = (double)i / totalMinutes;

                    decimal linOpen = before.Value.open + (decimal)t * (after.Value.open - before.Value.open);
                    decimal linHigh = before.Value.high + (decimal)t * (after.Value.high - before.Value.high);
                    decimal linLow  = before.Value.low  + (decimal)t * (after.Value.low  - before.Value.low);
                    decimal linClose = before.Value.close + (decimal)t * (after.Value.close - before.Value.close);
                    long volume      = 0;  // synthetic bar, no real volume
                    double avg       = (double)(linOpen + linClose) / 2;
                    int total        = 0;

                    insert.CommandText = @$"
                        INSERT OR IGNORE INTO historical_prices
                        VALUES ('{currentTime:yyyy-MM-dd HH:mm}', '{symbol}',
                                {linOpen.ToString(CultureInfo.InvariantCulture)},
                                {linHigh.ToString(CultureInfo.InvariantCulture)},
                                {linLow.ToString(CultureInfo.InvariantCulture)},
                                {linClose.ToString(CultureInfo.InvariantCulture)},
                                {volume},
                                {avg.ToString(CultureInfo.InvariantCulture)},
                                {total});
                    ";
                    insert.ExecuteNonQuery();

                    currentTime = currentTime.AddMinutes(1);
                }
            }

            //===Close connection
            transaction.Commit();
            Log.Information($"[ArtificialFill] Completed artificial fill for {symbol}");
        }



        //===Returns nearest bar BEFORE or ON the given timestamp
        public static (string timestamp, decimal open, decimal high, decimal low, decimal close, long volume, double average, int total)? GetNearestBar(string symbol, DateTime timestamp, SqliteConnection conn){

            using var cmd = conn.CreateCommand();
            cmd.CommandText = @$"
                SELECT timestamp, open, high, low, close, volume, average, total
                FROM historical_prices
                WHERE symbol = @symbol AND timestamp <= @ts
                ORDER BY timestamp DESC
                LIMIT 1;
            ";

            cmd.Parameters.AddWithValue("@symbol", symbol);
            cmd.Parameters.AddWithValue("@ts", timestamp.ToString("yyyy-MM-dd HH:mm"));

            using var reader = cmd.ExecuteReader();

            if (!reader.Read())
                return null;

            return (
                reader.GetString(0),
                reader.GetDecimal(1),
                reader.GetDecimal(2),
                reader.GetDecimal(3),
                reader.GetDecimal(4),
                reader.GetInt64(5),
                reader.GetDouble(6),
                reader.GetInt32(7)
            );
        }



        //Launches data call to APIManager
        private static async Task<List<(string timestamp, decimal open, decimal high, decimal low, decimal close, long volume, double average, int total)>> LaunchDataCall(string symbol, DateTime start, DateTime end, string interval){
            Log.Information($"Calling for {symbol} from {start.ToString()} to {end.ToString()}");
            //Convert times to UTC
            DateTime utcStart = ControlPanel.CESTtoUTC(start);
            DateTime utcEnd = ControlPanel.CESTtoUTC(end);

            //Convert to string
            //string utcStartString = utcStart.ToString("yyyy-MM-ddTHH:mm:ssZ");
            //string utcEndString = utcEnd.ToString("yyyy-MM-ddTHH:mm:ssZ");

            //===Create bar list to accomodate returned Bars (see APIManager)
            //===Ask Alpaca for the data (takes time if there's enough to overload rate limits)
            Log.Information($"Call parameters: symbol: {symbol}, utcStart: {utcStart}, utcEnd: {utcEnd}, 1Min");
            List<Bar> barsHoldingBucket = await APIManager.CallDataRange(symbol, utcStart, utcEnd, "1Min");

            //===Create tuple list to return later
            List<(string timestamp, decimal open, decimal high, decimal low, decimal close, long volume, double average, int total)> returnableData = new List<(string, decimal, decimal, decimal, decimal, long, double, int)>();

            //===Convert each bar into a tuple
            foreach(Bar bar in barsHoldingBucket){
                //===Convert time from UTC to CEST
                DateTime rawDT = DateTime.Parse(bar.t, null, DateTimeStyles.RoundtripKind);//string to DateTime
                DateTime processedDT = ControlPanel.UTCtoCEST(rawDT);//UTC to CEST
                string cestString = processedDT.ToString("yyyy-MM-dd HH:mm");//Back to string for storage

                //===Add data to tuple
                returnableData.Add((cestString, bar.o, bar.h, bar.l, bar.c, bar.v, bar.vw, bar.n));
            }

            return returnableData;
        }

        //===Finds gaps in the DB for a given symbol
        public static List<(DateTime start, DateTime end)> GapFinder(string symbol){
            Log.Information($"Finding gaps for {symbol}");
            //===Get the Ultimate Start Date from the control panel as a string and convert to DateTime
            var rawStartDate = ControlPanel.GetSetting("DataStart");
            DateTime ultimateStartDate = DateTime.ParseExact((string)rawStartDate, "yyyy-MM-dd HH:mm", null);
            
            //===Get current time
            DateTime recentTimeSnapshot = ControlPanel.GetCurrentTimeCEST();

            //===Get the timestamps for the given symbol from the DB
            //=NTS=The bracketed List etc tells the compiler that the generic object returned by ExecuteCommand is to be treated as a List etc
            var timeStamps = (List<Dictionary<string, object>>)ExecuteCommand($"SELECT timestamp FROM historical_prices WHERE symbol = '{symbol}'");
            
            //===Create Hashset for fast timestamp lookup
            HashSet<string> exisitngTimeStamps = new HashSet<string>();

            //===Prepare to find gaps, store as tuples
            List<(DateTime start, DateTime end)> gapRanges = new List<(DateTime, DateTime)>();
            DateTime? currentGapStart = null;

            //===Loop through timestamps and add to hashset
            if(timeStamps != null){
                foreach(Dictionary<string, object> timestamp in timeStamps){
                    exisitngTimeStamps.Add((string)timestamp["timestamp"]);
                }
            }else{
                //===This symbol has no data, add the whole timespan as a gap
                gapRanges.Add((ultimateStartDate, recentTimeSnapshot));
            }

            //===Loop through the hashset to find gaps
            DateTime examinedTime = ultimateStartDate;
            while(examinedTime <= recentTimeSnapshot){
                //===Convert current examined timestamp to string
                string timeStampString = examinedTime.ToString("yyyy-MM-dd HH:mm");

                //===Check if that timestamp exists in the database
                if(!exisitngTimeStamps.Contains(timeStampString)){
                    //===Close gap if open
                    if(currentGapStart == null){
                        //===Create gap tuple, and add to list, then nuke gapStart
                        //gapRanges.Add(((DateTime)currentGapStart, examinedTime));
                        currentGapStart = examinedTime;
                    }
                }else{
                    //===Open gap if none is registered
                    if(currentGapStart != null){
                        gapRanges.Add(((DateTime)currentGapStart, examinedTime));
                        currentGapStart = null;
                    }
                }

                //===Advance the loop
                examinedTime = examinedTime.AddMinutes(1);
            }

            //===Add the most recent section as a gap (it always will be because of physics)
            if(currentGapStart != null){
                gapRanges.Add(((DateTime)currentGapStart, recentTimeSnapshot));
            }

            //===Create a list of trimmed gaps that do not touch markert downtime
            List<(DateTime start, DateTime end)> trimmedGaps = new List<(DateTime, DateTime)>();

            //===Loop through the unvetted gaps to vet them
            foreach((DateTime start, DateTime end) gap in gapRanges){
                //===Loop through the tuples returned by the trimmer and shove them into the list
                foreach((DateTime, DateTime) returnedGap in TrimMarketDowntime(gap.start, gap.end)){
                    trimmedGaps.Add(returnedGap);
                }
            }

            //===Return trimmed gaps (this comment wins the Captain Obvious Award)
            return trimmedGaps;
        } 

        //===Trims a given time range to actual market hours, skipping holidays and half-days
        private static List<(DateTime start, DateTime end)> TrimMarketDowntime(DateTime gapStart, DateTime gapEnd){
            List<(DateTime start, DateTime end)> trimmedGaps = new();

            // Loop through each day in the range
            for (DateTime day = gapStart.Date; day <= gapEnd.Date; day = day.AddDays(1)){
                // Skip weekends and full holidays
                if (IsMarketHoliday(day) || IsWeekend(day))
                    continue;

                // Determine market open/close for this day
                DateTime marketOpen = new DateTime(day.Year, day.Month, day.Day, 15, 30, 0); // 15:30 CEST
                DateTime marketClose = IsHalfDay(day)
                    ? new DateTime(day.Year, day.Month, day.Day, 20, 0, 0)  // Half-day close
                    : new DateTime(day.Year, day.Month, day.Day, 22, 0, 0); // Normal close


                // Calculate the overlap with the requested gap
                DateTime start = gapStart > marketOpen ? gapStart : marketOpen;
                DateTime end = gapEnd < marketClose ? gapEnd : marketClose;

                if (start < end) // Only add valid intervals
                    trimmedGaps.Add((start, end));
            }

            return trimmedGaps;
        }

        //===Returns true if the day is a full holiday
        private static bool IsMarketHoliday(DateTime day){
            int year = day.Year;
            DateOnly date = DateOnly.FromDateTime(day);

            return IsNewYearsDay(date) || IsMLKDay(date) || IsPresidentsDay(date) ||
           IsGoodFriday(date) || IsMemorialDay(date) || IsJuneNineteenth(date) ||
           IsIndependenceDay(date) || IsLabourDay(date) || IsThanksgiving(date) ||
           IsChristmasDay(date);
        }

        //===Returns true if it’s a half day
        private static bool IsHalfDay(DateTime day){
            DateOnly date = DateOnly.FromDateTime(day);
            return IsIndependenceDayEve(date) || IsBlackFriday(date) || IsChristmasEve(date);
        }

        //===Returns true if the day is a weekend
        private static bool IsWeekend(DateTime day){
            return day.DayOfWeek == DayOfWeek.Saturday || day.DayOfWeek == DayOfWeek.Sunday;
        }


        //===These are used by MarketHoliday
        //===
        private static bool IsNewYearsDay(DateOnly day){
            return day == new DateOnly(day.Year, 1, 1);//First number is the year (day.Year), second is month, third is day
        }

        private static bool IsMLKDay(DateOnly day){//Third monday of January
            DateOnly jan1 = new DateOnly(day.Year, 1, 1);
            int daysToMonday = (7 - (int)jan1.DayOfWeek + 1) % 7;//(7 - first day number + target day number)
            DateOnly mlkDay = jan1.AddDays(daysToMonday + 14);//Add two extra weeks
            return day == mlkDay;
        }

        private static bool IsPresidentsDay(DateOnly day){//Third monday of February
            DateOnly feb1 = new DateOnly(day.Year, 2, 1);
            int daysToMonday = (7 - (int)feb1.DayOfWeek + 1) % 7;
            DateOnly presidentsDay = feb1.AddDays(daysToMonday + 14);
            return day == presidentsDay;
        }

        private static bool IsGoodFriday(DateOnly day){//Two days before Easter Sunday
            DateOnly goodFriday = GetEasterSunday(day).AddDays(-2);
            return day == goodFriday;
        }

        private static bool IsMemorialDay(DateOnly day){//Last monday in May
            DateOnly may25 = new DateOnly(day.Year, 5, 25);
            int daysToMonday = (7 - (int)may25.DayOfWeek + 1) % 7;
            DateOnly memorialDay = may25.AddDays(daysToMonday);
            return day == memorialDay;
        }

        private static bool IsJuneNineteenth(DateOnly day){//June nineteenth
            return day == new DateOnly(day.Year, 6, 19);
        }

        private static bool IsIndependenceDay(DateOnly day){//July fourth
            return day == new DateOnly(day.Year, 7, 4);
        }

        private static bool IsLabourDay(DateOnly day){//First monday of September
            DateOnly sep1 = new DateOnly(day.Year, 9, 1);
            int daysToMonday = (7 - (int)sep1.DayOfWeek + 1) % 7;
            DateOnly labourDay = sep1.AddDays(daysToMonday);
            return day == labourDay;
        }

        private static bool IsThanksgiving(DateOnly day){
            DateOnly nov1 = new DateOnly(day.Year, 11, 1);
            int daysToThursday = (7 - (int)nov1.DayOfWeek + 4) % 7;
            DateOnly thanksGiving = nov1.AddDays(daysToThursday + 21);
            return day == thanksGiving;
        }

        private static bool IsChristmasDay(DateOnly day){
            return day == new DateOnly(day.Year, 12, 25);
        }
        //===

        //===These are used by IsHalfDay
        //===
        private static bool IsIndependenceDayEve(DateOnly day){
            DateOnly independenceDayEve = new DateOnly(day.Year, 7, 3);
            return day == independenceDayEve;
        }

        private static bool IsBlackFriday(DateOnly day){//Day after Thanksgiving
            DateOnly nov1 = new DateOnly(day.Year, 11, 1);
            int daysToThursday = (7 - (int)nov1.DayOfWeek + 4) % 7;
            DateOnly blackFriday = nov1.AddDays(daysToThursday + 22);
            return day == blackFriday;
        }

        private static bool IsChristmasEve(DateOnly day){
            DateOnly christmasEve = new DateOnly(day.Year, 12, 24);
            return day == christmasEve;
        }
        //===

        //===This is for getting the date of Easter Sunday, necessary for Good Friday
        private static DateOnly GetEasterSunday(DateOnly day){
            // Anonymous Gregorian Computus algorithm
            int year = (int)day.Year;
            int a = year % 19;
            int b = year / 100;
            int c = year % 100;
            int d = b / 4;
            int e = b % 4;
            int f = (b + 8) / 25;
            int g = (b - f + 1) / 3;
            int h = (19 * a + b - d - g + 15) % 30;
            int i = c / 4;
            int k = c % 4;
            int l = (32 + 2 * e + 2 * i - h - k) % 7;
            int m = (a + 11 * h + 22 * l) / 451;
            int month = (h + l - 7 * m + 114) / 31;
            int dayOfMonth = ((h + l - 7 * m + 114) % 31) + 1;

            return new DateOnly(year, month, dayOfMonth);
        }

        

        

        /*New Year’s Day (Jan 1) --static
MLK Day (Jan 20) --dynamic
Presidents’ Day (Feb 17) --dynamic
Good Friday (Apr 18) --dynamic
Memorial Day (May 26) --dynamic
Juneteenth (Jun 19) --static
Independence Day (Jul 4) --static
Day Before Independence Day (Jul 3) --half //FIX THIS
Labor Day (Sep 1) --dynamic
Thanksgiving Day (Nov 27) --dynamic
Day Before Thanksgiving (Nov 26) --half //FIX THIS
Christmas Eve (Dec 24) --half //FIX THIS
Christmas Day (Dec 25) --static*/
    }
}