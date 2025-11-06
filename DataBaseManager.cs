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
            //===Get the gaps for that symbol
            List<(DateTime start, DateTime end)> gapsInData = GapFinder(symbol);

            //===Create list to store retrieved data
            List<(string timestamp, decimal open, decimal high, decimal low, decimal close, long volume, double average, int total)> fullData = new List<(string, decimal open, decimal, decimal, decimal, long, double, int)>();

            //===Loop through the gaps
            foreach((DateTime start, DateTime end) gap in gapsInData){
                Log.Information($"{gap.start.ToString()} to {gap.end.ToString()}");
                //===Get data for that gap, put into main list
                foreach((string, decimal, decimal, decimal, decimal, long, double, int) dataPoint in await LaunchDataCall(symbol, gap.start, gap.end, "1Min")){
                    fullData.Add(dataPoint);
                }
            }

            //Now we have all the missing data, we can add it to the DB
            foreach((string t, decimal o, decimal h, decimal l, decimal c, long v, double vw, int n) dataPoint in fullData){
                //===Create the command
                string insertCommand = @$"
                    INSERT INTO historical_prices VALUES
                    ('{dataPoint.t}',
                     '{symbol}',
                     {dataPoint.o.ToString(CultureInfo.InvariantCulture)},
                     {dataPoint.h.ToString(CultureInfo.InvariantCulture)},
                     {dataPoint.l.ToString(CultureInfo.InvariantCulture)},
                     {dataPoint.c.ToString(CultureInfo.InvariantCulture)},
                     {dataPoint.v},
                     {dataPoint.vw.ToString(CultureInfo.InvariantCulture)},
                     {dataPoint.n})";//VOLKSWAGEN: DAS AUTO
                //Culture info etc verifies that my decimals use a full stop and not a comma
                //===Run the command
                ExecuteCommand(insertCommand);
            }
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
                if(exisitngTimeStamps.Contains(timeStampString)){
                    //===Close gap if open
                    if(currentGapStart != null){
                        //===Create gap tuple, and add to list, then nuke gapStart
                        gapRanges.Add(((DateTime)currentGapStart, examinedTime));
                        currentGapStart = null;
                    }else{
                        //===Open gap if none is registered
                        if(currentGapStart == null){
                            currentGapStart = examinedTime;
                        }
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

        //===Takes a gap as tuple and returns a list of market uptimes within
        private static List<(DateTime start, DateTime end)> TrimMarketDowntime(DateTime givenStart, DateTime givenEnd){
            //===These store the uptime periods
            List<DateTime> startTimes = new List<DateTime>();
            List<DateTime> endTimes = new List<DateTime>();

            //===Create a DateTime for market hours to compare against
            DateTime marketOpen = new DateTime(givenStart.Year, givenStart.Month, givenStart.Day, 16, 30, 0);
            DateTime marketClose = new DateTime(givenStart.Year, givenStart.Month, givenStart.Day, 23, 0, 0);

            //===Set start time
            if(givenStart <= marketOpen){//before regular hours
                startTimes.Add(marketOpen);
            }else if(givenStart > marketOpen && givenStart < marketClose){//during regular hours
                startTimes.Add(givenStart);
            }else if(givenStart >= marketClose){//After regular hours
                startTimes.Add(marketOpen.AddDays(1));
            }

            //===Is examinedTime an opening time?
            bool isOpening = false;

            //===The opening time of the same day as the start time
            DateTime jumpingOffTime = new DateTime(startTimes[0].Year, startTimes[0].Month, startTimes[0].Day, 16, 30, 0);

            //===The time we are currently examining (we start with a closing time)
            DateTime examinedTime = jumpingOffTime.AddHours(6).AddMinutes(30);

            //===Loop through the market edge hours adding them if before givenEnd
            while (examinedTime < givenEnd){
                if(isOpening){
                    startTimes.Add(examinedTime);
                    examinedTime = examinedTime.AddHours(6).AddMinutes(30);
                }else{
                    endTimes.Add(examinedTime);
                    examinedTime = examinedTime.AddHours(17).AddMinutes(30);
                }
                isOpening = !isOpening;
            }

            //===Add givenEnd if inside market edge hours
            if(!isOpening){//the examined time just after givenEnd is a closing time therefore givenEnd is between market edge hours
                endTimes.Add(givenEnd);
            }

            //===List of gaps to return
            List<(DateTime start, DateTime end)> gapsToReturn = new List<(DateTime, DateTime)>();

            //===Pack the start and end times into tuples while eliminating holidays
            for(int i = 0; i < startTimes.Count; i++){
                if(!IsMarketHoliday(startTimes[i])){//that day is not a full holiday
                    if(!IsHalfDay(startTimes[i])){//that day is not a half holiday
                        gapsToReturn.Add((startTimes[i], endTimes[i]));
                    }else{//It's a half day
                        DateTime endOfHalfDay = new DateTime(startTimes[i].Year, startTimes[i].Month, startTimes[i].Day, 20, 0, 0);
                        if(startTimes[i] < endOfHalfDay){
                            if(endTimes[i] < endOfHalfDay){
                                gapsToReturn.Add((startTimes[i], endTimes[i]));//The gap fits half day
                            }
                            gapsToReturn.Add((startTimes[i], endOfHalfDay));//The gap doesn't fit half day
                        }
                    }
                }
            }                  
            return gapsToReturn;
        }

        //===Tells you if the inputted day is a market holiday
        private static bool IsMarketHoliday(DateTime thisDay){
            //===Chop off the time bc it's irrelevant
            DateOnly date = DateOnly.FromDateTime(thisDay);

            //===Test the date through each holiday
            List<bool> holidayTest = new List<bool>();
            holidayTest.Add(IsNewYearsDay(date));
            holidayTest.Add(IsMLKDay(date));
            holidayTest.Add(IsPresidentsDay(date));
            holidayTest.Add(IsGoodFriday(date));
            holidayTest.Add(IsMemorialDay(date));
            holidayTest.Add(IsJuneNineteenth(date));
            holidayTest.Add(IsIndependenceDay(date));
            holidayTest.Add(IsLabourDay(date));
            holidayTest.Add(IsThanksgiving(date));
            holidayTest.Add(IsChristmasDay(date));
            holidayTest.Add(IsWeekend(date));

            return holidayTest.Contains(true);
        }

        //===Tells you if it's a half day (ends at 7pm CEST)
        private static bool IsHalfDay(DateTime thisDay){
            //===Chop off the time bc it's irrelevant
            DateOnly date = DateOnly.FromDateTime(thisDay);

            //===Test the date through each half day
            List<bool> halfDayTest = new List<bool>();
            halfDayTest.Add(IsIndependenceDayEve(date));
            halfDayTest.Add(IsBlackFriday(date));
            halfDayTest.Add(IsChristmasEve(date));

            return halfDayTest.Contains(true);
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
            //CHRIST IS KING + + + + + + +
            return day == new DateOnly(day.Year, 12, 25);
        }

        private static bool IsWeekend(DateOnly day){
            return day.DayOfWeek == DayOfWeek.Saturday || day.DayOfWeek == DayOfWeek.Sunday;
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
/*/QUARANTINE LINE: VIBE CODED SLOP BEYOND THIS POINT

namespace DatabaseManagerSpace
{
    public static class DatabaseManager
    {
        private const string DatabaseFile = "../Data/historical_data.db";

        static DatabaseManager()
        {
            if (!File.Exists(DatabaseFile))
            {
                using (FileStream fs = File.Create(DatabaseFile)) { }
            }

            using var connection = new SqliteConnection($"Data Source={DatabaseFile};");
            connection.Open();

            string createTableQuery = @"
                CREATE TABLE IF NOT EXISTS historical_prices (
                    timestamp TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    close REAL NOT NULL,
                    PRIMARY KEY (timestamp, symbol)
                );";

            using var command = new SqliteCommand(createTableQuery, connection);
            command.ExecuteNonQuery();
        }

        public static void InsertBars(List<(string Time, string Symbol, decimal Close)> bars)
        {
            try
            {
                using var connection = new SqliteConnection($"Data Source={DatabaseFile};");
                connection.Open();

                using var transaction = connection.BeginTransaction();
                var command = connection.CreateCommand();

                command.CommandText = @"
                    INSERT INTO historical_prices (timestamp, symbol, close)
                    VALUES (@timestamp, @symbol, @close);";

                var timeParam = command.CreateParameter();
                timeParam.ParameterName = "@timestamp";
                command.Parameters.Add(timeParam);

                var symbolParam = command.CreateParameter();
                symbolParam.ParameterName = "@symbol";
                command.Parameters.Add(symbolParam);

                var closeParam = command.CreateParameter();
                closeParam.ParameterName = "@close";
                command.Parameters.Add(closeParam);

                foreach (var bar in bars)
                {
                    timeParam.Value = bar.Time;
                    symbolParam.Value = bar.Symbol;
                    closeParam.Value = bar.Close;
                    command.ExecuteNonQuery();
                }

                transaction.Commit();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ SQLite InsertBars error: {ex.Message}");
            }
        }

        public static List<string> QueryData(string symbol)
        {
            var result = new List<string>();

            using var connection = new SqliteConnection($"Data Source={DatabaseFile};");
            connection.Open();

            string selectQuery = @"
                SELECT timestamp, close FROM historical_prices
                WHERE symbol = @symbol
                ORDER BY timestamp;";

            using var command = new SqliteCommand(selectQuery, connection);
            command.Parameters.AddWithValue("@symbol", symbol);

            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                string timestamp = reader.GetString(0);
                decimal close = reader.GetDecimal(1);
                result.Add($"{timestamp}: Close={close:C}");
            }

            return result;
        }

        public static void ClearDatabase()
        {
            try
            {
                using var connection = new SqliteConnection($"Data Source={DatabaseFile};");
                connection.Open();

                string deleteQuery = "DELETE FROM historical_prices;";
                using var command = new SqliteCommand(deleteQuery, connection);
                command.ExecuteNonQuery();

                Console.WriteLine("✅ Database has been cleared.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Error clearing database: {ex.Message}");
            }
        }

        public static void AnalyzeDataGaps(string symbol)
        {
            Console.WriteLine("Analyzing...");
            try
            {
                DateTime currentCest = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("Central European Standard Time"));
                DateTime roundedCurrent = RoundToNearestFiveMinute(currentCest);
                DateTime start = new DateTime(2019, 1, 1, 15, 30, 0);

                HashSet<string> existingTimestamps = new HashSet<string>();

                using (var connection = new SqliteConnection($"Data Source={DatabaseFile};"))
                {
                    connection.Open();
                    var command = connection.CreateCommand();
                    command.CommandText = @"SELECT timestamp FROM historical_prices WHERE symbol = @symbol;";
                    command.Parameters.AddWithValue("@symbol", symbol);

                    using var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        existingTimestamps.Add(reader.GetString(0));
                    }
                }

                StringBuilder gapString = new StringBuilder();
                List<string> gapRanges = new List<string>();

                DateTime? gapStart = null;
                DateTime currentTime = start;

                while (currentTime <= roundedCurrent)
                {
                    if (!IsMarketTime(currentTime))
                    {
                        currentTime = currentTime.AddMinutes(5);
                        continue;
                    }

                    string timestampStr = currentTime.ToString("yyyy-MM-dd HH:mm");

                    if (existingTimestamps.Contains(timestampStr))
                    {
                        gapString.Append("|");

                        if (gapStart != null)
                        {
                            string gapRange = $"[{gapStart.Value:yyyy-MM-dd HH:mm} - {currentTime.AddMinutes(-5):yyyy-MM-dd HH:mm}]";
                            gapRanges.Add(gapRange);
                            gapStart = null;
                        }
                    }
                    else
                    {
                        gapString.Append(".");
                        if (gapStart == null)
                            gapStart = currentTime;
                    }

                    currentTime = currentTime.AddMinutes(5);
                }

                if (gapStart != null)
                {
                    string gapRange = $"[{gapStart.Value:yyyy-MM-dd HH:mm} - {roundedCurrent:yyyy-MM-dd HH:mm}]";
                    gapRanges.Add(gapRange);
                }

                Console.WriteLine("Gap Analysis Visual:");
                Console.WriteLine(gapString.ToString());

                Console.WriteLine("\nGap Ranges:");
                foreach (var range in gapRanges)
                {
                    Console.WriteLine(range);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Error in gap analysis: {ex.Message}");
            }
        }

        private static DateTime RoundToNearestFiveMinute(DateTime time)
        {
            int minutes = (time.Minute / 5) * 5;
            return new DateTime(time.Year, time.Month, time.Day, time.Hour, minutes, 0);
        }

        private static bool IsMarketTime(DateTime time)
        {
            // Market hours: Monday–Friday, 9:30–17:30 CEST
            if (time.DayOfWeek == DayOfWeek.Saturday || time.DayOfWeek == DayOfWeek.Sunday)
                return false;

            if (IsHoliday(time.Date))
                return false;

            TimeSpan start = new TimeSpan(15, 30, 0);
            TimeSpan end = new TimeSpan(22, 30, 0);
            TimeSpan current = time.TimeOfDay;

            return current >= start && current <= end;
        }

        private static bool IsHoliday(DateTime date)
        {
            int year = date.Year;

            DateTime goodFriday = GetEasterSunday(year).AddDays(-2);
            DateTime easterMonday = GetEasterSunday(year).AddDays(1);

            HashSet<DateTime> holidays = new HashSet<DateTime>
            {
                new DateTime(year, 1, 1),    // New Year's Day
                new DateTime(year, 5, 1),    // Labour Day
                goodFriday,
                easterMonday,
                new DateTime(year, 12, 25),  // Christmas Day
                new DateTime(year, 12, 26),  // Boxing Day
            };

            return holidays.Contains(date.Date);
        }

        private static DateTime GetEasterSunday(int year)
        {
            // Anonymous Gregorian Computus algorithm
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
            int day = ((h + l - 7 * m + 114) % 31) + 1;

            return new DateTime(year, month, day);
        }
    }
}
*/