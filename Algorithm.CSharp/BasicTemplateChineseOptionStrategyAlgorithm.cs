/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

using System;
using System.Text;
using System.Linq;
using System.IO;
using System.Collections.Generic;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Orders;
using QuantConnect.Securities.Option;
using QuantConnect.Securities;
using QuantConnect.Data.UniverseSelection;

namespace QuantConnect.Algorithm.CSharp
{
    /// <summary>
    /// This example demonstrates how to add option strategies for a given underlying equity security.
    /// It also shows how you can prefilter contracts easily based on strikes and expirations.
    /// It also shows how you can inspect the option chain to pick a specific option contract to trade.
    /// </summary>
    public class BasicTemplateChineseOptionStrategyAlgorithm : QCAlgorithm
    {
        private const string UnderlyingTicker = "510050";
        public readonly Symbol Underlying = QuantConnect.Symbol.Create(UnderlyingTicker, SecurityType.Equity, Market.SSE);
        public readonly Symbol OptionSymbol = QuantConnect.Symbol.Create(UnderlyingTicker, SecurityType.Option, Market.SSE);

        public override void Initialize()
        {
            SetStartDate(2015, 12, 24);
            SetEndDate(2015, 12, 24);
            SetCash(1000000000);

            var equity = AddEquity(UnderlyingTicker,market:Market.SSE);
            var option = AddChineseOption(UnderlyingTicker,market:Market.SSE);
            //AddData<ChineseOption>(UnderlyingTicker, Resolution.Minute, true ,leverage:0);
            

            equity.SetDataNormalizationMode(DataNormalizationMode.Raw);

            // set our strike/expiry filter for this option chain
            option.SetFilter(-2, +2, TimeSpan.Zero, TimeSpan.FromDays(180));

            // use the underlying equity as the benchmark
            SetBenchmark(equity.Symbol);
        }



        public Option AddChineseOption(string underlying, Resolution resolution = Resolution.Minute, string market = null, bool fillDataForward = true, decimal leverage = 0m)
        {
            if (market == null)
            {
                if (!BrokerageModel.DefaultMarkets.TryGetValue(SecurityType.Option, out market))
                {
                    throw new Exception("No default market set for security type: " + SecurityType.Option);
                }
            }

            Symbol canonicalSymbol;
            var alias = "?" + underlying;
            if (!SymbolCache.TryGetSymbol(alias, out canonicalSymbol))
            {
                canonicalSymbol = QuantConnect.Symbol.Create(underlying, SecurityType.Option, market, alias);
            }

            var marketHoursEntry = MarketHoursDatabase.FromDataFolder().GetEntry(market, underlying, SecurityType.Option);
            var symbolProperties = SymbolPropertiesDatabase.FromDataFolder().GetSymbolProperties(market, underlying, SecurityType.Option, CashBook.AccountCurrency);
            var canonicalSecurity = (Option)SecurityManager.CreateSecurity(new List<Type>() { typeof(ChineseOption) }, Portfolio, SubscriptionManager,
                marketHoursEntry.ExchangeHours, marketHoursEntry.DataTimeZone, symbolProperties, SecurityInitializer, canonicalSymbol, resolution,
                fillDataForward, leverage, false, false, false, LiveMode, true, false);
            canonicalSecurity.IsTradable = false;

            Securities.Add(canonicalSecurity);

            // add this security to the user defined universe
            Universe universe;
            if (!UniverseManager.TryGetValue(canonicalSymbol, out universe))
            {
                var settings = new UniverseSettings(resolution, leverage, true, false, TimeSpan.Zero);
                universe = new OptionChainUniverse(canonicalSecurity, settings, SubscriptionManager, SecurityInitializer);
                UniverseManager.Add(canonicalSymbol, universe);
            }

            return canonicalSecurity;
        }

        /// <summary>
        /// Event - v3.0 DATA EVENT HANDLER: (Pattern) Basic template for user to override for receiving all subscription data in a single event
        /// </summary>
        /// <param name="slice">The current slice of data keyed by symbol string</param>
        public override void OnData(Slice slice)
        {
            if (!Portfolio.Invested)
            {
                OptionChain chain;
                if (slice.OptionChains.TryGetValue(OptionSymbol, out chain))
                {
                    var atmStraddle = chain
                        .OrderBy(x => Math.Abs(chain.Underlying.Price - x.Strike))
                        .ThenByDescending(x => x.Expiry)
                        .FirstOrDefault();

                    if (atmStraddle != null)
                    {
                        Sell(OptionStrategies.Straddle(OptionSymbol, atmStraddle.Strike, atmStraddle.Expiry), 2);
                    }
                }
            }
            else
            {
                Liquidate();
            }

            foreach(var kpv in slice.Bars)
            {
                Console.WriteLine("---> OnData: {0}, {1}, {2}", Time, kpv.Key.Value, kpv.Value.Close.ToString("0.00"));
            }
        }

        /// <summary>
        /// Order fill event handler. On an order fill update the resulting information is passed to this method.
        /// </summary>
        /// <param name="orderEvent">Order event details containing details of the evemts</param>
        /// <remarks>This method can be called asynchronously and so should only be used by seasoned C# experts. Ensure you use proper locks on thread-unsafe objects</remarks>
        public override void OnOrderEvent(OrderEvent orderEvent)
        {
            Log(orderEvent.ToString());
        }
    }






    /// <summary>
    /// Custom Data Type: ChineseOption data from Local 
    /// </summary>
    public class ChineseOption : BaseData
    {
        /// <summary>
        /// 2. RETURN THE STRING URL SOURCE LOCATION FOR YOUR DATA:
        /// This is a powerful and dynamic select source file method. If you have a large dataset, 10+mb we recommend you break it into smaller files. E.g. One zip per year.
        /// We can accept raw text or ZIP files. We read the file extension to determine if it is a zip file.
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="date">Date of this source file</param>
        /// <param name="isLiveMode">true if we're in live mode, false for backtesting mode</param>
        /// <returns>String URL of source file.</returns>
        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {

            //return "http://my-ftp-server.com/futures-data-" + date.ToString("Ymd") + ".zip";
            // OR simply return a fixed small data file. Large files will slow down your backtest
            //return new SubscriptionDataSource("http://www.quandl.com/api/v1/datasets/BCHARTS/BITSTAMPUSD.csv?sort_order=asc", SubscriptionTransportMedium.RemoteFile);

            var source = GenerateSymbolFilePath(Globals.DataFolder, config.Symbol,date, config.Resolution);
            return new SubscriptionDataSource(source, SubscriptionTransportMedium.LocalFile, FileFormat.Csv);

        }

        /// <summary>
        /// 3. READER METHOD: Read 1 line from data source and convert it into Object.
        /// Each line of the CSV File is presented in here. The backend downloads your file, loads it into memory and then line by line
        /// feeds it into your algorithm
        /// </summary>
        /// <param name="line">string line from the data source file submitted above</param>
        /// <param name="config">Subscription data, symbol name, data type</param>
        /// <param name="date">Current date we're requesting. This allows you to break up the data source into daily files.</param>
        /// <param name="isLiveMode">true if we're in live mode, false for backtesting mode</param>
        /// <returns>New Bitcoin Object which extends BaseData.</returns>
        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {

            var symbol = ReadSymbolTxt(config.Symbol, config.Resolution, line);
            return new ChineseOption { Time = date, Symbol = symbol };
        }

        public static Symbol ReadSymbolTxt(Symbol symbol, Resolution resolution, string zipEntryName)
        {
            Encoding dstcode = Encoding.UTF8;
            Encoding srcencode = Encoding.GetEncoding(936);
            var parts = zipEntryName.Replace(".csv", string.Empty).Split('_');
            var patrs = zipEntryName.Split('\t');
            string unicodestr = dstcode.GetString(Encoding.Convert(srcencode,dstcode,srcencode.GetBytes(patrs[1])));

            switch (symbol.ID.SecurityType)
            {
                case SecurityType.Option:
                    {
                        var style = (OptionStyle)Enum.Parse(typeof(OptionStyle), parts[4], true);
                        var right = (OptionRight)Enum.Parse(typeof(OptionRight), parts[5], true);
                        var strike = decimal.Parse(parts[6]) / 10000m;
                        var expiry = DateTime.ParseExact(parts[7], DateFormat.EightCharacter, null);

                        //return Symbol.CreateOption(symbol.Underlying, Market.USA, style, right, strike, expiry);
                        //changed by rwayan for suited for chinese option
                        return Symbol.CreateOption(symbol.Underlying, symbol.Underlying.ID.Market, style, right, strike, expiry);
                    }
                    break;

                case SecurityType.Future:
                    var expiryYearMonth = DateTime.ParseExact(parts[4], DateFormat.YearMonth, null);
                    expiryYearMonth = new DateTime(expiryYearMonth.Year, expiryYearMonth.Month, DateTime.DaysInMonth(expiryYearMonth.Year, expiryYearMonth.Month));
                    return Symbol.CreateFuture(parts[1], Market.USA, expiryYearMonth);

                default:
                    throw new NotImplementedException("ReadSymbolTxt is not implemented for " + symbol.ID.SecurityType + " " + resolution);
            }

        }



        /// <summary>
        /// Generates the full zip file path rooted in the <paramref name="dataDirectory"/>
        /// </summary>
        public static string GenerateSymbolFilePath(string dataDirectory, Symbol symbol, DateTime date, Resolution resolution)
        {
            var res = resolution.ToLower();
            var securityType = symbol.ID.SecurityType.ToLower();
            var market = symbol.ID.Market.ToLower();
            var directory = Path.Combine(securityType,market, res);
            var formattedDate = date.ToString(DateFormat.EightCharacter);
            var datedir = Path.Combine(directory,  symbol.Underlying.Value.ToLower(), formattedDate);
            var filename = Path.Combine(datedir, "OP_Codes.txt");
            return Path.Combine("E:\\data\\optiondata", filename);
            //return Path.Combine(dataDirectory, filename);
        }

    }

}