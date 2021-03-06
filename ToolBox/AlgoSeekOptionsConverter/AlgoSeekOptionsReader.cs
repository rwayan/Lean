﻿/*
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
*/

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using QuantConnect.Data.Market;
using QuantConnect.Util;
using QuantConnect.Logging;
using System.Globalization;
using QuantConnect.Data;

namespace QuantConnect.ToolBox.AlgoSeekOptionsConverter
{
    /// <summary>
    /// Enumerator for converting AlgoSeek option files into Ticks.
    /// </summary>
    public class AlgoSeekOptionsReader : IEnumerator<Tick>
    {
        private DateTime _date;
        private Stream _stream;
        private StreamReader _streamReader;
        private HashSet<string> _symbolFilter;

        private Dictionary<string, Symbol> _underlyingCache;
        /*
        private readonly int _columnTimestamp = -1;
        private readonly int _columnTicker = -1;
        private readonly int _columnType = -1;
        private readonly int _columnSide = -1;
        private readonly int _columnPutCall = -1;
        private readonly int _columnExpiration = -1;
        private readonly int _columnStrike = -1;
        private readonly int _columnQuantity = -1;
        private readonly int _columnPremium = -1;
        private readonly int _columnExchange = -1;
        */

        private readonly int _columnTime = -1;
        private readonly int _columnPrice = -1;
        private readonly int _columnVolume = -1;
        private readonly int _columnAmount = -1;
        private readonly int _columnOpenInt = -1;
        private readonly int _columnTotalVol = -1;
        private readonly int _columnTotalAmount = -1;
        private readonly int _columnLastClose = -1;
        private readonly int _columnOpen = -1;
        private readonly int _columnHigh = -1;
        private readonly int _columnLow = -1;
        private readonly int _columnSP1 = -1;
        private readonly int _columnSV1 = -1;
        private readonly int _columnBP1 = -1;
        private readonly int _columnBV1 = -1;

        private readonly int _columnsCount = -1;
        private Symbol _symbol;
        /// <summary>
        /// Enumerate through the lines of the algoseek files.
        /// </summary>
        /// <param name="file">BZ File for algoseek</param>
        /// <param name="date">Reference date of the folder</param>
        public AlgoSeekOptionsReader(string file, DateTime date, HashSet<string> symbolFilter = null)
        {
            _date = date;
            _underlyingCache = new Dictionary<string, Symbol>();
            var streamProvider = StreamProvider.ForExtension(Path.GetExtension(file));
            _stream = streamProvider.Open(file).First();
            _streamReader = new StreamReader(_stream);
            _symbolFilter = symbolFilter;

            var baseinfostreamProvider = StreamProvider.ForExtension(Path.GetExtension("E:\\data\\optiondata\\optionbaseinfo.csv"));
            var baseinfostream = baseinfostreamProvider.Open("E:\\data\\optiondata\\optionbaseinfo.csv").First();
            var basestreamReader = new StreamReader(baseinfostream);
            var baseline = basestreamReader.ReadLine();
            decimal strike = 2.2m;
            var underlying = "510050";
            var optionRight = OptionRight.Call;
            var expiry = new DateTime(2015, 3, 25);

            while ((baseline = basestreamReader.ReadLine()) != null)
            {
                baseline = baseline.Replace("\"", "");
                var csv = baseline.ToCsv();
                if (csv[1] == Path.GetFileNameWithoutExtension(file).Substring(2, 8)){
                    strike = csv[12].ToDecimal();
                    underlying = csv[8];
                    optionRight = csv[11] == "CO" ? OptionRight.Call : OptionRight.Put;
                    expiry = DateTime.ParseExact(csv[18], "yyyy-MM-dd",null);
                    break;
                }

            }

            



            

            var optionStyle = OptionStyle.European; // couldn't see this specified in the file, maybe need a reference file

            if (!_underlyingCache.ContainsKey(underlying))
            {
                _symbol = Symbol.CreateOption(underlying, Market.SSE, optionStyle, optionRight, strike, expiry, null, false);
                _underlyingCache[underlying] = _symbol.Underlying;
            }
            else
            {
                _symbol = Symbol.CreateOption(_underlyingCache[underlying], Market.SSE, optionStyle, optionRight, strike, expiry);
            }

            // detecting column order in the file
            var headerLine = _streamReader.ReadLine();
            if (!string.IsNullOrEmpty(headerLine))
            {
                var header = headerLine.ToCsv();

                _columnTime = header.FindIndex(x => x == "Time");
                _columnPrice= header.FindIndex(x => x == "Price");
                _columnVolume = header.FindIndex(x => x == "Volume");
                _columnAmount = header.FindIndex(x => x == "Amount");
                _columnOpenInt = header.FindIndex(x => x == "OpenInt");
                _columnTotalVol = header.FindIndex(x => x == "TotalVol");
                _columnTotalAmount = header.FindIndex(x => x == "TotalAmount");
                _columnLastClose = header.FindIndex(x => x == "LastClose");
                _columnOpen = header.FindIndex(x => x == "Open");
                _columnHigh = header.FindIndex(x => x == "High");
                _columnLow = header.FindIndex(x => x == "Low");
                _columnSP1 = header.FindIndex(x => x == "SP1");
                _columnSV1 = header.FindIndex(x => x == "SV1");
                _columnBP1 = header.FindIndex(x => x == "BP1");
                _columnBV1 = header.FindIndex(x => x == "BV1");

                _columnsCount = Enumerable.Max(new[] { _columnTime, _columnPrice, _columnVolume, _columnAmount,
                    _columnOpenInt, _columnTotalVol, _columnTotalAmount, _columnLastClose, _columnOpen, _columnHigh,
                    _columnLow,_columnSP1,_columnSV1,_columnBP1,_columnBV1});
            }
            //Prime the data pump, set the current.
            Current = null;
            MoveNext();
        }

        /// <summary>
        /// Parse the next line of the algoseek option file.
        /// </summary>
        /// <returns></returns>
        public bool MoveNext()
        {
            string line;
            Tick tick = null;
            while ((line = _streamReader.ReadLine()) != null && tick == null)
            {
                // If line is invalid continue looping to find next valid line.
                tick = Parse(line);
            }
            Current = tick;
            return Current != null;
        }

        /// <summary>
        /// Current top of the tick file.
        /// </summary>
        public Tick Current
        {
            get; private set; 
            
        }

        /// <summary>
        /// Gets the current element in the collection.
        /// </summary>
        /// <returns>
        /// The current element in the collection.
        /// </returns>
        object IEnumerator.Current
        {
            get { return Current; }
        }

        /// <summary>
        /// Reset the enumerator for the AlgoSeekOptionReader
        /// </summary>
        public void Reset()
        {
            throw new NotImplementedException("Reset not implemented for AlgoSeekOptionsReader.");
        }

        /// <summary>
        /// Dispose of the underlying AlgoSeekOptionsReader
        /// </summary>
        public void Dispose()
        {
            _stream.Close();
            _stream.Dispose();
            _streamReader.Close();
            _streamReader.Dispose();
        }
        
        /// <summary>
        /// Parse a string line into a option tick.
        /// </summary>
        /// <param name="line"></param>
        /// <returns></returns>
        private Tick Parse(string line)
        {
            try
            {
                // parse csv check column count
                var csv = line.ToCsv();
                if (csv.Count - 1 < _columnsCount)
                {
                    return null;
                }

                TickType tickType = TickType.OpenInterest;


                // ignoring time zones completely -- this is all in the 'data-time-zone'
                var timeString = csv[_columnTime];
                string pattern = "yyyy-MM-dd hh:mm:ss";
                DateTime parsedDate;

                if (!DateTime.TryParseExact(timeString, pattern, null,
                                              DateTimeStyles.None, out parsedDate))
                {
                    return null;
                }
                else
                {

                    var hours = parsedDate.Hour; 
                    var minutes = parsedDate.Minute;
                    var seconds = parsedDate.Second;
                    var millis = parsedDate.Millisecond;
                    var time = _date.Add(new TimeSpan(0, hours, minutes, seconds, millis));

                    var price = csv[_columnPrice].ToDecimal(); //源程序除以10000的意图不明确，暂时不除 / 10000m;
                    var quantity = csv[_columnVolume].ToInt32();
                    var OpenInt = csv[_columnOpenInt].ToInt32();

                    switch (tickType)
                    {
                        case TickType.Quote:

                            var tick = new Tick
                            {
                                Symbol = _symbol,
                                Time = time,
                                TickType = tickType,
                                Exchange = Market.SSE,
                                Value = price,
                                AskPrice = csv[_columnSP1].ToDecimal(),
                                AskSize = csv[_columnSV1].ToInt64(),
                                BidPrice = csv[_columnBP1].ToDecimal(),
                                BidSize = csv[_columnBV1].ToInt64(),
                            };

                            return tick;
                            
                        case TickType.Trade:

                            tick = new Tick
                            {
                                Symbol = _symbol,
                                Time = time,
                                TickType = tickType,
                                Exchange = Market.SSE,
                                Value = price,
                                Quantity = quantity
                            };

                            return tick;

                    case TickType.OpenInterest:

                        tick = new Tick
                        {
                            Symbol = _symbol,
                            Time = time,
                            TickType = tickType,
                            Exchange = Market.SSE,
                            Value = OpenInt
                        };

                        return tick;
                            
                    }

                    return null;
                }
            }
            catch (Exception err)
            {
                Log.Error(err);
                Log.Trace("Line: {0}", line);
                return null;
            }
        }
    }
}
