from .database import *


class Download(Database):
    def __init__(self, exchange, trade_date, session, **kwargs):
        super().__init__()
        self.exchange = exchange
        self.trade_date = trade_date
        self.session = session
        self.lastbday = pd.datetime.today() - pd.tseries.offsets.BDay(1)

        for key, value in kwargs.items():
            setattr(self, key, value)

    @staticmethod
    def time_between(df, index, sess1time, sess2time):
        df = df.set_index(index)
        df.index = pd.DatetimeIndex(df.index)
        df = df.between_time(sess1time, sess2time, include_start=False, include_end=True)

        df.reset_index(inplace=True)
        return df

    def check_open(self, group_id):
        states = self.get_packet(group_id).session_state.unique()
        return 'opened' in states

    def find_opened_session(self, type_setting, group_id):
        count = 4
        iter_list = sorted(list(range(0, count)) * 2)
        date = self.trade_date
        session = self.session
        date_list = []
        session_list = []

        setting_dict = {'last': {'multiple': 1, 'session': '2', 'slice1': {'start': 0, 'end': -2},
                                 'slice2': {'start': 2, 'end': len(iter_list)}},
                        'next': {'multiple': -1, 'session': '1', 'slice1': {'start': 3, 'end': len(iter_list)},
                                 'slice2': {'start': 0, 'end': -2}}}

        type_dict = setting_dict[type_setting]

        if date.weekday() in [5, 6]:
            date = (date + pd.tseries.offsets.BDay(1) - pd.tseries.offsets.Day(0)).date()
            session = type_dict['session']

        if int(session) == 1:
            date_list = iter_list[type_dict['slice1']['start']:type_dict['slice1']['end']]
            session_list = [1, 0] * count
        elif int(session) == 2:
            date_list = iter_list[type_dict['slice2']['start']:type_dict['slice2']['end']]
            session_list = [-1, 0] * count

        date_session_tuple = [((date - type_dict['multiple'] * pd.tseries.offsets.BDay(x)).date()
                               , str(int(session) + y)) for x, y in zip(date_list, session_list)]
        i = 0
        check = False
        while (i < len(date_list)) & (check is False):
            lifeobj = Download(self.exchange, date_session_tuple[i][0], date_session_tuple[i][1])
            check = lifeobj.check_open(group_id)
            i += 1
        return date_session_tuple[i - 1]

    def get_packet(self, group_id):
        pd.options.mode.chained_assignment = None
        filename = self.generate_filename('packet', self.exchange, self.trade_date.strftime("%Y-%m-%d"),
                                          self.session, group_id=group_id)
        if (self.check_file(self.address['packet_address'], filename)) & (self.trade_date <= self.lastbday):
            df = self.load_pickle(self.address['packet_address'], filename)
        else:
            message_types_str = ''

            for message_type in ['O', 'C', 'A', 'X', 'D', 'E', 'e', 'P', 'p', 'j', 'k', 'l', 'B']:
                message_types_str += '&message_types=MOLD_' + message_type

            server_path = "http://lifeweb.lifetrading.com.au/marketdata/ASX/messages_csv?environment={}&inlineRadioOptions=" \
                          "trade_date&trade_date={}&Session={}{}&group_id={}&timezone=Australia%2FSydney" \
                .format(self.exchange,
                        self.trade_date.strftime("%Y-%m-%d"),
                        self.session,
                        message_types_str,
                        group_id)

            df = pd.read_csv(server_path)
            df = self.time_between(df, 'net_timestamp', self.session_time[self.session]['start_time'],
                                   self.session_time[self.session]['end_time'])
            self.store_pickle(self.address['packet_address'], filename, df)

        print("Successful pulling data for:{} on trade date:{} - session: {}".format(
            group_id, self.trade_date, self.session))
        return df

    #
    def get_orderbook_history(self, symbol):
        pd.options.mode.chained_assignment = None
        filename = self.generate_filename('price', self.exchange, self.trade_date.strftime("%Y-%m-%d"),
                                          self.session, symbol=symbol)
        if self.check_file(self.address['price_address'], filename) & (self.trade_date <= self.lastbday):
            df = self.load_pickle(self.address['price_address'], filename)
        else:
            server_path = "http://lifeweb.lifetrading.com.au/marketdata/ASX/orderbook_history?environment={}&Session={}" \
                          "&Symbol={}&TradeDate={}&Levels={}&timezone=Australia%2FSydney&submit=Submit" \
                .format(self.exchange,
                        self.session,
                        symbol,
                        self.trade_date.strftime("%Y-%m-%d"),
                        '10')
            df = pd.read_csv(server_path)
            df = self.time_between(df, 'exchange_time_str', self.session_time[self.session]['start_time'],
                                   self.session_time[self.session]['end_time'])
            self.store_pickle(self.address['price_address'], filename, df)

        print("Successful pulling data for:{} on trade date:{} - session: {}".format(symbol, self.trade_date,
                                                                                     self.session))
        return df

    def get_orderbook_history_group(self, group_id):
        pd.options.mode.chained_assignment = None
        filename = self.generate_filename('group_price', self.exchange,
                                          self.trade_date.strftime("%Y-%m-%d"),
                                          self.session, group_id=group_id)
        if self.check_file(self.address['group_price_address'], filename) & (
                self.trade_date <= self.lastbday):
            df = self.load_pickle(self.address['group_price_address'], filename)
        else:
            symbol_list = self.get_packet(group_id).symbol.unique()
            data = []
            for single_symbol in symbol_list:
                symbol = self.get_orderbook_history(single_symbol)
                data.append(symbol)
            df = pd.concat(data)
            df.sort_values('sequence_number', inplace=True)
            self.store_pickle(self.address['group_price_address'], filename, df)

        print("Successful pulling data for:{} on trade date:{} - session: {}".format(group_id, self.trade_date,
                                                                                     self.session))
        return df

    def get_fill(self, book):
        pd.options.mode.chained_assignment = None
        filename = self.generate_filename('fill', self.exchange, self.trade_date.strftime("%Y-%m-%d"),
                                          self.session, book=book)
        if self.check_file(self.address['fill_address'], filename) & (self.trade_date <= self.lastbday):
            df = self.load_pickle(self.address['fill_address'], filename)
        else:
            server_path = "http://lifeweb.lifetrading.com.au/trades/{}.csv?exchange={}&utc_fill_time_after={}" \
                          "&utc_fill_time_before={}&book_id={}&is_manual={}" \
                .format(self.trade_date.strftime("%Y-%m-%d"),
                        self.exchange,
                        (self.trade_date + datetime.timedelta(days=-1)).strftime("%Y-%m-%d"),
                        (self.trade_date + datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
                        book,
                        '1')

            df = pd.read_csv(server_path)
            df = df.loc[pd.to_datetime(df.trade_date) == self.trade_date]
            df['net_timestamp'] = pd.to_datetime(df.utc_fill_time).dt.tz_convert('Australia/Sydney')
            df = self.time_between(df, 'net_timestamp', self.session_time[self.session]['start_time'],
                                   self.session_time[self.session]['end_time'])
            self.store_pickle(self.address['fill_address'], filename, df)
        print("Successful pulling data for: {}".format(book))
        return df

    def get_orderbook_at(self, trade_time, symbol, detail, sequence_number):
        pd.options.mode.chained_assignment = None
        if sequence_number == None or sequence_number == "":
            server_path = "http://lifeweb.lifetrading.com.au/marketdata/ASX/orderbook_at.csv?environment={}&Symbol=" \
                          "{}&Detail={}&DateTime_At={}T{}%3A{}%3A{}&search=Date+Time&Session={}&Sequence_Number=" \
                .format(self.exchange,
                        symbol,
                        detail,
                        self.trade_date.strftime("%Y-%m-%d"),
                        trade_time.hour,
                        trade_time.minute,
                        trade_time.second,
                        self.session)
        else:
            server_path = "http://lifeweb.lifetrading.com.au/marketdata/ASX/orderbook_at.csv?environment={}&Symbol={}" \
                          "&Detail={}&DateTime_At=2019-06-20T00%3A00%3A00&TradeDate={}&Session={}&Sequence_Number={}" \
                          "&search=Sequence+Number" \
                .format(self.exchange,
                        symbol,
                        detail,
                        self.trade_date.strftime("%Y-%m-%d"),
                        self.session,
                        sequence_number)

        df = pd.read_csv(server_path)
        print("Successful pulling data for:{} on trade date:{} - sequence_number: {}".format(symbol, self.trade_date,
                                                                                             sequence_number))
        return df


class Update(Download):
    def __init__(self):
        super().__init__()

    @staticmethod
    def update_database(start_date, end_date, exchange_list, session_list, group_list, book_list):
        dates = pd.to_datetime(sorted(pd.bdate_range(start_date, end_date)))
        for exchange in exchange_list:
            for single_date in dates:
                for single_session in session_list:
                    life_obj = Download(exchange, single_date, single_session)
                    for book in book_list:
                        try:
                            life_obj.get_fill(book)
                        except Exception as e:
                            print(e)
                            continue
                    for single_group_id in group_list:
                        try:
                            packet = life_obj.get_packet(single_group_id)
                            symbol_list = packet.symbol.unique()
                            for single_symbol in symbol_list:
                                life_obj.get_orderbook_history(single_symbol)
                            life_obj.get_orderbook_history_group(single_group_id)

                        except Exception as e:
                            print(e)
                            continue
