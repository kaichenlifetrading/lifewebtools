import numpy as np
from .lifeweb import *


class OrderbookHistory(Download):
    def __init__(self, exchange, trade_date, session, group_id):
        super().__init__(exchange, trade_date, session)
        self.exchange = exchange
        self.trade_date = trade_date
        self.session = session
        self.group_id = group_id
        self.lastbday = pd.datetime.today() - pd.tseries.offsets.BDay(1)

        self.lifeobj = Download(exchange, trade_date, session)
        self.last_session = self.lifeobj.find_opened_session('last', group_id)
        self.lifeobj_previous = Download(exchange, self.last_session[0], self.last_session[1])
        self.next_session = self.lifeobj.find_opened_session('next', group_id)
        self.lifeobj_next = Download(exchange, self.next_session[0], self.next_session[1])

        self.symbol_list = self.lifeobj.get_packet(self.group_id).symbol.unique()
        self.df = self.lifeobj.get_orderbook_history_group(group_id)
        self.last_df = self.lifeobj_previous.get_orderbook_history_group(group_id)
        self.next_df = self.lifeobj_next.get_orderbook_history_group(group_id)

    def session_header(self, previous_df, current_df):
        previous = previous_df.loc[previous_df.state == 'OPEN']
        previous = previous.tail(5)
        previous['state'] = 'LAST'
        df = pd.concat([previous, current_df]).sort_values(['exchange_time_str', 'sequence_number'])
        return df

    def column_attributes(self, previous_df, current_df):
        df = self.session_header(previous_df, current_df)
        data = []
        for single_symbol in self.symbol_list:
            symbol_df = df.loc[(df.symbol == single_symbol) & (df.state.isin(['LAST', 'OPEN']))]
            symbol_df['book_imbalance'] = (symbol_df.bid_quantity_1 - symbol_df.ask_quantity_1) / (
                        symbol_df.bid_quantity_1 + symbol_df.ask_quantity_1)
            symbol_df['mid_price'] = ((symbol_df.ask_price_1 * symbol_df.bid_quantity_1) / (
                        symbol_df.bid_quantity_1 + symbol_df.ask_quantity_1)) + (
                                                 (symbol_df.bid_price_1 * symbol_df.ask_quantity_1) / (
                                                     symbol_df.bid_quantity_1 + symbol_df.ask_quantity_1))
            symbol_df['mid_price_delta'] = -1 * symbol_df.mid_price.diff(-1)
            symbol_df.mid_price_delta = symbol_df.mid_price_delta.shift(1)
            symbol_df['tick_change_1'] = (symbol_df.mid_price.round(2) * -100).apply(np.floor).diff(-1)
            symbol_df.tick_change_1 = symbol_df.tick_change_1.shift(1)
            symbol_df.tick_change_1.mask(symbol_df.tick_change_1.isin([0, np.nan]), np.nan, inplace=True)
            symbol_df['tick_change_time_1'] = symbol_df.exchange_time_str
            symbol_df.tick_change_time_1.mask(symbol_df.tick_change_1.isin([0, np.nan]), np.nan, inplace=True)
            data.append(symbol_df)
        df = pd.concat(data)
        df.sort_values('sequence_number', inplace=True)
        return df

    def orderbook_attributes(self):
        df = self.column_attributes(self.last_df, self.df)
        next_df = self.column_attributes(self.df, self.next_df)

        df_tick_change = df.loc[~df.tick_change_1.isin([np.nan])][
            ['sequence_number', 'exchange_time_str', 'symbol', 'tick_change_time_1', 'tick_change_1']]
        next_df_tick_change = next_df.loc[~next_df.tick_change_1.isin([np.nan])][
            ['sequence_number', 'exchange_time_str', 'symbol', 'tick_change_time_1', 'tick_change_1']]
        tick_change = pd.concat([df_tick_change, next_df_tick_change])
        tick_change['tick_change_index'] = tick_change.exchange_time_str.dt.strftime(
            '%Y-%m-%d') + '_' + tick_change.sequence_number.astype('str')
        tick_change.set_index('sequence_number', inplace=True)
        tick_index_dict = tick_change[['tick_change_index']].to_dict()
        df['tick_change_index'] = df.sequence_number.map(tick_index_dict['tick_change_index'])

        data = []
        for single_symbol in df.symbol.unique():
            symbol_tick_change = tick_change.loc[tick_change.symbol == single_symbol]
            for x in range(2, 6):
                symbol_tick_change['tick_change_' + str(x)] = symbol_tick_change.tick_change_1.shift(-(x - 1))
                symbol_tick_change['tick_change_time_' + str(x)] = symbol_tick_change.tick_change_time_1.shift(-(x - 1))
            data.append(symbol_tick_change)
        tick_change_dict = pd.concat(data)

        for x in range(2, 6):
            df['tick_change_' + str(x)] = df.sequence_number.map(tick_change_dict['tick_change_' + str(x)])
            df['tick_change_time_' + str(x)] = df.sequence_number.map(tick_change_dict['tick_change_time_' + str(x)])

        df = df.loc[df.state != 'LAST']
        return df
