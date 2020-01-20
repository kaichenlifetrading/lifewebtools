from .packet import *
from .orderbookhistory import *
from .trade import *


class Consolidated:
    def __init__(self, exchange, trade_date, session, group_id):
        self.exchange = exchange
        self.trade_date = trade_date
        self.session = session
        self.group_id = group_id

        self.packet = Packet(exchange, trade_date, session, group_id).packet_attributes()
        self.orderbook = OrderbookHistory(exchange, trade_date, session, group_id).orderbook_attributes()
        self.trade = Trade(exchange, trade_date, session, group_id).trade_attributes()

        self.packet_cols = ['exchange_timestamp', 'net_timestamp', 'state', 'sequence_number', 'frame_number',
                            'message_type', 'order_id', 'symbol', 'action_side', 'combined_quantity',
                            'combined_price']
        self.orderbook_cols = ['exchange_time_str', 'sequence_number', 'message_types',
                               'bid_price_1', 'bid_quantity_1', 'ask_price_1', 'ask_quantity_1',
                               'mid_price', 'mid_price_delta', 'book_imbalance', 'tick_change_index', 'tick_change_1',
                               'tick_change_2', 'tick_change_3', 'tick_change_4', 'tick_change_5',
                               'tick_change_time_1', 'tick_change_time_2', 'tick_change_time_3',
                               'tick_change_time_4', 'tick_change_time_5']
        self.trade_cols = ['exchange_timestamp', 'sequence_number', 'symbol_net_delta', 'symbol_total_buy',
                           'symbol_total_sell', 'symbol_total', 'symbol_trade_imbalance', 'vwap_buy',
                           'vwap_sell', 'vwap', 'group_net_delta', 'group_total_buy', 'group_total_sell',
                           'group_total', 'group_trade_imbalance']

        self.df = self.get_consolidated()

    def reduce_columns(self):
        self.packet = self.packet[self.packet_cols]
        self.orderbook = self.orderbook[self.orderbook_cols]
        self.trade = self.trade[self.trade_cols]
        self.orderbook.rename(columns={'exchange_time_str': 'orderbook_timestamp'}, inplace=True)
        self.trade.rename(columns={'exchange_timestamp': 'trade_timestamp'}, inplace=True)
        self.orderbook_cols.remove('exchange_time_str')
        self.trade_cols.remove('exchange_timestamp')
        self.orderbook_cols.append('orderbook_timestamp')
        self.trade_cols.append('trade_timestamp')

    def get_consolidated(self):
        self.reduce_columns()
        symbol_list = self.packet.symbol.unique()
        merged = self.packet.merge(self.orderbook, how='left', left_on='sequence_number', right_on='sequence_number',
                                   suffixes=('_x', '_y'))
        merged = merged.merge(self.trade, how='left', left_on='sequence_number', right_on='sequence_number',
                              suffixes=('_x', '_y'))
        self.orderbook_cols.remove('sequence_number')
        self.trade_cols.remove('sequence_number')
        data = []
        for single_symbol in symbol_list:
            contract_df = merged.loc[merged.symbol == single_symbol]
            contract_df[self.orderbook_cols] = contract_df[self.orderbook_cols].bfill()
            contract_df[self.trade_cols] = contract_df[self.trade_cols].ffill()
            data.append(contract_df)
        df = pd.concat(data)
        df.sort_values('sequence_number', inplace=True)
        return df
