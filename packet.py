from .lifeweb import *


class Packet(Download):
    def __init__(self, exchange, trade_date, session, group_id):
        super().__init__(exchange, trade_date, session)
        self.exchange = exchange
        self.trade_date = trade_date
        self.session = session
        self.group_id = group_id
        self.lastbday = pd.datetime.today() - pd.tseries.offsets.BDay(1)

        self.df = self.get_packet(group_id)

    def amend_orders(self):
        amend_order_ids = self.df.order_id.loc[self.df.message_type == 'X']
        amend = self.df.loc[
            ((self.df.order_id.isin(amend_order_ids)) & (self.df.message_type.isin(['A', 'X', 'E', 'C'])))].sort_values(
            ['order_id', 'sequence_number'], ascending=True)
        self.df['amend_quantity'] = -amend.groupby('order_id')['quantity'].diff()
        self.df['amend_price'] = self.df.order_id.loc[self.df.message_type == 'X'].map(
            amend.groupby('order_id')['price'].last().to_dict())

    def delete_orders(self):
        delete_order_ids = self.df.order_id.loc[self.df.message_type == 'D']
        delete = self.df.loc[((self.df.order_id.isin(delete_order_ids)) & (
            self.df.message_type.isin(['A', 'X', 'E', 'C'])))].sort_values(['order_id', 'sequence_number'],
                                                                           ascending=True)
        self.df['delete_quantity'] = self.df.order_id.loc[self.df.message_type == 'D'].map(
            delete.groupby('order_id')['quantity'].last().to_dict())
        self.df['delete_price'] = self.df.order_id.loc[self.df.message_type == 'D'].map(
            delete.groupby('order_id')['price'].last().to_dict())

    def implied_amend_orders(self):
        implied_amend_order_ids = self.df.order_id.loc[self.df.message_type == 'l']
        implied_amend = self.df.loc[((self.df.order_id.isin(implied_amend_order_ids)) & (
            self.df.message_type.isin(['j', 'l', 'e'])))].sort_values(['order_id', 'sequence_number'], ascending=True)
        self.df['implied_amend_quantity'] = implied_amend.groupby('order_id')['quantity'].diff()
        self.df['implied_amend_price'] = self.df.order_id.loc[self.df.message_type == 'l'].map(
            implied_amend.groupby('order_id')['price'].last().to_dict())

    def implied_delete_orders(self):
        implied_delete_order_ids = self.df.order_id.loc[self.df.message_type == 'k']
        implied_delete = self.df.loc[((self.df.order_id.isin(implied_delete_order_ids)) & (
            self.df.message_type.isin(['j', 'l', 'e'])))].sort_values(['order_id', 'sequence_number'], ascending=True)
        self.df['implied_delete_quantity'] = self.df.order_id.loc[self.df.message_type == 'k'].map(
            implied_delete.groupby('order_id')['quantity'].last().to_dict())
        self.df['implied_delete_price'] = self.df.order_id.loc[self.df.message_type == 'k'].map(
            implied_delete.groupby('order_id')['price'].last().to_dict())
        self.df_dict = self.df.set_index('sequence_number').to_dict()

    def combined_price_quantity(self):
        quantity_key = {'O': 'quantity', 'B': 'executed_quantity', 'C': 'executed_quantity', 'A': 'quantity',
                        'X': 'amend_quantity', 'D': 'delete_quantity', 'E': 'executed_quantity',
                        'e': 'executed_quantity', 'P': 'executed_quantity', 'p': 'executed_quantity', 'j': 'quantity',
                        'l': 'implied_amend_quantity', 'k': 'implied_delete_quantity'}
        self.df['quantity_key'] = self.df.message_type.map(quantity_key)
        price_key = {'O': 'price', 'B': 'trade_price', 'C': 'trade_price', 'A': 'price', 'X': 'amend_price',
                     'D': 'delete_price', 'E': 'trade_price', 'e': 'trade_price', 'P': 'trade_price',
                     'p': 'trade_price', 'j': 'price', 'l': 'implied_amend_price', 'k': 'implied_delete_price'}
        self.df['price_key'] = self.df.message_type.map(price_key)

        self.df['combined_quantity'] = [self.df_dict[mtype][seq_num] for seq_num, mtype in
                                        zip(self.df.sequence_number, self.df.quantity_key)]
        self.df['combined_price'] = [self.df_dict[mtype][seq_num] for seq_num, mtype in
                                     zip(self.df.sequence_number, self.df.price_key)]

    def action_side(self):
        bs = {'buy': 1, 'sell': -1}
        inverse_bs = {v: k for k, v in bs.items()}
        inverse_side = -self.df.side.loc[self.df.message_type.isin(['E', 'e', 'C'])].map(bs)
        self.df['action_side'] = inverse_side.combine_first(self.df.side.map(bs)).map(inverse_bs)

    def fill_session_state(self):
        grouped = self.df.groupby(['symbol', 'sequence_number']).last().session_state.ffill().reset_index()[
            ['sequence_number', 'session_state']]
        grouped.set_index('sequence_number', inplace=True)
        grouped_dict = grouped.to_dict()
        self.df['state'] = self.df.sequence_number.map(grouped_dict['session_state'])
        self.df.state = self.df.state.map({'maintenance': 'MAINTENANCE', 'pre_open': 'PRE_OPEN',
                                           'opened': 'OPEN', 'closed': 'CLOSED', 'halted': 'HALTED'})

    def fix_timestamp(self):
        self.df['exchange_timestamp'] = pd.DatetimeIndex(
            self.df.exchange_timestamp.mask(self.df.exchange_timestamp_ns == 0,
                                            self.df.net_timestamp.astype('object')).astype(str))
        self.df.sort_values(['exchange_timestamp', 'sequence_number'], inplace=True)

    def packet_attributes(self):
        self.amend_orders()
        self.delete_orders()
        self.implied_amend_orders()
        self.implied_delete_orders()
        self.combined_price_quantity()
        self.action_side()
        self.fill_session_state()
        self.fix_timestamp()

        return self.df

    def reduce_packet_columns(self):
        self.packet_attributes()
        cols = ['net_timestamp', 'frame_number', 'sequence_number', 'exchange_timestamp', 'message_type', 'order_id',
                'symbol', 'combined_quantity', 'combined_price', 'action_side']
        self.df = self.df[cols]
        return self.df
