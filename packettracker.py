from .packet import *


class PacketTracker():
    def __init__(self, exchange, trade_date, session, group_id, filter_list, order_list):
        self.exchange = exchange
        self.trade_date = trade_date
        self.session = session
        self.group_id = group_id
        self.filter_list = filter_list
        self.order_list = order_list
        self.lastbday = pd.datetime.today() - pd.tseries.offsets.BDay(1)

        self.lifeobj = Packet(exchange, trade_date, session, group_id)
        self.df = self.lifeobj.packet_attributes()

    def packet_preopen_order_id(self):
        mask = (self.df.state != 'OPEN') & (self.df.message_type.isin(['A', 'X', 'D', 'E', 'e'])) & (
            self.df.combined_quantity.isin(self.filter_list))
        flipper_order_id = self.df.loc[mask].reset_index()[['order_id', 'combined_quantity']].set_index('order_id')
        return flipper_order_id

    def packet_opened_aggregate_timestamp(self):
        mask = (self.df.state == 'OPEN') & (self.df.message_type.isin(['A', 'X', 'D', 'E', 'e']))
        timestamp_agg = pd.DataFrame(self.df.loc[mask].groupby('exchange_timestamp')['combined_quantity'].sum(),
                                     columns=['combined_quantity'])
        flipper_timestamp_agg = timestamp_agg.loc[timestamp_agg.combined_quantity.isin(self.filter_list)]
        return flipper_timestamp_agg

    def flipper_packet_attributes(self):
        if self.order_list is None:
            order_list = []
        else:
            order_list = self.order_list

        combined_order_list = list(self.packet_preopen_order_id().index) + order_list
        flipper_aggressive_packet = self.df.loc[
            (self.df.exchange_timestamp.isin(self.packet_opened_aggregate_timestamp().index)) & (
                self.df.message_type.isin(['A', 'X', 'D', 'E', 'e']))]
        flipper_passive_packet = pd.concat(
            [self.df.loc[(self.df.order_id.isin(combined_order_list)) & (self.df.message_type == 'A')],
             flipper_aggressive_packet.loc[flipper_aggressive_packet.message_type == 'A']])
        flipper_packet = pd.concat([flipper_aggressive_packet, flipper_passive_packet], sort=True)
        flipper_packet = flipper_packet.reset_index().groupby('sequence_number').last().reset_index().set_index(
            'exchange_timestamp')
        return flipper_aggressive_packet, flipper_passive_packet, flipper_packet

    def flipper_attributes(self):
        flipper_aggressive_packet, flipper_passive_packet, flipper_packet = self.flipper_packet_attributes()
        flipper_add = flipper_packet.loc[flipper_packet.message_type == 'A']

        flipper_aggressive = flipper_aggressive_packet.loc[flipper_aggressive_packet.message_type.isin(['E', 'e'])]
        flipper_passive = self.df.loc[
            (self.df.order_id.isin(flipper_passive_packet.order_id)) & (self.df.message_type.isin(['E', 'e']))]

        flipper_wash = flipper_aggressive.loc[flipper_aggressive.order_id.isin(flipper_passive_packet.order_id)]
        flipper_wash['flipper_action'] = 'W'
        flipper_aggressive_ex_wash = flipper_aggressive.loc[
            ~flipper_aggressive.sequence_number.isin(flipper_wash.sequence_number)]
        flipper_aggressive_ex_wash['flipper_action'] = 'A'
        flipper_passive_ex_wash = flipper_passive.loc[
            ~flipper_passive.sequence_number.isin(flipper_wash.sequence_number)]
        flipper_passive_ex_wash['flipper_action'] = 'P'

        flipper_execute = pd.concat([flipper_aggressive_ex_wash, flipper_wash, flipper_passive_ex_wash], sort=True)
        flipper_execute = flipper_execute.reset_index().groupby('sequence_number').last().reset_index().sort_values(
            'sequence_number')

        flipper_execute['adj_combined_quantity'] = flipper_execute.action_side.map(
            {'buy': 1, 'sell': -1}) * flipper_execute.flipper_action.map(
            {'A': 1, 'P': -1, 'W': 0}) * flipper_execute.combined_quantity
        flipper_execute[
            'adj_combined_price_quantity'] = flipper_execute.adj_combined_quantity * flipper_execute.combined_price

        return flipper_packet, flipper_add, flipper_execute


def flipper_filter():
    increment = list(range(0, 2, 1))
    main_size = list(range(500, 9999, 250))
    return [x + y for x in main_size for y in increment]
