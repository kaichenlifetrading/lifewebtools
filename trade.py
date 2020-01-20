from .lifeweb import *


class Trade(Download):
    def __init__(self, exchange, trade_date, session, group_id):
        super().__init__(exchange, trade_date, session)
        self.exchange = exchange
        self.trade_date = trade_date
        self.session = session
        self.group_id = group_id
        self.lastbday = pd.datetime.today() - pd.tseries.offsets.BDay(1)

        self.lifeobj = Download(exchange, trade_date, session)
        self.symbol_list = self.lifeobj.get_packet(self.group_id).symbol.unique()
        self.df = self.get_group_trade()

    def get_group_trade(self):
        trade = self.lifeobj.get_packet(self.group_id)
        trade = trade.loc[trade.message_type.isin(['C', 'E', 'e'])]

        bs = {'buy': 1, 'sell': -1}
        inverse_bs = {v: k for k, v in bs.items()}
        inverse_side = -trade.side.loc[trade.message_type.isin(['E', 'e', 'C'])].map(bs)
        trade['side'] = inverse_side.combine_first(trade.side.map(bs)).map(inverse_bs)

        cols = ['sequence_number', 'frame_number', 'exchange_timestamp', 'message_type',
                'symbol', 'side', 'executed_quantity', 'trade_price']
        trade = trade[cols]
        trade.rename(columns={'executed_quantity': 'quantity', 'trade_price': 'price'}, inplace=True)
        return trade

    def trade_attributes(self):
        data = []
        for single_symbol in self.symbol_list:
            contract_df = self.df.loc[self.df.symbol == single_symbol]
            contract_df['symbol_net_delta'] = (
                        contract_df.side.map({'buy': 1, 'sell': -1}) * contract_df.quantity).cumsum()
            contract_df['symbol_total_buy'] = (
                        contract_df.side.map({'buy': 1, 'sell': 0}) * contract_df.quantity).cumsum()
            contract_df['symbol_total_sell'] = (
                        contract_df.side.map({'buy': 0, 'sell': 1}) * contract_df.quantity).cumsum()
            contract_df['symbol_total'] = contract_df.quantity.cumsum()
            contract_df['symbol_trade_imbalance'] = (contract_df.symbol_total_buy - contract_df.symbol_total_sell) / (
                        contract_df.symbol_total_buy + contract_df.symbol_total_sell)
            contract_df['vwap_buy'] = (contract_df.side.map({'buy': 1,
                                                             'sell': 0}) * contract_df.quantity * contract_df.price).cumsum() / contract_df.symbol_total_buy
            contract_df['vwap_sell'] = (contract_df.side.map({'buy': 0,
                                                              'sell': 1}) * contract_df.quantity * contract_df.price).cumsum() / contract_df.symbol_total_sell
            contract_df['vwap'] = (contract_df.quantity * contract_df.price).cumsum() / contract_df.symbol_total
            data.append(contract_df)
        self.df = pd.concat(data)
        self.df.sort_values('sequence_number', inplace=True)
        self.df['group_net_delta'] = (self.df.side.map({'buy': 1, 'sell': -1}) * self.df.quantity).cumsum()
        self.df['group_total_buy'] = (self.df.side.map({'buy': 1, 'sell': 0}) * self.df.quantity).cumsum()
        self.df['group_total_sell'] = (self.df.side.map({'buy': 0, 'sell': 1}) * self.df.quantity).cumsum()
        self.df['group_total'] = self.df.quantity.cumsum()
        self.df['group_trade_imbalance'] = (self.df.group_total_buy - self.df.group_total_sell) / (
                    self.df.group_total_buy + self.df.group_total_sell)
        return self.df
