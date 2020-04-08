import datetime


class Mapping:
    def __init__(self):
        self.compression_type = 'zip'
        self.address = {'main_address': '/home/trader/notes/Kai/LifewebTools/database/',
                        'packet_address': '/home/trader/notes/Kai/LifewebTools/database/packet/',
                        'fill_address': '/home/trader/notes/Kai/LifewebTools/database/fill/',
                        'price_address': '/home/trader/notes/Kai/LifewebTools/database/price/',
                        'group_price_address': '/home/trader/notes/Kai/LifewebTools/database/group_price/'}
        self.fill_functions = {'position': '_position',
                               'raw_quantity': '_raw_quantity',
                               'adj_quantity': '_adj_quantity',
                               'price_quantity': '_adj_price_quantity',
                               'exchange_fills': '_exchange_fills'}
        self.price_functions = {'ask_price': '_ask_price_1',
                                'ask_quantity': '_ask_quantity_1',
                                'bid_price': '_bid_price_1',
                                'bid_quantity': '_bid_quantity_1',
                                'mid_price': '_mid_price_1',
                                'state': '_state'}
        self.expiry_letters = {'Jan': 'F', 'Feb': 'G', 'Mar': 'H', 'Apr': 'J', 'May': 'K', 'Jun': 'M',
                               'Jul': 'N', 'Aug': 'Q', 'Sep': 'U', 'Oct': 'V', 'Nov': 'X', 'Dec': 'Z'}
        self.session_time = {'1': {'start_time': datetime.time(7, 30, 0), 'end_time': datetime.time(16, 30, 0)},
                             '2': {'start_time': datetime.time(16, 30, 0), 'end_time': datetime.time(7, 30, 0)}}
        self.group_values = {'exchange': {'IR': 'ASX_PROD',
                                          'YT': 'ASX_PROD',
                                          'XT': 'ASX_PROD',
                                          'IRIR': 'ASX_PROD',
                                          'IRYT': 'ASX_PROD',
                                          'YTXT': 'ASX_PROD',
                                          'YTYT': 'ASX_PROD',
                                          'XTXT': 'ASX_PROD'},
                             'grouptype': {'IR': 'outright',
                                           'YT': 'outright',
                                           'XT': 'outright',
                                           'IRIR': 'spread',
                                           'IRYT': 'spread',
                                           'YTXT': 'spread',
                                           'YTYT': 'spread',
                                           'XTXT': 'spread'},
                             'mintick': {'IR': 0.01,
                                         'YT': 0.005,
                                         'XT': 0.005,
                                         'IRIR': 0.01,
                                         'IRYT': 0.005,
                                         'YTXT': 0.005,
                                         'YTYT': 0.005,
                                         'XTXT': 0.0025},
                             'dollarvalue': {'IR': 2440,
                                             'YT': 3060,
                                             'XT': 10150,
                                             'IRIR': 2440,
                                             'IRYT': 2440 * 20,
                                             'YTXT': 10150 * 10,
                                             'YTYT': 3060,
                                             'XTXT': 10150},
                             'session_time': {
                                 'IR': {'open': {'day': datetime.time(8, 18, 0), 'night': datetime.time(16, 58, 0)},
                                        'close': {'day': datetime.time(16, 30, 0), 'night': datetime.time(7, 0, 0)}},
                                 'YT': {'open': {'day': datetime.time(8, 20, 0), 'night': datetime.time(17, 0, 0)},
                                        'close': {'day': datetime.time(16, 30, 0), 'night': datetime.time(7, 0, 0)}},
                                 'XT': {'open': {'day': datetime.time(8, 22, 0), 'night': datetime.time(17, 2, 0)},
                                        'close': {'day': datetime.time(16, 30, 0), 'night': datetime.time(7, 0, 0)}},
                                 'IRIR': {'open': {'day': datetime.time(8, 28, 0), 'night': datetime.time(17, 8, 0)},
                                          'close': {'day': datetime.time(16, 30, 0), 'night': datetime.time(7, 0, 0)}},
                                 'IRYT': {'open': {'day': datetime.time(8, 30, 0), 'night': datetime.time(17, 10, 0)},
                                          'close': {'day': datetime.time(16, 30, 0), 'night': datetime.time(7, 0, 0)}},
                                 'YTXT': {'open': {'day': datetime.time(8, 32, 0), 'night': datetime.time(17, 12, 0)},
                                          'close': {'day': datetime.time(16, 30, 0), 'night': datetime.time(7, 0, 0)}}}
                             }

        self.factor_groupvalue = {'f1': 2440, 'f2': 2440, 'f3': 2440, 'f4': 2440, 'f5': 2440,
                                  'w1': 2440, 'w2': 2440,
                                  'f3y': 1, 'f10y': 1}

    @staticmethod
    def find_group_id(symbol):
        if len(symbol) == 4:
            group_id = symbol[:2]
        elif symbol[:2] == 'IR' and len(symbol) == 6:
            group_id = 'IRIR'
        elif symbol[:2] == 'IR' and len(symbol) == 12:
            group_id = 'IRYT'
        elif symbol[:2] == 'YT' and len(symbol) == 12:
            group_id = 'YTXT'
        elif symbol[:2] == 'YT' and len(symbol) == 6:
            group_id = 'YTYT'
        elif symbol[:2] == 'XT' and len(symbol) == 6:
            group_id = 'XTXT'
        else:
            group_id = 'Nan'

        return group_id

    def find_exchange(self, symbol):
        group_id = self.find_group_id(symbol)
        return self.group_values['exchange'][group_id]
