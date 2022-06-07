from fastbt.brokers.master_trust import MasterTrust 
from typing import Tuple, List, Dict
import pandas as pd

exchange_list = {
        1: ['NSE'],
        2: ['NFO'],
        3: ['NSE', 'NFO']
        }

class User(object):
    """
    A simple user class 
    """
    def __init__(self, client_id:str, password:str, pin:str, secret:str, capital:float=1.0, max_loss:float=1e10, trail_after:float=1e3, trail_percent:float=1e3, target:float=1e10, exc_code:int=1):
        """
        Initialize the user
        """
        self._capital:float = capital
        self._broker = MasterTrust(client_id=client_id, password=password, PIN=pin,
        secret=secret, token_file=f"tokens/token_{client_id}.tok")
        self._allowed_segments:List[str] = exchange_list.get(exc_code, 1)
        self._max_loss:float = abs(max_loss)
        self._target:float = abs(target)
        self._trail_after:float = trail_after
        self._trail_percent:float = trail_percent
        self._broker.authenticate()
        self._is_trailing:bool = False
        self._mtm:float= 0
        self._max_mtm:float = 0
        print(client_id)

    @property
    def capital(self)->float:
        return self._capital
    
    @property
    def broker(self)->MasterTrust:
        return self._broker

    @property
    def max_loss(self)->float:
        return self._max_loss

    @property
    def trail_after(self)->float:
        return self._trail_after

    @property
    def trail_percent(self)->float:
        return self._trail_percent

    @property
    def mtm(self)->float:
        return self._mtm

    @property
    def max_mtm(self)->float:
        return self._max_mtm

    @property
    def target(self)->float:
        return self._target

    @property
    def allowed_segments(self)->List[str]:
        return self._allowed_segments

    def quantity(self, qty:int=1)->int:
        """
        Get the quantity for the user based on capital
        """
        return int(self.capital*int(qty))

    def exit_all_bracket_orders(self):
        """
        Exit all bracket orders
        """
        broker = self.broker
        orders = broker.pending_orders()
        orders2 = broker.filter(orders, product='BO')
        if len(orders) > 0:
            orders = broker.filter(orders,product='BO', status='open')
            for order in orders:
                try:
                    oms_order_id = order['oms_order_id']
                    leg_order_indicator = order['leg_order_indicator']
                    kwargs = {
                            'oms_order_id': oms_order_id,
                            'leg_order_indicator': leg_order_indicator,
                            'status': 'open',
                            'client_id': self.broker.client_id
                            }
                    broker.exit_bracket_order(**kwargs)
                except Exception as e:
                    print(e)

    def exit_position_by_symbol(self, symbol:str, percent:float=1.0, product='MIS'):
        """
        exit positions by symbol
        symbol
            symbol to exit
        percentage
            percentage of orders to exit
        product
            MIS or NRML
        """
        positions = self.broker.positions()
        positions = self.broker.filter(positions, symbol=symbol, product=product)
        if len(positions) == 0:
            print(f"No positions for the given {symbol}")
            return
        else:
            positions = positions[0] # since there would only be one position matching this type
        quantity = positions.get('quantity', 0)
        percent = min(abs(percent),1.0)
        if quantity == 0:
            print(f"Nothing to exit for {symbol} since positions are zero")
            return
        side = 'BUY' if quantity <0 else 'SELL'
        exchange=positions.get('exchange')
        order_quantity = abs(int(quantity * percent))
        if exchange == 'NFO':
            order_quantity = int(order_quantity/50)*50
        order_args = dict(
                symbol=symbol,
                quantity=order_quantity,
                side = side,
                order_type='MARKET',
                exchange=exchange,
                product=product,
                validity='DAY'
                )
        status = self.broker.order_place(**order_args)
        return status

    
    def stop_for_position_by_symbol(self, symbol:str, triggerprice:float, percent:float=1.0, product='NRML'):
        """
        stop for positions by symbol
        symbol
            symbol to exit
        percentage
            percentage of orders to exit
        product
            MIS or NRML
        """
        positions = self.broker.positions()
        positions = self.broker.filter(positions, symbol=symbol, product=product)
        if len(positions) == 0:
            print(f"No positions for the given {symbol}")
            return
        else:
            positions = positions[0] # since there would only be one position matching this type
        quantity = positions.get('quantity', 0)
    
        percent = min(abs(percent),1.0)
        if quantity == 0:
            print(f"Nothing to exit for {symbol} since positions are zero")
            return
        side = 'BUY' if quantity <0 else 'SELL'
        delta = 1 if side=='BUY' else -1
        price = float(triggerprice) + (delta * 1/100 * float(triggerprice))        
        order_quantity = abs(int(quantity * percent))
        
        exchange=positions.get('exchange')
        if exchange == 'NFO':
            order_quantity = int(order_quantity/50)*50
        order_args = dict(
                symbol=symbol,
                quantity=order_quantity,
                price=price,
                side = side,
                trigger_price= triggerprice,
                order_type='SL',
                exchange=exchange,
                product=product,
                validity='DAY'
                )
        status = self.broker.order_place(**order_args)
        return status

    def target_for_position_by_symbol(self, symbol:str, price:float, percent:float=1.0, product='NRML'):
        """
        stop for positions by symbol
        symbol
            symbol to exit
        percentage
            percentage of orders to exit
        product
            MIS or NRML
        """
        positions = self.broker.positions()
        positions = self.broker.filter(positions, symbol=symbol, product=product)
        if len(positions) == 0:
            print(f"No positions for the given {symbol}")
            return
        else:
            positions = positions[0] # since there would only be one position matching this type
        quantity = positions.get('quantity', 0)
    
        percent = min(abs(percent),1.0)
        if quantity == 0:
            print(f"Nothing to exit for {symbol} since positions are zero")
            return
        side = 'BUY' if quantity <0 else 'SELL'
        price = float(price)
        order_quantity = abs(int(quantity * percent))
        
        exchange=positions.get('exchange')
        if exchange == 'NFO':
            order_quantity = int(order_quantity/50)*50
        order_args = dict(
                symbol=symbol,
                quantity=order_quantity,
                price=price,
                side = side,                
                order_type='LIMIT',
                exchange=exchange,
                product=product,
                validity='DAY'
                )
        status = self.broker.order_place(**order_args)
        return status

    @property
    def is_trailing(self)->bool:
        """
        Check whether trailing can be started
        """
        if self._is_trailing:
            return self._is_trailing
        else:
            trail_value = self.max_loss * self.trail_after
            if self.max_mtm > trail_value:
                self._is_trailing = True
            return self._is_trailing

    def update_mtm(self, mtm:float):
        """
        Update mtm
        """
        self._mtm = mtm
        self._max_mtm = max(self.max_mtm, mtm)

    @property
    def must_exit_all(self)->bool:
        """
        Checks whether all positions must be exited
            returns True if all positions must be exited
            else False
        """
        initial_trail = self.max_loss * (1-self.trail_percent)
        trailing_mtm = self.max_mtm * (1-self.trail_percent)
        if self.mtm < -self.max_loss:
            return True
        elif self.mtm > self.target:
            return True
        elif self.is_trailing:
            if (self.mtm < trailing_mtm):
                return True
            else:
                return False
        else:
            return False


def load_all_users(filename:str='users.xls') -> List[User]:
    """
    Load all users in the file with broker enabled
    filename
        Excel file in required xls format with one row per user
    """
    xls = pd.read_excel(filename).to_dict(orient='records')
    users = []
    for kwargs in xls:
        try:
            u = User(**kwargs)
            users.append(u)
        except Exception as e:
            print(e)
    return users

def load_shortcuts(filename:str='shortcuts.csv') -> Dict[str,str]:
    """
    Load user shortcuts
    """
    #TODO: Handle exception
    shortcuts = pd.read_csv('shortcuts.csv', index_col='shortcut')
    return shortcuts.to_dict()['full_name']
