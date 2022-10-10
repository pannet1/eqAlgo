from omspy.brokers.master_trust import MasterTrust 
from typing import Tuple, List, Dict
import pandas as pd

exchange_list = {
        1: ['NSE'],
        2: ['NFO'],
        3: ['NSE', 'NFO']
        }

class UserMtrust(object):
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

