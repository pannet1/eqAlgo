from fastbt.brokers.master_trust import MasterTrust, fetch_all_contracts
from user import load_all_users, load_shortcuts
from copy import deepcopy
import pandas as pd
import json
import concurrent.futures
import time
import requests

from flask import Flask, request, jsonify
app = Flask(__name__)

"""
Routes
------
1. /order/args - place a MIS order
2. /bracket/args - place a bracket order
3. /bs/symbol/stop - adjust stop loss for bracket order
4. /bt/symbol/target - adjust target for bracket order
5. /be/symbol - exit bracket order
6. /me/symbol - exit MIS order by symbol
7. /ne/symbol - exit NRML order by symbol
To exit the first order only, add ?mode=first like
/be/symbol/stop?first=True
"""


contracts = fetch_all_contracts(exchanges=['NSE', 'NFO'])
USERS = load_all_users()
for user in USERS:
    user.broker.contracts = contracts
    user.broker.exchange = 'NFO'

shortcuts = load_shortcuts()
print(USERS)
DISABLED_USERS = set()

def transform(varargs):
    """
    Transform the given arguments into a set of
    dictionary arguments for placing an order
    varargs
        varargs as a string
    """
    args = varargs.split('/')
    text = [txt.split('=') for txt in args]
    dct = {x:y for x,y in text}
    dct2 = {}
    for k,v in dct.items():
        sc = shortcuts.get(k)
        if sc:
            dct2[sc] = v
        else:
            dct2[k] = v
    return dct2

@app.route('/')
def hello_world():
    print(request.args)
    return "Go to http://127.0.0.1:8181/order to place your order"

@app.route('/order/<path:varargs>', methods=['GET'])
def order(varargs):
    """
    Place a MIS order
    """
    responses = []
    order_args = transform(varargs)
    exchange = order_args.get('exchange', 'NSE')
    try:
        lot_size = int(order_args.get('l', 50))
        num = int(order_args.get('n', 0))
    except Exception as e:
        lot_size = 50
        num = 0
    def _place_order(user, args):
        if user not in DISABLED_USERS:
            if exchange in user.allowed_segments:
                kwargs = deepcopy(args)
                quantity = user.quantity(kwargs.get('quantity', 0))
                quantity = round(quantity/lot_size)*lot_size
                kwargs['quantity'] = quantity
                response = user.broker.order_place(**kwargs)
                print(user.broker.client_id, time.monotonic())
            else:
                print("segment not allowed")
                response = {"message": "segment not allowed"}
            return response
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(_place_order, user, order_args) for user in USERS}
        for future in concurrent.futures.as_completed(futures):
            try:
                data = future.result()
                if data:
                    responses.append(data)
            except Exception as e:
                print(e)
    return jsonify(responses)


@app.route('/bracket/<path:varargs>', methods=['GET'])
def bracket(varargs):
    """
    Place bracket order
    """
    responses = []
    order_args = transform(varargs)
    for user in USERS:
        if user not in DISABLED_USERS:
            kwargs = deepcopy(order_args)
            kwargs['quantity'] = user.quantity(kwargs.get('quantity', 0))
            response = user.broker.place_bracket_order(**kwargs)
            responses.append(response)
    return str(responses)


@app.route('/bs/<symbol>/<stop>', methods=['GET'])
def bracket_stop(symbol, stop):
    """
    Modify stop loss of a bracket order by symbol
    """
    responses = []
    first = request.args.get('first')
    p = int(request.args.get('p', 0))
    if first:
        first = True
    else:
        first = False
    n = request.args.get('n', None)
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(USERS)) as executor:
        futures = {executor.submit(user.broker.modify_bracket_stop, symbol,stop,first,p,n) for user in USERS}
        for future in concurrent.futures.as_completed(futures):
            try:
                data = future.result()
                if data:
                    responses.append(data)
            except Exception as e:
                print(e)
    return jsonify(responses)

@app.route('/bt/<symbol>/<target>', methods=['GET'])
def bracket_target(symbol, target):
    """
    Modify stop loss of a bracket order by symbol
    """
    responses = []
    first = request.args.get('first')
    p = int(request.args.get('p', 0))
    if first:
        first = True
    else:
        first = False
    n = request.args.get('n', None)
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(USERS)) as executor:
        futures = {executor.submit(user.broker.modify_bracket_target, symbol,target,first,n,p) for user in USERS}
        for future in concurrent.futures.as_completed(futures):
            try:
                data = future.result()
                if data:
                    responses.append(data)
            except Exception as e:
                print(e)
    return jsonify(responses)

@app.route('/be/<symbol>', methods=['GET'])
def bracket_exit(symbol):
    """
    Modify stop loss of a bracket order by symbol
    """
    responses = []
    first = request.args.get('first')
    p = int(request.args.get('p', 0))
    if first:
        first = True
    else:
        first = False
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(USERS)) as executor:
        futures = {executor.submit(user.broker.exit_bracket_by_symbol,symbol,first,p) for user in USERS}
        for future in concurrent.futures.as_completed(futures):
            try:
                data = future.result()
                if data:
                    responses.append(data)
            except Exception as e:
                print(e)
    return jsonify(responses)

@app.route('/me/<symbol>', methods=['GET'])
def mis_exit(symbol):
    """
    Exit MIS order by symbol
    """
    responses = []
    symbol = str(symbol).upper()
    percent = float(request.args.get('p', 1.0))
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(USERS)) as executor:
        futures = {executor.submit(user.exit_position_by_symbol,symbol,percent,"MIS") for user in USERS}
        for future in concurrent.futures.as_completed(futures):
            try:
                data = future.result()
                if data:
                    responses.append(data)
            except Exception as e:
                print(e)
        return jsonify(responses)

@app.route('/ne/<symbol>', methods=['GET'])
def nrml_exit(symbol):
    """
    Exit NRML order by symbol
    """
    responses = []
    symbol = str(symbol).upper()
    percent = float(request.args.get('p', 1.0))
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(USERS)) as executor:
        futures = {executor.submit(user.exit_position_by_symbol,symbol,percent,"NRML") for user in USERS}
        for future in concurrent.futures.as_completed(futures):
            try:
                data = future.result()
                if data:
                    responses.append(data)
            except Exception as e:
                print(e)
        return jsonify(responses)

@app.route('/pending')
def pending():
    lst = []
    def get_pending_orders(user,timeout=1):
        return user.broker.pending_orders()
    users = [user for user in USERS]
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        future_to_result = {executor.submit(get_pending_orders,user) for user in USERS}
        for future in concurrent.futures.as_completed(future_to_result):
            try:
                data = future.result()
                if data:
                    lst.extend(data)
            except Exception as e:
                print(e)
    return jsonify(lst)

@app.route('/positions')
def positions():
    lst = []
    statuses = []
    all_mtm = 0
    users = [user for user in USERS]
    def check_and_close(user, timeout=1):
        pos = user.broker.positions()
        if type(pos) == list:
            lst.extend(user.broker.positions())
            mtm = user.broker.mtm(pos)
            user.update_mtm(mtm)
            client_id = user.broker.client_id
            max_mtm = user.max_mtm
            dd = (max_mtm-mtm)/(max_mtm+1)
            print(f"{client_id}|Current PnL:{int(mtm)}|Max PnL:{int(max_mtm)}|DD:{dd :.2f}%")
            if user.must_exit_all:
                if user.broker.pending_orders():
                    print('Triggering panic - closing all BO orders')
                    user.exit_all_bracket_orders()
                    user.broker.cancel_all_orders_by_conditions()
                    #requests.get(f"http://127.0.0.1:8181/disable/{client_id}")
                else:
                    print('No pending orders to exit')
                user.exit_all_positions()
            return pos
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(users)) as executor:
        future_to_result = {executor.submit(check_and_close, user) for user in USERS}
        for future in concurrent.futures.as_completed(future_to_result):
            try:
                data = future.result()
                if data:
                    statuses.extend(data)
                    all_mtm += user.broker.mtm(data)
            except Exception as e:
                print(e)
    print(f"Combined MTM: {int(all_mtm)}")
    return jsonify(lst)

@app.route('/panic')
def exit_all():
    # Exit all positions for all users
    users = [user for user in USERS]
    responses = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(users)) as executor:
        future_to_result = {executor.submit(user.exit_all_positions) for user in USERS}
        for future in concurrent.futures.as_completed(future_to_result):
            try:
                data = future.result()
                if data:
                    responses.extend(data)
            except Exception as e:
                print(e)
    return jsonify(responses)

@app.route('/cancel_all')
def cancel_all_orders():
    # Cancel all pending orders for all users
    users = [user for user in USERS]
    responses = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(users)) as executor:
        future_to_result = {executor.submit(user.broker.cancel_all_orders) for user in USERS}
        for future in concurrent.futures.as_completed(future_to_result):
            try:
                data = future.result()
                if data:
                    responses.extend(data)
            except Exception as e:
                print(e)
    return jsonify(responses)

@app.route('/mtm')
def mtm():
    lst =  []
    for user in USERS:
        try:
            mtm = user.broker.mtm()
            max_mtm = user.max_mtm
            client_id = user.broker.client_id
            dct = {
                    'client_id': client_id,
                    'mtm': mtm,
                    'max_mtm': max_mtm
                    }
            lst.append(dct)
        except Exception as e:
            print(e)
    return jsonify(lst)

@app.route('/delay')
def delay():
    # Induce a delay for testing purpose
    import time
    time.sleep(10)
    return 'Hi! This is a delayed response'

@app.route('/users')
def all_users():
    # Return all users
    return jsonify([user.broker.client_id for user in USERS])

@app.route('/disable/<client_id>')
def disable_user(client_id):
    """
    Disable the user with client_id from placing new orders
    This does not prevent from modifying the existing orders
    client_id
        client_id of the user - forced to upper case
    returns the list of all disabled clients
    """
    client_id = str(client_id).upper()
    for user in USERS:
        if user.broker.client_id == client_id:
            DISABLED_USERS.add(user)
            break
    return jsonify([user.broker.client_id for user in DISABLED_USERS])

@app.route('/enable/<client_id>')
def enable_user(client_id):
    """
    Enable a previously disabled user
    This does not prevent from modifying the existing orders
    client_id
        client_id of the user - forced to upper case
    returns the list of all enabled clients
    """
    client_id = str(client_id).upper()
    for user in DISABLED_USERS:
        if user.broker.client_id == client_id:
            DISABLED_USERS.remove(user)
            break
    enabled_users = set(USERS) - DISABLED_USERS
    return jsonify([user.broker.client_id for user in enabled_users])

@app.route('/write_report')
def report():
    def generate_report():
        positions = pd.read_csv('reports/positions.csv')
        pending = pd.read_csv('reports/pending.csv')
        pending['value'] = [(trigger if trigger > 0 else price)*quantity for price,trigger,quantity in zip(pending.price, pending.trigger_price, pending.quantity)]
        pos_grouped = positions.groupby(['symbol']).agg({
            'quantity': sum,
            'average_buy_price': 'mean',
            'average_sell_price': 'mean',
            'net_amount': sum,
            'ltp': 'mean'
            }).reset_index()
        pos_grouped['bep'] = pos_grouped.eval('net_amount/quantity').round(2).abs()
        pending_grouped = pending.groupby(['symbol', 'side',
            'order_type']).agg({
            'quantity': sum,
            'value': sum,
            })
        pending_grouped['avg_price'] = pending_grouped.eval('value/quantity')

        pending_grouped = pending_grouped.unstack(level='order_type').reset_index().rename(
                    columns={'trading_symbol': 'symbol'})

        grp = pos_grouped.merge(pending_grouped, on=['symbol'])
        grp.index = grp.index+1
        columns = ['symbol', 'quantity', 'bep', 'ltp', ('avg_price', 'LIMIT'),
                ('avg_price', 'SL')]
        columns = [col for col in columns if col in grp.columns]
        grp[columns].to_csv('reports/reports.csv')
    try:
        generate_report()
        return f"Report generated succesfully"
    except Exception as e:
        return str(e)

@app.route('/modify/<path:varargs>', methods=['GET'])
def modify(varargs):
    """
    Place a MIS order
    """
    responses = []
    filter_args = transform(varargs)
    exchange = filter_args.get('exchange', 'NFO')
    # Check for modifications
    price = filter_args.pop('price', 0)
    quantity = filter_args.pop('quantity', 0)
    trigger_price = filter_args.pop('trigger_price', 0)
    if trigger_price:
        filter_args['status'] = 'trigger pending'
    else:
        filter_args['status'] = 'open'
    n = filter_args.pop('n', 0)
    modifications = {}
    if quantity:
        modifications['quantity'] = quantity
    modifications['price' ] = price
    modifications['trigger_price'] = trigger_price
    for user in USERS:
        if user not in DISABLED_USERS:
            response = user.broker.modify_all_orders_by_conditions(modifications,
                    n=n,**filter_args)
            responses.append(response)
    return str(responses)

@app.route('/cancel/<path:varargs>', methods=['GET'])
def cancel(varargs):
    """
    Place a MIS order
    """
    responses = []
    filter_args = transform(varargs)
    n = filter_args.pop('n', 0)
    exchange = filter_args.get('exchange', 'NFO')
    # Check for modifications
    for user in USERS:
        if user not in DISABLED_USERS:
            response = user.broker.cancel_all_orders_by_conditions(n=n,**filter_args)
            responses.append(response)
    return str(responses)

bo_string = "exc=NSE/sym=TATAPOWER-EQ/qty=4/val=DAY/sq_val=1/sl_val=1/pr=82.8/tsl=1/ot=LIMIT/prd=BO/side=BUY/user_order_id=10003"

if __name__ == "__main__":
    app.run(port=8181, debug=True)
