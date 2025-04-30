from kafka import KafkaConsumer
from json import loads
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from os import path
from datetime import datetime
import numpy as np

db = SQLAlchemy()

def create_app():
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database.db'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db.init_app(app)

    if not path.exists('database.db'):
        with app.app_context():
            db.create_all()

    return app

class Transaction(db.Model):
    __tablename__ = 'transactions'

    id = db.Column(db.Integer, primary_key=True)
    custid = db.Column(db.Integer)
    type = db.Column(db.String(3))
    date = db.Column(db.DateTime)
    amt = db.Column(db.Integer)

    def __repr__(self):
        return f"<Transaction(custid={self.custid}, type='{self.type}', date='{self.date}', amt={self.amt})>"

class XactionConsumer:
    def __init__(self, app):
        self.app = app
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            group_id='transaction-consumer-group-sqlite',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        self.ledger = {}
        self.deposits = []
        self.withdrawals = []
        self.consumer_balance = {}

    def store_transaction(self, message):
        with self.app.app_context():
            try:
                timestamp = datetime.fromtimestamp(message['date'])
                db_transaction = Transaction(
                    custid=message['custid'],
                    type=message['type'],
                    date=timestamp,
                    amt=message['amt']
                )
                db.session.add(db_transaction)
                db.session.commit()
                print(f"Transaction stored in DB: {db_transaction}")
            except Exception as e:
                db.session.rollback()
                print(f"Error storing transaction in DB: {e}")
    
    def update_summary(self, transaction):
        if transaction['type'].lower() == 'dep':
            self.deposits.append(transaction['amt'])
        elif transaction['type'].lower() == 'wth':
            self.withdrawals.append(transaction['amt'])
        self.print_summary()

    def print_summary(self):
        if self.deposits:
            mean_deposit = np.mean(self.deposits)
            std_dev_deposit = np.std(self.deposits)
        else:
            mean_deposit = 0
            std_dev_deposit = 0

        if self.withdrawals:
            mean_withdrawal = np.mean(self.withdrawals)
            std_dev_withdrawal = np.std(self.withdrawals)
        else:
            mean_withdrawal = 0
            std_dev_withdrawal = 0

        print("\n--- Numerical Summary ---")
        print(f"Mean Deposit: ${mean_deposit:.2f}")
        print(f"Std Dev Deposit: ${std_dev_deposit:.2f}")
        print(f"Mean Withdrawal: ${mean_withdrawal:.2f}")
        print(f"Std Dev Withdrawal: ${std_dev_withdrawal:.2f}")
        print("-------------------------")

    def update_balance(self, transaction):
        custid = transaction['custid']
        amount = transaction['amt']
        transaction_type = transaction['type'].lower()

        if custid not in self.consumer_balance:
            self.consumer_balance[custid] = 0

        if transaction_type == 'dep':
            self.consumer_balance[custid] += amount
        elif transaction_type == 'wth':
            self.consumer_balance[custid] -= amount
        print(f"Current Balances (in-memory): {self.consumer_balance}")

    def limit_consumer(self, transaction, lower_limit=-5000, upper_limit=5000):
        exceeding_ids = []
        custid = transaction['custid']
        balance = self.consumer_balance.get(custid)
        for custid, balance in self.consumer_balance.items():
            if balance < lower_limit or balance > upper_limit:
                exceeding_ids.append(custid)
        print(f"\n--- Customers Exceeding Limit [${lower_limit}, ${upper_limit}] ---")
        print(f"IDs: {exceeding_ids}")
        print("------------------------------------------------------------------")


    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            self.store_transaction(message)
            self.update_summary(message)
            self.update_balance(message)
            self.limit_consumer(message)

if __name__ == "__main__":
    app = create_app()
    consumer = XactionConsumer(app)
    consumer.handleMessages()