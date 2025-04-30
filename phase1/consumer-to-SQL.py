from kafka import KafkaConsumer
from json import loads
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from os import path
from datetime import datetime

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
        self.custBalances = self.load_customer_balances()

    def load_customer_balances(self):
        balances = {}
        with self.app.app_context():
            try:
                all_transactions = Transaction.query.all()
                for transaction in all_transactions:
                    custid = transaction.custid
                    if custid not in balances:
                        balances[custid] = 0
                    if transaction.type == 'Dep':
                        balances[custid] += transaction.amt
                    elif transaction.type == 'Wth':
                        balances[custid] -= transaction.amt
            except Exception as e:
                print(f"Error loading customer balances: {e}")
        return balances

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

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            self.store_transaction(message)
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)

if __name__ == "__main__":
    app = create_app()
    consumer = XactionConsumer(app)
    consumer.handleMessages()