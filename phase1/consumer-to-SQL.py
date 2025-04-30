from kafka import KafkaConsumer, TopicPartition
from json import loads
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from os import path

ledger = SQLAlchemy()

def create_app():    
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database.ledger'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    ledger.init_app(app)

    if not path.exists('database.ledger'):
        with app.app_context():
            ledger.create_all()

    return app

class Transaction(ledger_db.Model):
    __tablename__ = 'transactions'

    id = ledger_db.Column(ledger_db.Integer, primary_key=True)
    custid = ledger_db.Column(ledger_db.Integer)
    type = ledger_db.Column(ledger_db.String(3))
    date = ledger_db.Column(ledger_db.DateTime)
    amt = ledger_db.Column(ledger_db.Integer)

    def __repr__(self):
        return f"<Transaction(custid={self.custid}, type='{self.type}', date='{self.date}', amt={self.amt})>"

class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            group_id='transaction-consumer-group-sqlite',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.

        #Go back to the readme.
    def load_customer_balances(self):
        """Loads customer balances from the database."""
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
        """Stores a transaction in the database."""
        with self.app.app_context():
            try:
                timestamp = datetime.fromtimestamp(message['date'])
                db_transaction = Transaction(
                    custid=message['custid'],
                    type=message['type'],
                    date=timestamp,
                    amt=message['amt']
                )
                ledger_db.session.add(db_transaction)
                ledger_db.session.commit()
                print(f"Transaction stored in DB: {db_transaction}")
            except Exception as e:
                ledger_db.session.rollback()
                print(f"Error storing transaction in DB: {e}")

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            self.store_transaction(message)
            # add message to the transaction table in your SQL usinf SQLalchemy
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