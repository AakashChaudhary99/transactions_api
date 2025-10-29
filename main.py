from fastapi import FastAPI, BackgroundTasks, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import os
from datetime import datetime
import threading
import pytz
import json
import time

ist = pytz.timezone('Asia/Kolkata')

app = FastAPI()

TRANSACTIONS = 'transactions.json'
LOCK = threading.Lock()
@app.get('/', status_code=200)
def health_check():
    return {"status": "HEALTHY", "current_time": datetime.now(ist).strftime("%Y-%m-%dT%H:%M:%S")}

class TransactionBody(BaseModel):
    transaction_id: str
    source_account: str
    destination_account: str
    amount: float
    currency: str


def process_transaction(tx: TransactionBody):
    try:
        with LOCK:
            with open(TRANSACTIONS, 'r+') as f:
                transactions = json.load(f)

                exists = False
                for t in transactions:
                    if t['transaction_id'] == tx.transaction_id:
                        exists = True
                        break

                if exists:
                    print(f"Transaction {tx.transaction_id} already exists!")
                    return

                if not exists:
                    new_tx = {
                        "transaction_id": tx.transaction_id,
                        "source_account": tx.source_account,
                        "destination_account": tx.destination_account,
                        "amount": tx.amount,
                        "currency": tx.currency,
                        "status": "PROCESSING",
                        "created_at": datetime.now(ist).strftime("%Y-%m-%dT%H:%M:%S")
                    }
                    transactions.append(new_tx)

                f.seek(0)
                json.dump(transactions, f, indent=2)
                f.truncate()
        print("Processing Transactions")
        time.sleep(30)

        with LOCK:
            with open(TRANSACTIONS, "r+") as f:
                transactions = json.load(f)
                for t in transactions:
                    if t["transaction_id"] == tx.transaction_id:
                        t["status"] = "PROCESSED"
                        t["processed_at"] = datetime.now(ist).strftime("%Y-%m-%dT%H:%M:%S")
                        break

                f.seek(0)
                json.dump(transactions, f, indent=2)
                f.truncate()

    except Exception as e:
        print("Error occurred in process_transaction", e)


@app.post('/v1/webhooks/transactions')
def create_transactions(data:TransactionBody, background_tasks: BackgroundTasks):
    try:
        background_tasks.add_task(process_transaction, data)
        return JSONResponse(status_code=202, content={"message": "Transaction received", "transaction_id": data.transaction_id})

    except Exception as e:
        print("Error occurred in creating transaction", e)

@app.get('/v1/transactions/{transaction_id}')
def get_by_id(transaction_id):
    try:
        with open(TRANSACTIONS, 'r') as f:
            transactions = json.load(f)

            for t in transactions:
                if t['transaction_id'] == f"{transaction_id}":
                    return t
            # if not tx:
            return JSONResponse(status_code=404, content={"message": f"Transaction {transaction_id} not found"})
            #
            # return tx

    except Exception as e:
        print("Error occurred while getting transaction by id for id", transaction_id, e)
