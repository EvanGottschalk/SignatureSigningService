import os
import sys
import json

import sqlite3
import pandas

from eth_account import Account
import secrets
from web3 import Web3, HTTPProvider

def main():
    SSS = SignatureSigningService()
    SSS.main()
    del SSS

class SignatureSigningService:
    def __init__(self):
        goerli_URL = 'https://goerli.infura.io/v3/'
        self.test_account = {'public': '0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266',
                             'private': '0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80'}

    def main(self):
        batch_size = int(input('\nPlease enter your preferred batch size.\nBatch Size: '))
        self.connectWeb3()
        random_data_list = self.initializeServer()
        account_list = []
        for num in range(100):
            account = self.generateKeyPair()
            account_list.append(account)
        batch_transaction_count = 0
        total_transaction_count = 0
        account_id = 0
        for random_data in random_data_list:
            transaction_signature = self.signTransaction(account_list[account_id], data=random_data)
            self.cursor.execute(
                "INSERT INTO signatures_table (data_id, signature) VALUES (" + str(total_transaction_count) + ", '" + str(transaction_signature) + "')")
            batch_transaction_count += 1
            total_transaction_count += 1
            if batch_transaction_count >= batch_size:
                batch_transaction_count = 0
                account_id = (account_id + 1) % 100
        self.cursor.execute('''
          SELECT
          a.data,
          b.signature
          FROM data_table a
          LEFT JOIN signatures_table b ON a.data_id = b.data_id
          ''')
        self.database_connection.commit()
        dataframe = pandas.DataFrame(self.cursor.fetchall(), columns=['data', 'signature'])
        print(dataframe)
        return(random_data_list)

    def connectWeb3(self):
        self.web3 = Web3(HTTPProvider('http://127.0.0.1:8545/'))

    def initializeServer(self):
        random_data_list = []
        self.database_connection = sqlite3.connect('new_database') 
        self.cursor = self.database_connection.cursor()
        self.cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS data_table
            ([data_id] INTEGER PRIMARY KEY, [data] TEXT)
            ''')
        self.cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS signatures_table
            ([data_id] INTEGER PRIMARY KEY, [signature] TEXT)
            ''')               
        self.database_connection.commit()
        for num in range(100000):
            random_data = self.generateRandomData()
            random_data_list.append(random_data)
            self.cursor.execute(
                "INSERT INTO data_table (data_id, data) VALUES (" + str(num) + ", '" + random_data + "')")
        return(random_data_list)

    def generateKeyPair(self):
        account = self.web3.eth.account.create(self.generateRandomData())
        return(account)

    def generateRandomData(self):
        data = secrets.token_hex(32)
        return(data)

    def signTransaction(self, account, data=None):
        nonce = self.web3.eth.getTransactionCount(self.test_account['public'])
        transaction = {
            'from': self.test_account['public'],
            'to': account.address,
            'value': self.web3.toWei(0.001, 'ether'),
            'gas': self.web3.toWei(50000, 'wei'),
            'gasPrice': self.web3.toWei(1, 'gwei'),
            'nonce': nonce,
            'chainId': 1337}
        signed_transaction = self.web3.eth.account.sign_transaction(transaction, self.test_account['private'])
        transaction_hash = self.web3.eth.sendRawTransaction(signed_transaction.rawTransaction)
        nonce = self.web3.eth.getTransactionCount(account.address)
        transaction = {
            'from': account.address,
            'to': account.address,
            'value': self.web3.toWei(0.0000001, 'ether'),
            'gas': self.web3.toWei(50000, 'wei'),
            'gasPrice': self.web3.toWei(1, 'gwei'),
            'nonce': nonce,
            'chainId': 1337}
        if data:
            transaction['data'] = self.web3.toHex(bytes(data, 'ascii'))
        signed_transaction = self.web3.eth.account.sign_transaction(transaction, account.privateKey.hex())
        transaction_hash = self.web3.eth.sendRawTransaction(signed_transaction.rawTransaction)
        #print(data, transaction_hash)
        return(transaction_hash.hex())
        #print ("Latest Ethereum block number" , self.web3.eth.blockNumber)
        #print(self.web3.toHex(transactionHash))
        #print(self.web3.eth.get_transaction(transactionHash))
        #print(account.address)
        #print(self.web3.eth.get_balance(account.address))

        



if __name__ == "__main__":
    main()
