import asyncio
import time

import discord
import psycopg2

from utils.db_queries import *

TOKEN = input("INPUT TOKEN:\n")


class WhaleCord(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn = psycopg2.connect(CONNECTION, **keepalive_kwargs)
        self.transactions = asyncio.Queue()
        self.last_time = str(int(time.time() * 1000)) + "000000"
        self.channels = []
        self.producer = self.loop.create_task(self.stream_database())
        self.consumer = self.loop.create_task(self.alert())

    async def on_ready(self):
        print("Logged in as")
        print(self.user.name)
        print(self.user.id)
        print("------")

    async def stream_database(self):
        while True:
            cur = self.conn.cursor()
            cur.execute(
                f"""
                SELECT transactions.block_timestamp, transactions.transaction_hash, transactions.signer_account_id,
                    transactions.receiver_account_id, actions.args
                FROM transaction_actions actions
                INNER JOIN transactions ON transactions.transaction_hash = actions.transaction_hash
                WHERE actions.action_kind = 'TRANSFER' and transactions.block_timestamp >  {self.last_time}
                ORDER BY transactions.block_timestamp asc;
                """
            )
            # col_names = cur.description
            # col_names = [c.name for c in col_names]
            if cur.rowcount:
                print(f"Adding {cur.rowcount} transactions to the queue")
            for record in cur:
                await self.transactions.put(record)
                self.last_time = record[0]  # block_timestamp
            await asyncio.sleep(30)

    async def alert(self):
        while True:
            record = await self.transactions.get()
            print(f"Length of queue: {self.transactions.qsize()}")
            amount = int(record[-1]["deposit"][0:-21]) / 1e3
            if amount > 1000:
                tx_hash = record[1]
                sender = record[2] if ".near" in record[2] else "unkown"
                receiver = record[3] if ".near" in record[3] else "unkown"
                for channel in self.channels:
                    await channel.send(
                        f"""
                        {amount} NEAR transferred from {sender} to {receiver} wallet
                        https://explorer.near.org/transactions/{tx_hash}"
                        """
                    )

    async def on_message(self, message):
        # we do not want the bot to reply to itself
        if message.author.id == self.user.id:
            return
        if message.content.startswith("$add-whale"):
            print(f"Adding channel {message.channel.name} id: {message.channel.id}")
            self.channels.append(message.channel)
            await message.channel.send(
                f"Added channel {message.channel.name} to the subscriber channels"
            )


client = WhaleCord()
client.run(TOKEN)
