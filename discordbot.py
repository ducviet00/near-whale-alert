import asyncio
import time

import aiohttp
import discord
import psycopg2

from utils.db_queries import *

# TOKEN = input("INPUT TOKEN:\n")


class WhaleCord(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn = psycopg2.connect(CONNECTION, **keepalive_kwargs)
        self.transactions = asyncio.Queue()
        self.last_time = str(int(time.time() * 1000)) + "000000"
        self.near_price = 3.14
        self.channels = []
        self.producer = self.loop.create_task(self.stream_database())
        self.consumer = self.loop.create_task(self.alert())
        self.get_price = self.loop.create_task(self.get_near_price())

    async def on_ready(self):
        print("Logged in as")
        print(self.user.name)
        print(self.user.id)
        print("------")
        with open("subcribers.txt", "r") as f:
            id_channels = f.readlines()
            id_channels = [int(i.strip()) for i in id_channels]
            id_channels = list(set(id_channels))
        self.channels = [self.get_channel(i) for i in id_channels]
        self.channels = [c for c in self.channels if c]  # Remove deleted channels

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
        await self.wait_until_ready()
        while True:
            record = await self.transactions.get()
            print(f"Length of queue: {self.transactions.qsize()}")
            amount = int(record[-1]["deposit"][0:-21]) / 1e3
            amount_usd = amount * self.near_price
            if amount_usd > 100_000:
                alert = "🚨" * min(int(amount_usd // 100_000), 10)
                tx_hash = record[1]
                sender = record[2] if ".near" in record[2] else "unkown"
                receiver = record[3] if ".near" in record[3] else "unkown"
                for i, channel in enumerate(self.channels):
                    try:
                        await channel.send(
                            f"{alert}\n"
                            f"{amount:,.2f} NEAR ({amount_usd:,.0f} USD) "
                            f"transferred from {sender} to {receiver} wallet\n"
                            f"https://explorer.near.org/transactions/{tx_hash}"
                        )
                    except:
                        self.channels.pop(i)

    async def get_near_price(self):
        while True:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://api.binance.com/api/v3/ticker/price?symbol=NEARUSDT"
                ) as response:
                    res = await response.json()
                    self.near_price = float(res["price"])

            await asyncio.sleep(300)

    async def on_message(self, message):
        # we do not want the bot to reply to itself
        if message.author.id == self.user.id:
            return
        if message.content.startswith("$add-whale"):
            if message.channel in self.channels:
                await message.channel.send(
                    f"Channel {message.channel.name} is already in the subscriber channels"
                )
                return
            print(f"Adding channel {message.channel.name} id: {message.channel.id}")
            with open("subcribers.txt", "a") as f:
                f.write(f"{message.channel.id}\n")
            self.channels.append(message.channel)
            await message.channel.send(
                f"Added channel {message.channel.name} to the subscriber channels"
            )


client = WhaleCord()
client.run(TOKEN)
