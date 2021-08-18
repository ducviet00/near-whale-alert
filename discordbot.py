import asyncio
import json
import time

import aiohttp
import discord
import psycopg2

from utils import backup_subcribers
from utils.db_queries import *
from utils.exchange_wallet import detect_wallet

TOKEN = input("INPUT TOKEN:\n")


class WhaleCord(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn = psycopg2.connect(CONNECTION, **keepalive_kwargs)
        self.transactions = asyncio.Queue()
        self.last_time = str(int(time.time() * 1000)) + "000000"
        self.near_price = 3.14
        self.channels = dict()
        self.producer = self.loop.create_task(self.stream_database())
        self.consumer = self.loop.create_task(self.alert())
        self.get_price = self.loop.create_task(self.get_near_price())

    async def on_ready(self):
        print("Logged in as")
        print(self.user.name)
        print(self.user.id)
        print("------")
        try:
            with open("subcribers.json", "r") as f:
                self.channels = json.load(f)
            self.channels = {int(k): v for k, v in self.channels.items()}
        except:
            pass
        print(len(self.channels))

    async def stream_database(self):
        while True:
            cur = self.conn.cursor()
            cur.execute(
                """
                SELECT transactions.block_timestamp, transactions.transaction_hash, transactions.signer_account_id,
                    transactions.receiver_account_id, actions.args
                FROM transaction_actions actions
                INNER JOIN transactions ON transactions.transaction_hash = actions.transaction_hash
                WHERE actions.action_kind = 'TRANSFER' and transactions.block_timestamp >  %s
                ORDER BY transactions.block_timestamp asc;
                """,
                (self.last_time,),
            )
            # col_names = cur.description
            # col_names = [c.name for c in col_names]
            if cur.rowcount:
                print(f"Adding {cur.rowcount} transactions to the queue")
            for record in cur:
                await self.transactions.put(record)
                self.last_time = str(record[0])  # block_timestamp
            await asyncio.sleep(30)

    async def alert(self):
        await self.wait_until_ready()
        while True:
            try:
                record = await self.transactions.get()
                print(f"Length of queue: {self.transactions.qsize()}")
                if not record[-1]["deposit"].strip():
                    continue
                # Get transaction information
                amount = int(record[-1]["deposit"][0:-21]) / 1e3
                amount_usd = amount * self.near_price
                alert = "🚨" * min(int(amount_usd // 100_000), 10)
                tx_hash = record[1]
                sender = detect_wallet(record[2])
                receiver = detect_wallet(record[3])
                # Send to subcribers list
                for channel_id, threshold in self.channels.items():
                    if amount_usd > threshold:
                        try:
                            channel = self.get_channel(channel_id)
                            await channel.send(
                                f"{alert}\n"
                                f"{amount:,.2f} NEAR ({amount_usd:,.0f} USD) "
                                f"transferred from {sender} to {receiver} wallet\n"
                                f"https://explorer.near.org/transactions/{tx_hash}"
                            )
                        except:
                            del self.channels[channel_id]

            except Exception as e:
                print(e)

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
        args = message.content.split(" ")
        # Add channel
        if message.content.startswith("$add-whale-alert"):
            if message.channel.id in self.channels:
                await message.channel.send(
                    f"Channel {message.channel.name} is already in the subscriber channels"
                )
                return
            try:
                threshold = float(args[-1])
            except:
                threshold = 200_000
            print(f"Adding channel {message.channel.name} id: {message.channel.id}")
            self.channels[message.channel.id] = threshold
            # Backup subcribers list
            backup_subcribers(self.channels)
            await message.channel.send(
                f"Added channel {message.channel.name} with threshold {threshold:,.1f}USD to the subscriber channels"
            )
        # Remove channel
        if message.content.startswith("$rm-whale-alert"):
            if message.channel.id in self.channels:
                del self.channels[message.channel.id]
                await message.channel.send(
                    f"Channel {message.channel.name} was removed in the subscriber channels"
                )
                # Backup subcribers list
                backup_subcribers(self.channels)
                return
            await message.channel.send(
                f"Channel {message.channel.name} is not in the subscriber channels"
            )
            return
        # Change threshold
        if message.content.startswith("$set-alert"):
            if message.channel.id not in self.channels:
                await message.channel.send(
                    f"Channel {message.channel.name} is not in the subscriber channels"
                )
                return
            if len(args) != 2:
                await message.channel.send(f"Invalid {message.channel.name} command")
                return
            try:
                threshold = float(args[-1])
            except:
                await message.channel.send(f"Invalid {message.channel.name} command")
                return
            self.channels[message.channel.id] = threshold
            await message.channel.send(
                f"Change channel {message.channel.name} with threshold {threshold:,.1f}USD"
            )
            # Backup subcribers list
            backup_subcribers(self.channels)
            return


client = WhaleCord()
client.run(TOKEN)
