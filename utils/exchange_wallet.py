EX_WALLET = {
    "7747991786f445efb658b69857eadc7a57b6b475beec26ed14da8bc35bb2b5b6": "binance"
}


def detect_wallet(wallet_id):
    if wallet_id in EX_WALLET:
        return EX_WALLET[wallet_id]
    if wallet_id.endswith(".near"):
        return wallet_id
    return "unknow"
