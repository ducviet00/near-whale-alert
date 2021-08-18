EX_WALLET = {
    "7747991786f445efb658b69857eadc7a57b6b475beec26ed14da8bc35bb2b5b6": "Binance",
    "601483a1b22699b636f1df800b9b709466eba4e1d5ce7c2e1e20317af8bbd1f3": "exchange",
    "7981ae28a8314f939c7b2d0cb4fca0cb0ea70e5c022b219425bc9c4d5723e8ce": "exchange",
    "d73888a2619c7761735f23c798536145dfa87f9306b5f21275eb4b1a7ba971b9": "exchange",
    "0d584a4cbbfd9a4878d816512894e65918e54fae13df39a6f520fc90caea2fb0": "exchange",
}


def detect_wallet(wallet_id):
    if wallet_id in EX_WALLET:
        return EX_WALLET[wallet_id]
    if wallet_id.endswith(".near"):
        return wallet_id
    return "unknow"
