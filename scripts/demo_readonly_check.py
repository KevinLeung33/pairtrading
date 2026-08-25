from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from okx_pair_executor.config import AppConfig
from okx_pair_executor.okx_client import OkxV5Client


async def main() -> None:
    config = AppConfig.from_env()
    if not config.demo:
        raise SystemExit("Refusing non-demo environment. Set OKX_DEMO=1.")
    client = OkxV5Client(config.api_key, config.secret_key, config.passphrase, demo=True)
    spot = await client.instrument_rules(config.spot_inst_id)
    swap = await client.instrument_rules(config.swap_inst_id)
    print("Demo API authenticated")
    print(f"spot={config.spot_inst_id} lot={spot.lot_size} min={spot.min_size}")
    print(f"swap={config.swap_inst_id} lot={swap.lot_size} min={swap.min_size} ctVal={swap.contract_value}")
    snapshot = await client.account_snapshot([config.spot_inst_id, config.swap_inst_id])
    position_contracts = snapshot.get("positions", {}).get(config.swap_inst_id, "0")
    position_base = Decimal(position_contracts) * swap.contract_value
    print(f"balances BTC={snapshot.get('balances', {}).get('BTC', '0')} USDT={snapshot.get('balances', {}).get('USDT', '0')}")
    print(f"swap_position contracts={position_contracts} base_qty={position_base} sign=long(+)/short(-)")
    print("No orders were placed.")


if __name__ == "__main__":
    asyncio.run(main())
