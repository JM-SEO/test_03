# api/api_quotation.py

from .api_request import APIRequest

api = APIRequest(base_url="https://api.upbit.com/v1")


def get_all_markets():
    """업비트 전체 마켓 목록 조회 (KRW-*, BTC-*, USDT-* 등)"""
    return api.get("/market/all")


def get_krw_tickers():
    """KRW 마켓에 해당하는 티커 목록 반환"""
    markets = get_all_markets()
    return [m['market'] for m in markets if m['market'].startswith("KRW-")]


def get_current_price(market: str):
    """특정 마켓의 현재가 (trade_price) 조회"""
    result = api.get("/ticker", params={"markets": market})
    return result[0]['trade_price'] if result else None


def get_multiple_current_prices(markets: list):
    """여러 마켓의 현재가 조회 (list of market codes)"""
    result = api.get("/ticker", params={"markets": ",".join(markets)})
    return {item['market']: item['trade_price'] for item in result} if result else {}
