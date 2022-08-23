import datetime
import time

from xtb.wrapper.logger import logger
from xtb.wrapper.xtb_client import APIClient, CommandFailed

MIN_REQUEST_INTERVAL = 0.200


class ChartLastRequest(object):
    def __init__(self, client: APIClient):
        self.client = client
        self.lastRequestTime = time.time()

    @staticmethod
    def decide_on_days_history(period: int) -> int:
        result = 1
        if period == 1:
            result = 30
        elif period == 5:
            result = 30
        elif period == 15:
            result = 30
        elif period == 30:
            result = 120
        elif period == 60:
            result = 120
        elif period == 240:
            result = 360
        elif period == 1440:
            result = 360

        return result

    @staticmethod
    def get_chart_start(days: int):
        now = datetime.datetime.now()
        startFrom = now - datetime.timedelta(days=days)
        toTimestamp = datetime.datetime.timestamp(startFrom) * 1000

        return toTimestamp

    @staticmethod
    def prepare_info(period: int, start: float, symbol: str) -> dict:
        info = dict(period=period, start=start, symbol=symbol)
        infoWrapper = dict(info=info)

        return infoWrapper

    def get_chart_last_request(self, period: int, daysRequested: int, symbol: str) -> list[dict]:
        # logger.info("requested {} days".format(daysRequested))
        start = self.get_chart_start(daysRequested)
        info = self.prepare_info(period, start, symbol)
        responseCommand = self.client.commandExecute("getChartLastRequest", info)

        self.lastRequestTime = time.time()

        if responseCommand["status"] is False:
            raise (responseCommand)

        return responseCommand

    @staticmethod
    def transform_candles(response):
        candle_history = []

        for candle in response['rateInfos']:
            _pr = candle['open']
            op_pr = _pr / 10 ** response['digits']
            cl_pr = (_pr + candle['close']) / 10 ** response['digits']
            hg_pr = (_pr + candle['high']) / 10 ** response['digits']
            lw_pr = (_pr + candle['low']) / 10 ** response['digits']
            new_candle_entry = {'timestamp': candle['ctm'] / 1000, 'open':
                op_pr, 'close': cl_pr, 'high': hg_pr, 'low': lw_pr,
                                'volume': candle['vol']}
            candle_history.append(new_candle_entry)

        return candle_history

    def request_with_limit(self, period: int, daysRequested: int, symbol: str) -> list[dict]:
        try:
            response = self.get_chart_last_request(period, daysRequested, symbol)

            return self.transform_candles(response['returnData'])
        except CommandFailed as cf:
            if cf.err_code == "EX009":
                newDaysRequested = int(round(0.95 * daysRequested))
                logger.info("new days requested: {}".format(newDaysRequested))

                requestAttemptTime = time.time()
                if MIN_REQUEST_INTERVAL > (requestAttemptTime - self.lastRequestTime):
                    time.sleep(MIN_REQUEST_INTERVAL)

                self.request_with_limit(period, newDaysRequested, symbol)
            else:
                logger.error(cf.msg)
                raise cf
        except Exception as ex:
            logger.error(ex)
            raise ex

    def request_candle_history_with_limit(self, symbol: str, period: int) -> list[dict]:
        initialDaysRequested = self.decide_on_days_history(period)

        return self.request_with_limit(period, initialDaysRequested, symbol)
