import datetime
import unicodedata


def get_next_date(today:datetime,days:int=1)->datetime:
  """ Return
    Next_date
  """
  next_date = today + datetime.timedelta(days=days)
  return next_date

def get_prev_date(today:datetime, days:int)->datetime:
  """ Return
    Prev_date
  """
  prev_date = today - datetime.timedelta(days=days)
  return prev_date


def preformat_cjk (string, width, align='<', fill=' '):
  """
    UNICODE - ALIGN
  """
  count = (width - sum(1 + (unicodedata.east_asian_width(c) in "WF")
                        for c in string))
  return {
      '>': lambda s: fill * count + s,
      '<': lambda s: s + fill * count,
      '^': lambda s: fill * (count / 2)
                      + s
                      + fill * (count / 2 + count % 2)
}[align](string)