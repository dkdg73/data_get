import time 

start = time.time()

import equity_dg
equity_time = round(time.time() - start, 1)
print(f'equity data uptodate; {equity_time}s run time')

startcredit = time.time()
import credit_dg
credit_time = round(time.time() - startcredit, 1)
print(f'credit data uptodate; {credit_time}s run time')

startgov = time.time()
import gov_dg
gov_time = round(time.time()- startgov, 1)
print(f'gov data uptodate; {gov_time}s run time')

startinf = time.time()
import inflation_dg
inflation_time = round(time.time() - startinf, 1)
print(f'inflation data uptodate; {inflation_time}s run time')

startmoney = time.time()
import money_dg
money_time = round(time.time() - startmoney, 1)
print(f'money data uptodate; {money_time}s run time')

starttips = time.time()
import tips_dg
tips_time = round(time.time() - starttips, 1)
print(f'TIPS data uptodate; {tips_time}s run time')

startgfd = time.time()
import gfddata
gfd_time = round(time.time() - starttips, 1)
print(f'GFD data uptodate; {gfd_time}s run time')


total_run_time = round(time.time() - start, 1)
print(f'Total runtime: {total_run_time}s')

