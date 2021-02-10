import time 

start = time.time()

import equity_dg
equity_time = time.time() - start
print(f'equity data uptodate; {equity_time}s run time')

import credit_dg
credit_time = time.time() - equity_time
print(f'credit data uptodate; {credit_time}s run time')

import gov_dg
gov_time = time.time()-credit_time
print(f'gov data uptodate; {gov_time}s run time')

import inflation_dg
inflation_time = time.time() - gov_time
print(f'inflation data uptodate; {inflation_time}s run time')

import money_dg
money_time = time.time() - inflation_time
print(f'money data uptodate; {money_time}s run time')

import tips_dg
tips_time = time.time() - money_time
print(f'TIPS data uptodate; {tips_time}s run time')

print(f'Total runtime: {tips_time - start}s')

