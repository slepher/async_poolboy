async_poolboy
=====

works like https://github.com/devinus/poolboy

but with these features added

h1. functions added

promise_call/2, promise_call/3, promise_transaction/2, promise_transaction/3

h1. poolboy worker checkout worker

while a poolboy worker firstime checkout a poolboy worker, a new worker is created.
while the last process checkouted by a poolboy worker checkin, the worker checkedin is destroyed.
