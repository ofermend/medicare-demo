#!/bin/bash

# Usage: find-anomalies.py pct

if [[ -z $1 ]]; then
	pct="100"
else
	pct="$1"
fi

./socialite/bin/socialite code/find-anomalies.py "medicare_$pct" | tee "results-$pct.txt"

