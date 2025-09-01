#!/bin/bash
set -euo pipefail

startDate=$(date -v-5y -v-2d +%F)
endDate=$(date +%F)

IFS='-' read -r startYear startMonth startDay <<<"${startDate}"
IFS='-' read -r endYear endMonth endDay <<<"${endDate}"

# TODO: need to get acessKey
rclone config create s3polygon s3 env_auth=false access_key_id=$(get) secret_access_key=$(get) endpoint=https://files.polygon.io

currYear=$startYear
while [ "$currYear" -le "$endYear" ]; do
    echo getting year $currYear
    if [ "$currYear" -eq "$startYear" ]; then
        monthStart=$((10#$startMonth))
    else
        monthStart=1
    fi
    if [ "$currYear" -eq "$endYear" ]; then
        monthEnd=$endMonth
    else
        monthEnd=12
    fi

    if [ "$currYear" -ne "$startYear" ]; then
        echo Get $currYear ------------
        rclone sync s3polygon:flatfiles/us_stocks_sip/day_aggs_v1/${currYear} ./flatfiles/$currYear --progress --ignore-existing --transfers 100 --checkers 100 -vv || echo Error on sync for year === $currYear
    else
        currMonth=$monthStart
        while [ "$currMonth" -le "$monthEnd" ]; do
            if [ "$currMonth" -le 9 ]; then
                monthStr="0$currMonth"
            else
                monthStr=$currMonth
            fi
            yearDir=./flatfiles/$currYear/$monthStr
            mkdir -p $yearDir
            lastDay=$(date -v+1m -v-1d -j -f %F "${currYear}-${currMonth}-01" +%d)

            fromDay=1
            toDay=$lastDay
            if [ "$currYear" -eq "$startYear" ] && [ "$currMonth" -eq "$startMonth" ]; then
                fromDay=$startDay
            fi
            if [ "$currYear" -eq "$endYear" ] && [ "$currMonth" -eq "$endMonth" ]; then
                toDay=$endDay
            fi

            if [ $fromDay -eq 1 ] && [ $toDay -eq $lastDay ]; then
                rclone copy s3polygon:flatfiles/us_stocks_sip/day_aggs_v1/$currYear/$monthStr $yearDir --progress --ignore-existing -vv || echo Error on copy month $monthStr not found
            else
                for ((i = $fromDay; i <= $toDay; i++)); do
                    dayStr=$i
                    if [ $dayStr -le 9 ]; then
                        dayStr="0"$dayStr
                    fi
                    fileName="$currYear-$monthStr-$dayStr.csv.gz"
                    echo "Getting dates $fileName"
                    rclone copyto s3polygon:flatfiles/us_stocks_sip/day_aggs_v1/$currYear/$monthStr/$fileName $yearDir --progress --ignore-existing -vv || echo $fileName not found
                done
            fi

            currMonth=$((currMonth + 1))
        done
    fi

    currYear=$((currYear + 1))
done
