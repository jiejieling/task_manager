#!/usr/bin/env bash
MYSQL="mysql -hudb1 -umonitor -pevLLJwnPGR6LE2 -N compass"
DATETIME_AT_24_HOUR_AGO=`date -d "24 hour ago" +"%Y-%m-%d %H:%M"`
THRESHOLD=10

NOW=`date +"%Y-%m-%d %H:%M"`
ALERTER=('sevenling@shijiebang.net' 'felix@shijiebang.net' 'adyliu@shijiebang.net')
ALERT_LOCK=/tmp/check_web_question_alert.lock

function sendmail()
{
    subject=$1
    content=$2

    mutt -s "${subject}" ${ALERTER[*]} <<EOF
${content}
EOF
}

function check()
{
    count=`${MYSQL} -e "SELECT count(*) FROM questions WHERE created_at >= '${DATETIME_AT_24_HOUR_AGO}'"`
    if [ $? -ne 0 ]
    then
        echo 'Exec SQL error, exit....'
        return 1
    fi

    if [ ${count} -gt ${THRESHOLD} ]
    then
        if [ ! -e ${ALERT_LOCK} ]
        then
            touch ${ALERT_LOCK}
            echo "Warning, questions count ${count} > 10/24h"
            return 2
        fi
    else
        if [ -e ${ALERT_LOCK} ]
        then
            rm -f ${ALERT_LOCK}
            echo "Info, questions count recover normal"
            return 3
        fi

    fi

    return 0
}

content=`check`
if [ $? -ne 0 ]
then
    subject="社区提问问题报警 at ${NOW}"
    sendmail "${subject}" "${content}"
fi
