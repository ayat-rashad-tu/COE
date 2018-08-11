# Check the tasks log files if there were any errors in completing the tasks.
export job_id=$1
export attempt_id=$2

if [ ! -z $3 ] 
then
	export n=$3 
else
	export n=30
fi

# JobTracker logs
printf "Jobtracker Log:\n"
tail -n $n /usr/local/hadoop/logs/hadoop-hduser-jobtracker-ip-172-31-54-48.log

ls -al /usr/local/hadoop/logs/userlogs/${job_id}

# Attempts logs
printf "\n\n\n"
printf "Attempt Log STDERR: \n"
tail -n ${n} /usr/local/hadoop/logs/userlogs/${job_id}/${attempt_id}/stderr

printf "\n\n\n"
printf "Attempt Log SYSLOG: \n"
tail -n ${n} /usr/local/hadoop/logs/userlogs/${job_id}/${attempt_id}/syslog
