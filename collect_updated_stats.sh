#!/bin/bash
trap "exit" INT


mkdir -p stats_updated/$1
network_file=stats_updated/$1/network
memory_file=stats_updated/$1/memory
disk_file=stats_updated/$1/disk
cpu_file=stats_updated/$1/cpu

function network() {
	sar -n DEV 1 1 | egrep -v " lo|IFACE|Linux"  | sed  '/^$/d' >> $network_file
}

function memory() {
	echo $(date "+%s") mem usage: $(free -mh | grep Mem | tr -s " " | cut -d " " -f3) >> $memory_file
}

function disk() {
	sar -d 1 1  | egrep -v "DEV|Linux" | sed '/^$/d' >> $disk_file
}

function cpu() {
	sar -u 1 1 | egrep -v "Linux|CPU" | sed '/^$/d' >> $cpu_file
}

function _do() {
	echo $(date) collecting stats... >&2
	while :; do
		network
		memory
		disk
		cpu
		sleep $1
	done
}


_do $2
