#!/bin/bash

cd /home/pi/Python
sed -i "s/\r//"  *.py

if [ $# = 1 ];
	then ./main.py $1
else
	./main.py
fi
