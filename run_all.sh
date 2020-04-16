#! /bin/bash

for INPUT in input forestier_mayotte.txt deontologie_police_nationale.txt domaine_public_fluvial.txt
do
    for NB in 2 3 4 5 6 7 8 9 10
    do
        echo Running on file $INPUT on $NB machines
        ant run -Dinput=$INPUT -DnbMachines=$NB
    done
done