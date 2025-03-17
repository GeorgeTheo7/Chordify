#!/bin/bash
# Run each command in a new terminal tab with a 3-second delay

#gnome-terminal --tab -- bash -c "python app.py --bootstrap --port 5000 -rf 1; exec bash"

ssh team_32-vm1 "cd Chordify/src ; python chordify.py ; join -b 10.0.39.177 5050" 
sleep 3
ssh team_32-vm1 "cd Chordify/src ; python chordify.py ; join"
sleep 3
ssh team_32-vm2 "cd Chordify/src ; python chordify.py ; join"
sleep 3
ssh team_32-vm2 "cd Chordify/src ; python chordify.py ; join"
sleep 3
ssh team_32-vm3 "cd Chordify/src ; python chordify.py ; join"
sleep 3
ssh team_32-vm3 "cd Chordify/src ; python chordify.py ; join"
sleep 3
ssh team_32-vm4 "cd Chordify/src ; python chordify.py ; join"
sleep 3
ssh team_32-vm4 "cd Chordify/src ; python chordify.py ; join"
sleep 3
ssh team_32-vm5 "cd Chordify/src ; python chordify.py ; join"
sleep 3
ssh team_32-vm5 "cd Chordify/src ; python chordify.py ; join"
sleep 3

# gnome-terminal --tab -- bash -c "python app.py --port 5001; exec bash"
# sleep 3

# gnome-terminal --tab -- bash -c "python app.py --port 5002 --local; exec bash"
# sleep 3

# gnome-terminal --tab -- bash -c "python app.py --port 5003 --local; exec bash"
# sleep 3

# gnome-terminal --tab -- bash -c "python app.py --port 5004 --local; exec bash"
# sleep 3

# gnome-terminal --tab -- bash -c "python app.py --port 5005 --local; exec bash"
# sleep 3

# gnome-terminal --tab -- bash -c "python app.py --port 5006 --local; exec bash"
# sleep 3

# gnome-terminal --tab -- bash -c "python app.py --port 5007 --local; exec bash"
# sleep 3

# gnome-terminal --tab -- bash -c "python app.py --port 5008 --local; exec bash"
# sleep 3

# gnome-terminal --tab -- bash -c "python app.py --port 5009 --local; exec bash"
# sleep 3

echo "All commands finished!"

### local
