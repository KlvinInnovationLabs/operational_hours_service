# ON-OFF HOURS 

This repo takes in code from Hemanth's on off hours code, runs on a schedule and inserts into the db. Also comes with a script to run once and process backlog of data.

Inputs 
1. db configs
2. thresholds object

Outputs
1. Inserts on hours per day into the db, updates the record if seen in the db for the device else creates one record that stores on hours of motors today.