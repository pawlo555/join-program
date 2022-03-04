# join-program
Program that perform a specified join operation on two csv files

Run: `join first.csv second.csv on [mode]`

All files can be much bigger than operating memory. Alsa result can be larger than memory.

Solution is based on pandas library and used merge function.
Data are read in chunks. To display an inner join result 
I joined each chunk from first file with each chunk from second file.

To calculate left/right join I keep loaded chunk from first file and create a copy of it
to keep data that are now in chunks from second file.


Future to do:
- change output to be saved in file (then loaded read part by part then displayed and removed)