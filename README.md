# rocks_analysis_pipeline

This repo contains scripts for running katydid on the CENPA cluster (rocks) and then conducting the post processing of these tracks and events. 

## Notes as I build this out: 

* Currently the following things work: 
	* Get set up: 
		* Log on to rocks. 
		* `cd /data/eliza4/he6_cres`
		* `pip3 install -r requirements.txt`
		* Notes: 
			* The following should contain all necessary python packages but if that isn't the case please let me (drew) know. 
			* Be sure to add the `module load python-3.7.3` to your enviornment setup file or .bash_profile file so that you have access to python3.
			* The above must be done by each user, as it's the current users python packages that the scripts below will be utilizing.  

	* Run katydid on a list of run_ids: 
		* Log on to rocks. 
		* `cd /data/eliza4/he6_cres`
		* `./rocks_analysis_pipeline/qsub_katydid.py -rids 440 439 438 -b "2-12_dbscan_high_energy.yaml" -fn 10`
			* The above will run at most fn files for each run_id listed using the base config file provided. 
			* A analysis_id (aid) will be assigned to the analysis. Example: aid = 9.
			* A job log for each run_id will be created. Example: rid_0440_009.txt

	* Clean up. Let the above run (perhaps overnight) and then run the following clean-up script. Say the analysis_id assigned to the above katydid run was 009, then you will do the following to clean up that run. The same log files as above will be written to. 
		* Log on to rocks. 
		* `cd /data/eliza4/he6_cres`
		* `./rocks_analysis_pipeline/qsub_katydid.py -rids 440 439 438 -b "2-12_dbscan_high_energy.yaml" -aid 9`
			* The above will rerun all of the files in analysis_id 9 that haven't been created. 

	* Gather the root file outputs. Collect tracks, clean and cluster events: 
		* Log on to rocks. 
		* `qrsh`
		* `cd /data/eliza4/he6_cres`
		* `./rocks_analysis_pipeline/post_processing.py -rids 440 439 438 -aid 9 -name "demo" -nft 2 -nfe 2`
			* The above will collect tracks from nft files per run_id (2 in the example above) files and clean and cluster events for nfe files per run_id (2 in the example above). These tracks and events are written to the directory `katydid_analysis/saved_experiments/demo_aid_009`.
			* For now be sure to first run qrsh before running the above as it would severely clog up the head node.

	* Grab the saved experiment and investigate the quality of the analysis. 
		* This is to be done locally. 
		* `class ExperimentResults`
			* This class will copy the saved experiment dir above (`katydid_analysis/saved_experiments/demo_aid_009`). 
			* It will also grab the root files associated with the tracks in the `tracks.csv` file so one can overlay the identified tracks and the sparse spectrogram 
			* It will enable the user to visualize the events, tracks, and sparse spectrograms all overlaid. 


## Useful stuff: 
* `qrsh` will open a terminal in a new node (you are on the head/login node by default). Use this to test any computationally intensive processes. 
* `qstat` to see all of the jobs you have running or in the queue. 
* `qdel -u drewbyron` (delete all the jobs of user drewbyron)
* To look at the description of command line arguments for a given .py file use: 
	* `my_file.py -h`


## TODOs: 

* qsub_katydid.py
	* Working well as far as I can tell. 
	* The get_env_data() method doesn't work rn. This needs to retrieve the nmr/rate for each second of data. 
* post_processing.py
	* There is an issue writing the tracks_df to disk right now. I think it has to do with how I'm dealing with files with no identified tracks. Need to debug this. 
	* Once this works ok I will need to get it split up into different chunks and then have a recombining script. This would be a lot for one machine to do for 5000 files. 
	* Figure out how to grab the root files and put them in the local directory then put them into a sparse spec. 
* class ExperimentResults
	* One function that takes flags for events, tracks, sparse spec and outputs one plot.


