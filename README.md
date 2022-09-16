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

	* Run Katydid:
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

	* Post Processing: 
		* Overview: This is a three stage process. Run each stage without changing anything but the -stage argument.
			* For each of the steps, begin by navigating to our groups directory on eliza4: 
				* Log on to rocks. 
				* `cd /data/eliza4/he6_cres`
		* Stage 0: Set-up.  
			* `./rocks_analysis_pipeline/qsub_post_processing.py -rids 440 439 438 -aid 15 -name "rocks_demo" -nft 2 -nfe 3 -stage 0`
				* The above will first build the saved_experiment directory and then collect all of the `root_files.csv` files in the given list of run_ids and gather them into one csv that will be written into the saved_experiment directory ([name]_aid_[aid]). 
				* Before moving on to stage 1, check to see that the directory was made and the `root_files.csv` is present. 
		* Stage 1: Processing.  
			* `./rocks_analysis_pipeline/qsub_post_processing.py -rids 440 439 438 -aid 15 -name "rocks_demo" -nft 2 -nfe 3 -stage 1`	
				* This is the meat and potatoes of the post processing. nft files worth of tracks for each run_id, and nfe files worth of events for each run_id are written to disk as csvs. In order to allow for this to be done in parallel, each node is handed one file_id and processes all of the files with that file_id across all run_ids. Two files (tracks_[fid].csv, events_[fid].csv) are built for each fid. 
				* Before moving on to stage 2, check to see that the directory contains nft tracks and nfe events csvs. 
		* Stage 2: Clean-up. 
			* `./rocks_analysis_pipeline/qsub_post_processing.py -rids 440 439 438 -aid 15 -name "rocks_demo" -nft 2 -nfe 3 -stage 2`
				* The above will gather all of the events and tracks csvs (respectively) into one csv. 

	* Investigate Results:
		* Grab the saved experiment and investigate the quality of the analysis. 
			* This is to be done locally. 
			* `class ExperimentResults`
				* For now I will just have a posted demo of how to use this. But proper documentation to come soon.  

## Useful stuff: 
* `qrsh` will open a terminal in a new node (you are on the head/login node by default). Use this to test any computationally intensive processes. 
* `qstat` to see all of the jobs you have running or in the queue. 
* `qdel -u drewbyron` (delete all the jobs of user drewbyron)
* To look at the description of command line arguments for a given .py file use: 
	* `my_file.py -h`


## TODOs: 

* general: 
	* put start and stop print statements for each job that gets written out to a log file. 
	* Get sphynx documnetation going. 
* qsub_katydid.py
	* Working well as far as I can tell. 
	* The get_env_data() method doesn't work rn. This needs to retrieve the nmr/rate for each second of data. 
	* Change the job_logs dir to be under  `job_logs/katydid`. 
* post_processing.py
	* There is an issue writing the tracks_df to disk right now. I think it has to do with how I'm dealing with files with no identified tracks. Need to debug this. 
	* Once this works ok I will need to get it split up into different chunks and then have a recombining script. This would be a lot for one machine to do for 5000 files. 
	* Figure out how to grab the root files and put them in the local directory then put them into a sparse spec. 
	* is nft/nfe even doing anything here anymore?? 
	* The job name is being overwritten
* class ExperimentResults
	* One function that takes flags for events, tracks, sparse spec and outputs one plot.


