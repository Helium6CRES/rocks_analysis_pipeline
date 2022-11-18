
<p align="center"><img width="30%" src="/demo/readme_imgs/he6-cres_logo.png" /></p>

--------------------------------------------------------------------------------
# rocks_analysis_pipeline

This repo contains scripts for running katydid, a C++ based analysis tool adapted from Project 8 that extracts physically relevant features from spectrograms, on the CENPA cluster (rocks) and then conducting the post processing of these tracks and events.

--------------------------------------------------------------------------------
### Run an analysis then make interactive plots of cres track features!

<p align="center"><img width="40%" src="/demo/readme_imgs/plot_stuff.png" />              <img width="50%" src="/demo/readme_imgs/s.png" /></p>

<p align="center"><img width="40%" src="/demo/readme_imgs/plot_stuff_1.png" />              <img width="50%" src="/demo/readme_imgs/s.png" /></p>

--------------------------------------------------------------------------------

## Instructions for running an analysis on rocks: 


* **Get set up:** 
	* Log on to rocks. 
	* `cd /data/eliza4/he6_cres`
	* Note: May need to upgrade pip. 
		* For Winston and I this worked: `pip3 install --upgrade pip`
		* For Heather the above didn't work and she needed to do the following: 
	* `pip3 install -r rocks_analysis_pipeline/requirements.txt`
	* Notes: 
		* The following should contain all necessary python packages but if that isn't the case please let me (drew) know. 
		* Be sure to add the `module load python-3.7.3` to your enviornment setup file or .bash_profile file so that you have access to python3.
		* The above must be done by each user, as it's the current users python packages that the scripts below will be utilizing.  

* **Run katydid:**
	* Overview: Run katydid on a list of run_ids.
	* Step 0. Run katydid for the first time on a list of run_ids: 
		* Log on to rocks. 
		* `cd /data/eliza4/he6_cres`
		* `./rocks_analysis_pipeline/qsub_katydid.py -rids 373 380 385 393 399 405 411 418 424 430 436 -nid 436 -b "2-12_dbscan_high_energy.yaml" -fn 3`
			* The above will run at most fn files for each run_id listed using the base config file provided. 
			* For reference the above jobs (one job per run_id) were mostly finished in 30 mins. 
			* A analysis_id (aid) will be assigned to the analysis. Example: aid = 9.
			* A job log for each run_id will be created. Example: rid_0440_009.txt

	* Step 1. Clean up. Let the above run (perhaps overnight) and then run the following clean-up script. Say the analysis_id assigned to the above katydid run was 009, then you will do the following to clean up that run. The same log files as above will be written to. Best to run the below twice if doing an analysis that has many many run_ids/spec files (greater than 500 files or so).
		* Log on to rocks. 
		* `cd /data/eliza4/he6_cres`
		* `./rocks_analysis_pipeline/qsub_katydid.py -rids 373 380 385 393 399 405 411 418 424 430 436 -nid 436 -b "2-12_dbscan_high_energy.yaml" -aid 9`
			* The above will rerun all of the files in analysis_id 9 that haven't yet been created. 

* **Post Processing:**
	* Overview: This is a three stage process. Run each stage without changing anything but the -stage argument.
		* For each of the steps, begin by navigating to our groups directory on eliza4: 
			* Log on to rocks. 
			* `cd /data/eliza4/he6_cres`
	* Stage 0: Set-up.  
		* `./rocks_analysis_pipeline/qsub_post_processing.py -rids 373 380 385 393 399 405 411 418 424 430 436 -aid 9 -name "rocks_demo" -nft 2 -nfe 3 -stage 0`
			* The above will first build the saved_experiment directory and then collect all of the `root_files.csv` files in the given list of run_ids and gather them into one csv that will be written into the saved_experiment directory ([name]_aid_[aid]). 
			* Before moving on to stage 1, check to see that the directory was made and the `root_files.csv` is present. 
	* Stage 1: Processing.  
		* `./rocks_analysis_pipeline/qsub_post_processing.py -rids 373 380 385 393 399 405 411 418 424 430 436 -aid 9 -name "rocks_demo" -nft 2 -nfe 3 -stage 1`	
			* This is the meat and potatoes of the post processing. nft files worth of tracks for each run_id, and nfe files worth of events for each run_id are written to disk as csvs. In order to allow for this to be done in parallel, each node is handed one file_id and processes all of the files with that file_id across all run_ids. Two files (tracks_[fid].csv, events_[fid].csv) are built for each fid. 
			* Before moving on to stage 2, check to see that the directory contains nft tracks and nfe events csvs. 
	* Stage 2: Clean-up. 
		* `./rocks_analysis_pipeline/qsub_post_processing.py -rids 373 380 385 393 399 405 411 418 424 430 436 -aid 9 -name "rocks_demo" -nft 2 -nfe 3 -stage 2`
			* The above will gather all of the events and tracks csvs (respectively) into one csv. 

* **Investigate results:**
	* Grab the saved experiment and investigate the quality of the analysis. 
		* This is to be done locally. 
		* `class ExperimentResults`
			* For now I will just have a posted demo of how to use this. But proper documentation to come soon.  

## Useful stuff: 

* **SGE:**
	* `qrsh` will open a terminal in a new node (you are on the head/login node by default). Use this to test any computationally intensive processes. 
	* `qstat` to see all of the jobs you have running or in the queue. 
	* `qdel -u drewbyron` (delete all the jobs of user drewbyron)
	* To look at the description of command line arguments for a given .py file use: 
		* `my_file.py -h`


## TODOs: 

* general: 
	* put start and stop print statements for each job that gets written out to a log file. 
	* Get sphynx documnetation going. Make it pretty. Use this resource to get started: https://keep.google.com/u/0/#NOTE/1QBoOz6T-t5-MZk35VD79ZzVyOxJDCackSwkSVI6s3UmufJrvEmYfLYvjO9rP
	* Delete all unused or commented out code and .py files. 
	* Make sure permissions things are ok for other people...
	* Add a datetime timestamp to all of the writes to the logs. This will be very useful for debugging. 
	* Print statements are all over the place. Make it so that the logs are readable (once things work well). 
	* Delete all commented code that isn't being used. 
	* Document and make a nice readme with some gifs illustrating what this does. 
	* Make sure that the files with no tracks are still getting kept track of somehow. Maybe just in the file df? How is this being dealt with at the moment? Need some way to keep track of the total number of files at each field... 
	* add utility functions module with get time now, database query,...
	* Make a new demo ipynb, suggest a workflow (ipynb and saved dirs together on a harddrive)
	* Cleam up the demo nb and add instructions on how to use it (copy it and move to different directory). 
	* Make some progress on documenting what is actually done at each stage and how things are passed around. This will save me (and others) a ton of headaches. 
	* It would be nice for the root files df to contain a col for if this file is included in the tracks or events df. Right now it's a bit hard to tell which is a problem. 
	* Helper function for viewing the noise spectrum from a root file in the results class. 
	* Add RGA data into root files table. 
* run_katydid.py
	* The get_env_data() method doesn't work rn. This needs to retrieve the nmr/rate for each second of data.  
	* Change the job_logs dir to be under  `job_logs/katydid`. 
	* The time that is printed to the log for how long katydid took on one file doesn't align with how long the jobs take to run? Why is this?
	* I don't like the way this is organized rn. I would be nice to build it into a class the way the post processing is. 
	* The copies of the .yaml isn't getting deleted rn. FIXED (I think, need to verify)
* run_post_processing.py
	* Indexing of the root_files df is still off. 
	* Fix indexing of events and tracks df. 
	* Why are there these random (clearly unphysical) events that cross the whole second of data? Need a cut to deal with this.  
	* Add the clean-up settings to the list of arguments?
	* Are the files with no tracks or events being dealt with intelligently?
	* Check to see if an experiment with this name exists.
	* Organize all of the files in the class so that it's clear which ones are used where. Organize by stage. 
* results.py
	* Check to make sure this works on other people's machines. 
	* Add the visualization tool into the class that shows the different relationships between variables (in demo nb rn.)


## Testing this on 100 files: 

* All but one file worked the first time through katydid, the clean-up didn't help. The file must have a weirdness
	* Weird file: Freq_data_2022-08-18-06-29-23_010.root
* Now running the post processing. This meant submitting 100 jobs. 