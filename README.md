
<p align="center"><img width="30%" src="/demo/readme_imgs/he6-cres_logo.png" /></p>

--------------------------------------------------------------------------------
# rocks_analysis_pipeline

This repo contains scripts for running katydid, a C++ based analysis tool adapted from Project 8 that extracts physically relevant features from spectrograms, on the CENPA cluster (rocks) and then conducts the post processing of these tracks and events.

--------------------------------------------------------------------------------
### Run an analysis then make interactive plots of cres track features!

#### Look at track and event classification overlaid on top of raw data: 

<p align="center"><img width="42%" src="/demo/readme_imgs/sparse_spec.png" />              <img width="42%" src="/demo/readme_imgs/track_overlay.png" /><img width="80%" src="/demo/readme_imgs/event_overlay.png" /></p>

#### Inertactively look at relationships between extracted cres event features: 

<p align="center"><img width="19%" src="/demo/readme_imgs/display_options_1.png" />              <img width="73%" src="/demo/readme_imgs/scatter_plot_0.png" /></p>

<p align="center"><img width="19%" src="/demo/readme_imgs/display_options_2.png" />              <img width="73%" src="/demo/readme_imgs/scatter_plot_1.png" /></p>

<p align="center"><img width="30%" src="/demo/readme_imgs/display_options_3.png" />              <img width="60%" src="/demo/readme_imgs/scatter_plot_2.png" /></p>

--------------------------------------------------------------------------------

## Instructions for running an analysis on rocks: 


### Get set up on rocks: 

* Log on to rocks. You are in your root directory which contains your .bash_profile  .bashrc
* Add the `module load python-3.7.3` to your enviornment setup file or .bash_profile file so that you have access to python3. The above must be done by each user, as it's the current users python packages that the scripts below will be utilizing. 
	* Example: `$ nano .bash_profile`
	Add `module load python-3.7.3` to the end of the file. Write and exit.
* Restart your session.
* Now you need to install your dependancies. Doing this installs them for the python-3.7.3 module`
	* $ `pip3 install -r /data/eliza4/he6_cres/rocks_analysis_pipeline/requirements.txt --user
	* Note: May need to upgrade pip. For Winston and Drew this worked: `pip3 install --upgrade pip`
* Parts of the analysis (`run_katydid.py`) are run within a singularity image. There aren't modules on the image (it can't load `module python 3.7.3` for example) and so the default python version is used as this was what was installed on the image. Each user must have these packages (in python version 3.8 but might be different for future users) available for the image.
	* $ cd /data/eliza4/he6_cres
	* $`singularity shell --bind /data/eliza4/he6_cres/ /data/eliza4/he6_cres/containers/he6cres-katydid-base.sif`
	* Singularity> `pip3 install -r rocks_analysis_pipeline/requirements.txt --user`
	* Singularity> `exit` 
* Notes: 
	* The requirements.txt should contain all necessary python packages but if that isn't the case please let me (drew) know.  

### Run katydid:

* **Overview:** Run katydid on a list of run_ids.
* **Step 0:** Run katydid for the first time on a list of run_ids: 
	* Log on to rocks. 
	* `cd /data/eliza4/he6_cres`
	* `./rocks_analysis_pipeline/qsub_katydid.py -rids 373 380 385 393 399 405 411 418 424 430 436 -nid 436 -b "2-12_dbscan_high_energy.yaml" -fn 3`
		* The above will run at most fn files for each run_id listed using the base config file provided. 
		* For reference the above jobs (one job per run_id) were mostly finished in 30 mins. 
		* A analysis_id (aid) will be assigned to the analysis. Example: aid = 9.
		* A job log for each run_id will be created. Example: rid_0440_009.txt

* **Step 1:** Clean up. Let the above run (perhaps overnight) and then run the following clean-up script. Say the analysis_id assigned to the above katydid run was 009, then you will do the following to clean up that run. The same log files as above will be written to. Best to run the below twice if doing an analysis that has many many run_ids/spec files (greater than 500 files or so).
	* Log on to rocks. 
	* `cd /data/eliza4/he6_cres`
	* `./rocks_analysis_pipeline/qsub_katydid.py -rids 373 380 385 393 399 405 411 418 424 430 436 -nid 436 -b "2-12_dbscan_high_energy.yaml" -aid 9`
		* The above will rerun all of the files in analysis_id 9 that haven't yet been created. 

### Post Processing:

* **Overview:** This is a three stage process. Run each stage without changing anything but the -stage argument.
	* For each of the steps, begin by navigating to our groups directory on eliza4: 
		* Log on to rocks. 
		* `cd /data/eliza4/he6_cres`
* **Stage 0:** Set-up.  
	* `./rocks_analysis_pipeline/qsub_post_processing.py -rids 373 380 385 393 399 405 411 418 424 430 436 -aid 9 -name "rocks_demo" -nft 2 -nfe 3 -stage 0`
		* The above will first build the saved_experiment directory and then collect all of the `root_files.csv` files in the given list of run_ids and gather them into one csv that will be written into the saved_experiment directory ([name]_aid_[aid]). 
		* Before moving on to stage 1, check to see that the directory was made and the `root_files.csv` is present. 
* **Stage 1:** Processing.  
	* `./rocks_analysis_pipeline/qsub_post_processing.py -rids 373 380 385 393 399 405 411 418 424 430 436 -aid 9 -name "rocks_demo" -nft 2 -nfe 3 -stage 1`	
		* This is the meat and potatoes of the post processing. nft files worth of tracks for each run_id, and nfe files worth of events for each run_id are written to disk as csvs. In order to allow for this to be done in parallel, each node is handed one file_id and processes all of the files with that file_id across all run_ids. Two files (tracks_[fid].csv, events_[fid].csv) are built for each fid. 
		* Before moving on to stage 2, check to see that the directory contains nft tracks and nfe events csvs. 
* **Stage 2:** Clean-up. 
	* `./rocks_analysis_pipeline/qsub_post_processing.py -rids 373 380 385 393 399 405 411 418 424 430 436 -aid 9 -name "rocks_demo" -nft 2 -nfe 3 -stage 2`
		* The above will gather all of the events and tracks csvs (respectively) into one csv. 



### Tools to investigate event classification quality and to conduct analysis. 

* **Investigate results:**
	* Grab the saved experiment and investigate the quality of the analysis. 
	* This is to be done locally using `class ExperimentResults` in module `results.py`.
	* A full demo of how this is done is here: `/rocks_analysis_pipeline/demo/rocks_analysis_demo.ipynb`. 
	* Copy the `.ipynb` into your own directory suitable for analysis and give it a try. Make neat plots like the one you see in the top of this readme. 


--------------------------------------------------------------------------------

## Useful stuff: 

* **SGE:**
	* `qrsh` will open a terminal in a new node (you are on the head/login node by default). Use this to test any computationally intensive processes. 
	* `qstat` to see all of the jobs you have running or in the queue. 
	* `qdel -u drewbyron` (delete all the jobs of user drewbyron)
	* To look at the description of command line arguments for a given .py file use: 
		* `my_file.py -h`
* **Permissions:**
	* I'm finding that with multiple users working in this analysis pipeline simultaneously the permissions can get weird. The following two commands run from `/he6_cres` should help 

--------------------------------------------------------------------------------

## Testing: 

* 11/18/22: Getting back into this and finishing up the documentation. Testing to see how things are working as of 11/18/22. I had to uninstall he6cresspec sims. Ran the following: 
	* ./rocks_analysis_pipeline/qsub_katydid.py -rids 393 424 430 436 -nid 436 -b "2-12_dbscan_high_energy.yaml" -fn 2
	* ./rocks_analysis_pipeline/qsub_katydid.py -rids 393 424 430 436 -nid 436 -b "2-12_dbscan_high_energy.yaml" -aid 2
	* ./rocks_analysis_pipeline/qsub_post_processing.py -rids 393 424 430 436 -aid 2 -name "test_11182022" -nft 2 -nfe 2 -stage 0
	* ./rocks_analysis_pipeline/qsub_post_processing.py -rids 393 424 430 436 -aid 2 -name "test_11182022" -nft 2 -nfe 2 -stage 1
	* **Summary:** Things are working well. I uninstalled he6-cres-spec-sims and instead just pointed to the local directory on rocks. So these two repos are intertwined now. 


## TODOs + Improvements to Make: 

* **General**
	* Make sure that the files with no tracks are still getting kept track of somehow. Maybe just in the file df? How is this being dealt with at the moment? Need some way to keep track of the total number of files at each field.
	* Add docstrings for each module and class. 
	* Make some progress on documenting what is actually done at each stage and how things are passed around. This will save me (and others) a ton of headaches. 
	* It would be nice for the root files df to contain a col for if this file is included in the tracks or events df. Right now it's a bit hard to tell which is a problem. 
	* Helper function for viewing the noise spectrum from a root file in the results class. 
	* Add RGA data into root files table. 
* **run_katydid.py**
	* The time that is printed to the log for how long katydid took on one file doesn't align with how long the jobs take to run? Why is this?
* **run_post_processing.py**
	* Indexing of the root_files df is still off. 
	* Fix indexing of events and tracks df. 
	* Why are there these random (clearly unphysical) events that cross the whole second of data? Need a cut to deal with this.  
	* Are the files with no tracks or events being dealt with intelligently?
* **results.py**
	* Check to make sure this works on other people's machines. 


## Log of changes: 

* 12/1/22: 
	* Adding option for self noise floot with nid = -1. 
	* Also building out the event properties. 
	* Testing changes with: 
		* `./rocks_analysis_pipeline/qsub_katydid.py -rids 393 -nid 393 -b "2-12_dbscan_high_energy.yaml" -fn 2`
