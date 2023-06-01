
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
* Now you need to install your dependancies. Doing this installs them for the python-3.7.3 module
	* $ `pip3 install -r /data/eliza4/he6_cres/rocks_analysis_pipeline/requirements.txt --user`
	* Note: May need to upgrade pip. For Winston and Drew this worked: `pip3 install --upgrade pip`
* Parts of the analysis (`run_katydid.py`) are run within a singularity image. There aren't modules on the image (it can't load `module python 3.7.3` for example) and so the default python version is used as this was what was installed on the image. Each user must have these packages (in python version 3.8 but might be different for future users) available for the image.
	* $ `cd /data/eliza4/he6_cres`
	* $`singularity shell --bind /data/eliza4/he6_cres/ /data/eliza4/he6_cres/containers/he6cres-katydid-base.sif`
	* Singularity> `pip3 install -r rocks_analysis_pipeline/requirements.txt --user`
	* Singularity> `exit` 
* Notes: 
	* The requirements.txt should contain all necessary python packages but if that isn't the case please let me (drew) know. 
	
### Notes about the singularity shell post-02/07/23 meeting with Clint
We found an issue with psycopg2 (a postgress library) for Heather. The way it is currently set up is that a submission script run on the head node acesses the postgress database and uses the information about the field to build the config files for the requested run_ids. The jobs are then sent to compute nodes. The first thing the compute nodes do is load the singularity image which can be shared between users to essentially create a uniform virtual environment. Katydid runs. Then to do the post-processing, a submission script is run on the head node which then submits a bunch of jobs to the compute nodes. Right now, these do NOT load the singularity image. The logic was that, if the user had run requirements.txt in their head node, then the compute nodes would be able to use all those loaded modules. We found that this is not always true if there is som library involved that you would locally need to instal with apt get. As a result for Heather, eventhough she had installed psycopg2 for her user on the head node, when the post-processing jobs on the compute nodes tried to use psycopg2 they threw and error that they couldn't find a library. The solution, according to Clint, is to also load the singularity image on the compute nodes before each post-processing job is run on one of those nodes. 

Our norm should be that the only thing you run on the head node are submission scripts. You cannot submit jobs from within a singularity image, so these have to be in the user's environment. However all the heavy lisfting is done withing jobs on the compute nodes. These should ALWAYS load the singularity image so that they are run in a consistant environment across users and nodes. Drew is going to work on implementing this.

If you want to test something in the same environment as it will be run with when it is submitted to a compute nose, first run 
	'singularity shell --bind /data/eliza4/he6_cres/ /data/eliza4/he6_cres/containers/he6cres-katydid-base.sif'
to enter an interactive singularity shell and then do your tests there.

	
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
	* `./rocks_analysis_pipeline/qsub_katydid.py -rids 373 380 385 393 399 405 411 418 424 430 436 -nid 436 -b "2-12_dbscan_high_energy.yaml" -fn 3 -aid 9`
		* The above will rerun all of the files in analysis_id 9 that haven't yet been created.
		* Note that you want to include "-fn 3" here in case a node failed before even creating the  

### Post Processing:

* **Overview:** This is a three stage process. Run each stage without changing anything but the -stage argument.
	* For each of the steps, begin by navigating to our groups directory on eliza4: 
		* Log on to rocks. 
		* `cd /data/eliza4/he6_cres`
* **Stage 0:** Set-up.  
	* `./rocks_analysis_pipeline/qsub_post_processing.py -rids 373 380 385 393 399 405 411 418 424 430 436 -aid 9 -name "rocks_demo" -nft 2 -nfe 3 -dbscan 1 -stage 0`
		* The above will first build the saved_experiment directory and then collect all of the `root_files.csv` files in the given list of run_ids and gather them into one csv that will be written into the saved_experiment directory ([name]_aid_[aid]). 
		* Before moving on to stage 1, check to see that the directory was made and the `root_files.csv` is present. 
* **Stage 1:** Processing.  
	* `./rocks_analysis_pipeline/qsub_post_processing.py -rids 373 380 385 393 399 405 411 418 424 430 436 -aid 9 -name "rocks_demo" -nft 2 -nfe 3 -dbscan 1 -stage 1`	
		* This is the meat and potatoes of the post processing. nft files worth of tracks for each run_id, and nfe files worth of events for each run_id are written to disk as csvs. In order to allow for this to be done in parallel, each node is handed one file_id and processes all of the files with that file_id across all run_ids. Two files (tracks_[fid].csv, events_[fid].csv) are built for each fid. 
		* Before moving on to stage 2, check to see that the directory contains nft tracks and nfe events csvs. 
		* If for some reason (most likely failed nodes) all of the events_{n}.csv's aren't created rerun the exact same command. It will detect the missing ones and rerun those. 
		* `-dbscan` flag: Flag to run the default dbscan colinear event clustering (1) or not (0). Note that right now there are only default EventTimeIntc eps values (found by Heather via histogramming event time intercepts) for .75 - 3.25 in .25 T steps. This needs to be generalized at some point. 
* **Stage 2:** Clean-up. 
	* `./rocks_analysis_pipeline/qsub_post_processing.py -rids 373 380 385 393 399 405 411 418 424 430 436 -aid 9 -name "rocks_demo" -nft 2 -nfe 3 -dbscan 1 -stage 2`
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
	* Use `qstat | wc -l` to count the number of jobs you have open/active. 
	* To delete jobs of state `Eqw` or any state, add the following alias to your .bashrc: 
		* `alias killEqw="qstat | grep drewbyron | grep 'Eqw' | cut -d ' ' -f1 | xargs qdel"`
		* Change your username and f1 means first column contains the job id, if otherwise then change to -fx for xth column. 

* **Permissions:**
	* I'm finding that with multiple users working in this analysis pipeline simultaneously the permissions can get weird. The following two commands run from `/he6_cres` should help: 
		* `chmod -R 774 katydid_analysis/`
		* `chgrp -R he6_cres katydid_analysis/`

* **Singularity Container:**
	* To interactively enter the analysis singularity container run the following: 
		* `singularity shell --bind /data/eliza4/he6_cres/ /data/eliza4/he6_cres/containers/he6cres-katydid-base.sif`
	* To exit the container: 
		* `exit`
		
* **BASH:**
	* Pretty print a csv in bash (useful for sanity checking):
		* `column -s, -t < root_files.csv | less -#2 -N -S`
		* `.q` to exit. 
	* Check number of rows in a csv (useful for checking len of df/csv): 
		* `column -s, -t < events.csv | less -#2 -N -S | wc -l`
	* Count number of files in a directory: 
		* `ls -1 | wc -l`

* **Rocks:**
	* To check on rocks use this site (won't work when on the cenpa VPN): 
		* `http://cenpa-rocks.npl.washington.edu/ganglia/?r=hour&cs=&ce=&m=load_one&s=by+name&c=&tab=m&vn=&hide-hf=false`
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
	* Add in a summary function where you just list the run_ids and aid and it prints to screen the summary of how many root files there are and such...
	* I think the set_permissions() method of rocks_utility is used too frequently. It may take a long time so may be slowing things down. 
	* Need to work on protecting the permissions of the data files. Not sure how exactly to do this but this is important. 
* **run_katydid.py**
	* The time that is printed to the log for how long katydid took on one file doesn't align with how long the jobs take to run? Why is this?
	* Make sure a representative .yaml is being written to the aid_xxx directory not just the generic unedited one. 
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
		`./rocks_analysis_pipeline/qsub_katydid.py -rids 393 -nid -1 -b "2-12_dbscan_high_energy.yaml" -fn 2`
* 12/2/22: 
	* The above worked and now I'm moving on to a sanity check with the entire Ne dataset: 
		* `./rocks_analysis_pipeline/qsub_katydid.py -rids 561 560 559 558 557 555 554 553 552 551 549 548 546 545 544 543 542 540 539 538 537 536 534 533 532 531 530 528 527 526 525 524 522 521 520 519 518 516 515 514 513 512 510 509 508 507 506 504 503 502 501 500 496 495 494 493 492 -nid -1 -b "2-12_dbscan_high_energy.yaml" -fn 2`
		* `./rocks_analysis_pipeline/qsub_katydid.py -rids 561 560 559 558 557 555 554 553 552 551 549 548 546 545 544 543 542 540 539 538 537 536 534 533 532 531 530 528 527 526 525 524 522 521 520 519 518 516 515 514 513 512 510 509 508 507 506 504 503 502 501 500 496 495 494 493 492 -nid -1 -b "2-12_dbscan_high_energy.yaml" -aid 4`
		* `./rocks_analysis_pipeline/qsub_post_processing.py -rids 561 560 559 558 557 555 554 553 552 551 549 548 546 545 544 543 542 540 539 538 537 536 534 533 532 531 530 528 527 526 525 524 522 521 520 519 518 516 515 514 513 512 510 509 508 507 506 504 503 502 501 500 496 495 494 493 492 -aid 4 -name "new_event_features_test" -nft 2 -nfe 2 -stage 0`
			* The above is failing because of the permissions issues. ehh. 
			* 1435: Ok actually it failed because katydid didn't seem to run... 

		* Ok now I've come to understand that katydid is working it was just the noise file being -1 that was causing issues. Not sure how exactly. Need to try that again. 
		* Ok it's hacky but the chmod is working now with suppressed output. 
		* The self noise file still doesn't work. Would like to get that working. 
* 12/09/22:	Running the following to get the noise floor for each run_id for ne: 12/09/22 
	* `./rocks_analysis_pipeline/qsub_katydid.py -rids 561 560 559 558 557 555 554 553 552 551 549 548 546 545 544 543 542 540 539 538 537 536 534 533 532 531 530 528 527 526 525 524 522 521 520 519 518 516 515 514 513 512 510 509 508 507 506 504 503 502 501 500 496 495 494 493 492 -nid -1 -b "2-12_dbscan_high_energy.yaml" -fn 2`
	* `./rocks_analysis_pipeline/qsub_katydid.py -rids 561 560 559 558 557 555 554 553 552 551 549 548 546 545 544 543 542 540 539 538 537 536 534 533 532 531 530 528 527 526 525 524 522 521 520 519 518 516 515 514 513 512 510 509 508 507 506 504 503 502 501 500 496 495 494 493 492 -nid -1 -b "2-12_dbscan_high_energy.yaml" -aid 7`
	* `./rocks_analysis_pipeline/qsub_post_processing.py -rids 561 560 559 558 557 555 554 553 552 551 549 548 546 545 544 543 542 540 539 538 537 536 534 533 532 531 530 528 527 526 525 524 522 521 520 519 518 516 515 514 513 512 510 509 508 507 506 504 503 502 501 500 496 495 494 493 492 -aid 7 -name "new_event_features_test" -nft 2 -nfe 2 -stage 0`
	* `./rocks_analysis_pipeline/qsub_post_processing.py -rids 561 560 559 558 557 555 554 553 552 551 549 548 546 545 544 543 542 540 539 538 537 536 534 533 532 531 530 528 527 526 525 524 522 521 520 519 518 516 515 514 513 512 510 509 508 507 506 504 503 502 501 500 496 495 494 493 492 -aid 7 -name "new_event_features_test" -nft 2 -nfe 2 -stage 1`
	* Notes for next time: 
		* stage 1 is failing because of the order of the event features being added to the tracks. Push those changes to the remote and then pull on rocks and make it work. 
		* They try stage 2. 
		* Then pull locally and make sure it's working ok. 
		* Then make a plot of the noise floors over time. Make a function for doing this so you can do it for He as well. 
		* Then think about submitting a new analysis for the full He and Ne sets (see the new submissions in the google doc). Get this banged out. 
* 12/20/22:
	* Trying to get back into this and make it work!
	* Task 1: Get the selfing noise file to work and make a plot for the noise file over time for both the ne and he data. 
		* Running the following to that end: 
			* `./rocks_analysis_pipeline/qsub_post_processing.py -rids 561 560 559 558 557 555 554 553 552 551 549 548 546 545 544 543 542 540 539 538 537 536 534 533 532 531 530 528 527 526 525 524 522 521 520 519 518 516 515 514 513 512 510 509 508 507 506 504 503 502 501 500 496 495 494 493 492 -aid 7 -name "new_event_features_test" -nft 2 -nfe 2 -stage 0`
			* Stage 1 of the above fails with this: 
				* `Index(['EventTimeIntc'], dtype='object')] are in the [columns]`
			* Ok this seems to be working now. Need to make sure the visualizations still work!
	* For tomorrow: 
		* Get the noise floors plots and post!
		* Use this file to do it: `rocks_analysis_notebooks/plotting_scripts/noise_floors_over_time.py`
		* Then on to Task 2: getting the analysis working for 8,9,10 SNR cuts. 
* 12/21/22: 
	* The above worked, and I was able to make the noise files for Neon over the entire course of the data taking. Now working on doing the same for Helium. 
	* Running things through rocks: 
		* `./rocks_analysis_pipeline/qsub_katydid.py -rids 440 439 438 437 436 434 433 432 431 430 428 427 426 425 424 422 421 420 419 418 416 415 414 413 412 411 409 408 407 406 405 403 402 401 400 399 397 396 395 394 393 391 390 389 388 387 384 383 382 381 380 377 376 375 374 373 -nid -1 -b "2-12_dbscan_high_energy.yaml" -fn 2`
		
		* FOR NEXT TIME RUN THE FOLLOWING: 
			* `./rocks_analysis_pipeline/qsub_katydid.py -rids 440 439 438 437 436 434 433 432 431 430 428 427 426 425 424 422 421 420 419 418 416 415 414 413 412 411 409 408 407 406 405 403 402 401 400 399 397 396 395 394 393 391 390 389 388 387 384 383 382 381 380 377 376 375 374 373 -nid -1 -b "2-12_dbscan_high_energy.yaml" -aid 10`
			* `./rocks_analysis_pipeline/qsub_post_processing.py -rids 440 439 438 437 436 434 433 432 431 430 428 427 426 425 424 422 421 420 419 418 416 415 414 413 412 411 409 408 407 406 405 403 402 401 400 399 397 396 395 394 393 391 390 389 388 387 384 383 382 381 380 377 376 375 374 373 -aid 10 -name "he_noise_floors_test" -nft 2 -nfe 2 -stage 0`
			* Then check that the above works and push through all the stages.
* 12/22/22: 
	* The Helium all went fine. But run_id 381 is messed up and this is causing issues. 
	* Also (BIG and ANNOYING) the event reconstruction seems broken now... check on the Helium and Neon datasets.  
	* Ok working on different SNR cuts. Will take notes of all the files I run here. Going to start with something managable like 10 files per run_id first, and build the machinery to compare the ratio plots and such for each. 
	* SNR tests: 
	* SNR cut_9
	* Neon: (submitted 12/22/22 1338)
		* `./rocks_analysis_pipeline/qsub_katydid.py -rids 561 560 559 558 557 555 554 553 552 551 549 548 546 545 544 543 542 540 539 538 537 536 534 533 532 531 530 528 527 526 525 524 522 521 520 519 518 516 515 514 513 512 510 509 508 507 506 504 503 502 501 500 496 495 494 493 492 -nid -1 -b "2-12_dbscan_high_energy_snr8.yaml" -fn 2`
	* Helium: (submitted 12/22/22 1338)
		* `./rocks_analysis_pipeline/qsub_katydid.py -rids 440 439 438 437 436 434 433 432 431 430 428 427 426 425 424 422 421 420 419 418 416 415 414 413 412 411 409 408 407 406 405 403 402 401 400 399 397 396 395 394 393 391 390 389 388 387 384 383 382 380 377 376 375 374 373 -nid -1 -b "2-12_dbscan_high_energy_snr8.yaml" -fn 2`
* 2/22/23: 
	* Now the post processing is all run within the singularity container as well. There was an issue with uproot vs uproot4. When I swiched to uproot everything started working fine. The requirements.txt has been updated accordingly but be sure to enter the image then pip install as is described above. 