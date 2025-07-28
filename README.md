
<p align="center"><img width="30%" src="/demo/readme_imgs/he6-cres_logo.png" /></p>

--------------------------------------------------------------------------------
# rocks_analysis_pipeline

This repo contains scripts for running katydid, a C++ based analysis tool adapted from Project 8 that extracts physically relevant features from spectrograms, on the CENPA cluster (WULF) and then conducts the post processing of these tracks and events.

--------------------------------------------------------------------------------
### Run an analysis then make interactive plots of cres track features!

#### Look at track and event classification overlaid on top of raw data: 

<p align="center"><img width="42%" src="/demo/readme_imgs/sparse_spec.png" />              <img width="42%" src="/demo/readme_imgs/track_overlay.png" /><img width="80%" src="/demo/readme_imgs/event_overlay.png" /></p>

#### Inertactively look at relationships between extracted cres event features: 

<p align="center"><img width="19%" src="/demo/readme_imgs/display_options_1.png" />              <img width="73%" src="/demo/readme_imgs/scatter_plot_0.png" /></p>

<p align="center"><img width="19%" src="/demo/readme_imgs/display_options_2.png" />              <img width="73%" src="/demo/readme_imgs/scatter_plot_1.png" /></p>

<p align="center"><img width="30%" src="/demo/readme_imgs/display_options_3.png" />              <img width="60%" src="/demo/readme_imgs/scatter_plot_2.png" /></p>

--------------------------------------------------------------------------------

## Instructions for running an analysis on WULF (CENPA compute cluster): 


### Get set up on WULF: 
To facilitate developers having the same environment across machines and on cluster nodes, we need to use a containerized development environemnt. This is done with apptainer. The container is built to be totally independant of previous projects, such as project8, to support ROOT, Katydid, and include all the base python dependancies used mainly for analysis on the CENPA Wulf cluster. The container is deffined via the apptainer definition file, he6cres-base.def which builds everything from ubuntu:20.04 to make a reproducible He6-CRES environment with Python 3.7.3, pip dependencies, and ROOT 6.22/06. This definition file is used to buld the container: he6cres-base.sif which can be used by anyone. \
In normal operations, developers should just use the existing .sif file as is while changing katydid or analysis scripts for example for the beta monitor.

The container can be found at: \
`/data/raid2/eliza4/he6_cres/containers/he6cres-base.sif`

To enter an interactive appt session, run \
`apptainer shell --bind /data/raid2/eliza4/he6_cres \ /data/raid2/eliza4/he6_cres/containers/he6cres-base.sif`

Here you can, for example, check which python version is in the container: \
`which python3`
`python3`

With the container, after getting an account on WULF, you should be all set to go as long as you run jobs within the container. Most scripts are already configured to do this when submitting jobs to nodes. If you run anything on the head node outside the container, note that you will not have any dependancies installed.

### Update katydid on ROCKS:
* cd into katydid directory, stash existing version and run 
    * $ `git pull origin feature/FreqDomainInput.`
* Check permissions. Go back to /data/raid2eliza4/he6_cres/ and run 
    * $ `chmod -R 777 katydid`
* Enter apptainer and bind the local file system on the wulf head node.
    * $ `apptainer shell --bind /data/raid2/eliza4/he6_cres /data/raid2/eliza4/he6_cres/containers/he6cres-base.sif`
* source the script that uses the good CMake version and makes root libraries acessable:
    * $ `source /data/raid2/eliza4/he6_cres/root/bin/thisroot.sh`
* Then compile
    * > `cd katydid/build`
    * > `cmake .. -DCMAKE_BUILD_TYPE=RELEASE -DUSE_CPP14=ON -DKatydid_USE_MATLAB=OFF`
    * > `make` 
    * > `make install`
* Then exit singularity, from he6_cres copy over new config gile to base_configs:
    * $ `cp katydid/Examples/ConfigFiles/2-12_LTF_MBEB_tausnr7_2400.yaml katydid_analysis/base_configs/`
* and set it's permisissions
    * $ `chmod 774 katydid_analysis/base_configs/2-12_LTF_MBEB_tausnr7_2400.yaml`
	
### Run katydid:

* **Overview:** Run katydid on a list of run_ids.
* **Step 0:** Run katydid for the first time on a list of run_ids: 
	* Log on to rocks. 
	* `cd /data/raid2/eliza4/he6_cres`
	* `./rocks_analysis_pipeline/qsub_katydid.py -rids 373 380 385 393 399 405 411 418 424 430 436 -nid 436 -b "2-12_dbscan_high_energy.yaml" -fn 3`
		* The above will run at most fn files for each run_id listed using the base config file provided. 
		* For reference the above jobs (one job per run_id) were mostly finished in 30 mins. 
		* A analysis_id (aid) will be assigned to the analysis. Example: aid = 9.
		* A job log for each run_id will be created. Example: rid_0440_009.txt

* **Step 1:** Clean up. Let the above run (perhaps overnight) and then run the following clean-up script. Say the analysis_id assigned to the above katydid run was 009, then you will do the following to clean up that run. The same log files as above will be written to. Best to run the below twice if doing an analysis that has many many run_ids/spec files (greater than 500 files or so).
	* Log on to rocks. 
	* `cd /data/raid2/eliza4/he6_cres`
	* `./rocks_analysis_pipeline/qsub_katydid.py -rids 373 380 385 393 399 405 411 418 424 430 436 -nid 436 -b "2-12_dbscan_high_energy.yaml" -fn 3 -aid 9`
		* The above will rerun all of the files in analysis_id 9 that haven't yet been created.
		* Note that you want to include "-fn 3" here in case a node failed before even creating the  

### Post Processing:

* **Overview:** This is a three stage process. Run each stage without changing anything but the -stage argument. the -ms_standard argument determines the expected
spec(k) file name time format for the data you want to process.
	* 0: Root file names only to second. %Y-%m-%d-%H-%M-%S use for rid 1570 and earlier!
   	* 1: Root file names to ms. "%Y-%m-%d-%H-%M-%S-%f
	* For each of the steps, begin by navigating to our groups directory on eliza4: 
		* Log on to rocks. 
		* `cd /data/raid2/eliza4/he6_cres`
* **Stage 0:** Set-up.  
	* `./rocks_analysis_pipeline/qsub_post_processing.py -rids 373 380 385 393 399 405 411 418 424 430 436 -aid 9 -name "rocks_demo" -nft 2 -nfe 3 -dbscan 1 -ms_standard 1 -stage 0`
		* The above will first build the saved_experiment directory and then collect all of the `root_files.csv` files in the given list of run_ids and gather them into one csv that will be written into the saved_experiment directory ([name]_aid_[aid]). 
		* Before moving on to stage 1, check to see that the directory was made and the `root_files.csv` is present. 
* **Stage 1:** Processing.  
	* `./rocks_analysis_pipeline/qsub_post_processing.py -rids 373 380 385 393 399 405 411 418 424 430 436 -aid 9 -name "rocks_demo" -nft 2 -nfe 3 -dbscan 1 -ms_standard 1 -stage 1`	
		* This is the meat and potatoes of the post processing. nft files worth of tracks for each run_id, and nfe files worth of events for each run_id are written to disk as csvs. In order to allow for this to be done in parallel, each node is handed one file_id and processes all of the files with that file_id across all run_ids. Two files (tracks_[fid].csv, events_[fid].csv) are built for each fid. 
		* Before moving on to stage 2, check to see that the directory contains nft tracks and nfe events csvs. 
		* If for some reason (most likely failed nodes) all of the events_{n}.csv's aren't created rerun the exact same command. It will detect the missing ones and rerun those. 
		* `-dbscan` flag: Flag to run the default dbscan colinear event clustering (1) or not (0). Note that right now there are only default EventTimeIntc eps values (found by Heather via histogramming event time intercepts) for .75 - 3.25 in .25 T steps. This needs to be generalized at some point. 
* **Stage 2:** Clean-up. 
	* `./rocks_analysis_pipeline/qsub_post_processing.py -rids 373 380 385 393 399 405 411 418 424 430 436 -aid 9 -name "rocks_demo" -nft 2 -nfe 3 -dbscan 1 -ms_standard 1 -stage 2`
		* The above will gather all of the events and tracks csvs (respectively) into one csv. 

### Document your analysis
There is an elog for analyses run on ROCKS. Please see https://maxwell.npl.washington.edu/elog/he6cres/Katydid+analysis/ under our software elog. When you finish running a new analysis as described above, you should document it here. The title should be the "experiment_name" entered in the post-processing, and should contain
* The "experiment name" and who ran the analysis
* A short written summary of the goals indicating what run_ids were used, why this analysis was run, and any issues with it or context that future users might want to know
* A copy of the top output from the post-processing job_log up to where it says Post Processing Stage 0 DONE at PST time: XXX as this contains most of the relevant information including paths to written csv files.
This elog is not currently backfilled from before the first phase-II data campaign. Going forward anyone who runs an analysis should make an elog in this format. I know this is a bit annoying because right now we can't access the elog while on the vpn, so you have to copy info from the job_log and then close the connection to rocks, close the vpn, and then make the elog post. I recomend making a local file and then copying the contents to your elog post when you are off the vpn.

### Tools to investigate event classification quality and to conduct analysis. 

* **Investigate results:**
	* Grab the saved experiment and investigate the quality of the analysis. 
	* This is to be done locally using `class ExperimentResults` in module `results.py`.
	* A full demo of how this is done is here: `/rocks_analysis_pipeline/demo/rocks_analysis_demo.ipynb`. 
	* Copy the `.ipynb` into your own directory suitable for analysis and give it a try. Make neat plots like the one you see in the top of this readme. 


--------------------------------------------------------------------------------

## Useful stuff: 

* **Slurm:**
	* `sbatch submit_job.sh` Submit a job script
	* `squeue` to see all of the jobs you have running or in the queue.
	* `scancel -u netid` (delete all the jobs of user netid)
	* `scancel 4807` (delete job id 4807)
	* `scontrol show job 466` show info about a job eg jobid 466
	* `scontrol show node n4180` show info about a node

* **Permissions:**
	* I'm finding that with multiple users working in this analysis pipeline simultaneously the permissions can get weird. The following two commands run from `/he6_cres` should help: 
		* `chmod -R 774 katydid_analysis/`
		* `chgrp -R he6_cres katydid_analysis/`

* **Apptainer:**
	* To interactively enter the analysis apptainer run the following: 
		* `apptainer shell --bind /data/raid2/eliza4/he6_cres /data/raid2/eliza4/he6_cres/containers/he6cres-base.sif`
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
